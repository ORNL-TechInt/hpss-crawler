#!/usr/bin/env python
import base64
import CrawlConfig
import CrawlDBI
import hpss
import ibm_db as db2
import os
import pdb
import pprint
import re
import sys
import tcc_common
import util

sectname = 'tape-copy-checker'
# -----------------------------------------------------------------------------
def main(cfg):
    """
    Tape Copy Checker retrieves the necessary information from the DB2 database
    to find files where the number of copies stored may not match the number
    called for by the COS.
    """
    # retrieve configuration items as needed
    how_many = int(cfg.get_d(sectname, 'operations', 10))
    util.log("tape-copy-checker: firing up for %d items" % how_many)
    
    # retrieve COS info
    cosinfo = get_cos_info(cfg)
    # for cos_id in cosinfo:
    #     util.log("%d => %d" % (int(cos_id), int(cosinfo[cos_id])))

    # get the nsobject_id of the next bitfile to process from mysql
    next_nsobj_id = get_next_nsobj_id(cfg)
    util.log("next nsobject id = %d" % next_nsobj_id)
    
    # fetch the next N bitfiles from DB2
    bfl = get_bitfile_set(cfg, int(next_nsobj_id), int(next_nsobj_id + how_many))
    
    util.log("got %d bitfiles" % len(bfl))

    if len(bfl) == 0:
        util.log("No bitfiles in range -- updating to %s" %
                 (next_nsobj_id + how_many))
        update_next_nsobj_id(cfg, next_nsobj_id + how_many)
    else:
        # for each bitfile, if it does not have the right number of copies,
        # report it
        for bf in bfl:
            if bf['SC_COUNT'] != cosinfo[bf['BFATTR_COS_ID']]:
                tcc_report(bf, cosinfo)
                util.log("%s %s %d != %d" %
                         (bf['OBJECT_ID'],
                          tcc_common.hexstr(bf['BFID']),
                          bf['SC_COUNT'],
                          cosinfo[bf['BFATTR_COS_ID']]))
            elif cfg.getboolean(sectname, 'verbose'):
                util.log("%s %s %d == %d" %
                         (bf['OBJECT_ID'],
                          tcc_common.hexstr(bf['BFID']),
                          bf['SC_COUNT'],
                          cosinfo[bf['BFATTR_COS_ID']]))

            update_next_nsobj_id(cfg, bf['OBJECT_ID'])
            last_obj_id = bf['OBJECT_ID']

        util.log("last nsobject in range: %s" % last_obj_id)


    # # for each bitfile, if it does not have the right number of copies, report
    # # it
    # for bf in bfl:
    #     if bf['SC_COUNT'] != cosinfo[bf['BFATTR_COS_ID']]:
    #         tcc_report(bf)
    #         util.log("%s %s %d != %d" %
    #                  (bf['OBJECT_ID'],
    #                   tcc_common.hexstr(bf['BFID']),
    #                   bf['SC_COUNT'],
    #                   cosinfo[bf['BFATTR_COS_ID']]))
    #     elif cfg.getboolean(sectname, 'verbose'):
    #         util.log("%s %s %d == %d" %
    #                  (bf['OBJECT_ID'],
    #                   tcc_common.hexstr(bf['BFID']),
    #                   bf['SC_COUNT'],
    #                   cosinfo[bf['BFATTR_COS_ID']]))
    #         
    #     update_next_nsobj_id(cfg, bf['OBJECT_ID'])
    #     # util.log("recording next nsobject id: %d" % bf['OBJECT_ID'])

# -----------------------------------------------------------------------------
def db2cxn(dbsel):
    """
    Cache and return the DB2 connection for either the 'cfg' or 'subsys' database
    """
    try:
        rval = db2cxn._db[dbsel]
    except AttributeError:
        db2cxn._db = {}
        cfg = CrawlConfig.get_config()
        cfgname = cfg.get('db2', 'db_cfg_name')
        subname = cfg.get('db2', 'db_sub_name')
        dbhost = cfg.get('db2', 'hostname')
        dbport = cfg.get('db2', 'port')
        dbuser = cfg.get('db2', 'username')
        dbpwd = base64.b64decode(cfg.get('db2', 'password'))
        db2cxn._db['cfg'] = db2.connect("database=%s;" % cfgname +
                                        "hostname=%s;" % dbhost +
                                        "port=%s;" % dbport +
                                        "uid=%s;" % dbuser +
                                        "pwd=%s;" % dbpwd,
                                        "",
                                        "")
        db2cxn._db['subsys'] = db2.connect("database=%s;" % subname +
                                        "hostname=%s;" % dbhost +
                                        "port=%s;" % dbport +
                                        "uid=%s;" % dbuser +
                                        "pwd=%s;" % dbpwd,
                                        "",
                                        "")
        rval = db2cxn._db[dbsel]
    return rval
        
# -----------------------------------------------------------------------------
def get_bitfile_path(bitfile):
    """
    Given a bitfile id, walk back up the tree in HPSS to generate the bitfile's
    path
    """
    db = db2cxn('subsys')

    sql = """
          select parent_id, name from nsobject where bitfile_id = %s
          """ % tcc_common.hexstr(bitfile['BFID'])

    util.log("Query: %s" % sql)
    r = db.exec_immediate(db, sql)
    x = db2.fetch_assoc(r)
    bfl = []
    while (x):
        bfl.append(x)
        x = db2.fetch_assoc(r)

    if 1 < len(bfl):
        raise StandardError("Multiple objects found for bf %s" %
                            tcc_common.hexstr(bitfile['BFID']))

    rval = bfl[0]['NAME']

    x = bfl[0]
    while x['NAME'] != '/':
        sql = """
              select parent_id, name from nsobject where object_id = %s
              """ % x['PARENT_ID']
        r = db2.exec_immediate(db, sql)
        x = db2.fetch_assoc(r)
        rval = os.path.join([x['NAME'], rval])

    return rval

# -----------------------------------------------------------------------------
def get_bitfile_set(cfg, first_nsobj_id, last_nsobj_id):
    """
    Get a collection of bitfiles from DB2 returning a dict. The bitfiles in the
    set begin with object_id first_nsobj_id and end with the one before
    last_nsobj_id.
    """
    rval = {}
    db = db2cxn('subsys')
    sql = """
          select A.object_id,
                 B.bfid, B.bfattr_cos_id, B.bfattr_create_time,
                 count(C.storage_class) as sc_count
          from hpss.nsobject A, hpss.bitfile B, hpss.bftapeseg C
          where A.bitfile_id = B.bfid and B.bfid = C.bfid and
                 B.bfattr_data_len > 0 and C.bf_offset = 0 and
                 ? <= A.object_id and A.object_id < ?
          group by A.object_id, B.bfid, B.bfattr_cos_id, B.bfattr_create_time
          """
    rval = []
    stmt = db2.prepare(db, sql)
    r = db2.execute(stmt, (first_nsobj_id, last_nsobj_id))
    x = db2.fetch_assoc(stmt)
    while (x):
        rval.append(x)
        x = db2.fetch_assoc(stmt)
    return rval

# -----------------------------------------------------------------------------
def get_cos_info(cfg):
    """
    Read COS info from tables COS and HIER in the DB2 database
    """
    rval = {}
    db = db2cxn('cfg')
    sql = """select a.cos_id, a.hier_id, b.slevel0_migrate_list_count
             from hpss.cos as a, hpss.hier as b
             where a.hier_id = b.hier_id"""
    r = db2.exec_immediate(db, sql)
    x = db2.fetch_assoc(r)
    while (x):
        rval[x['COS_ID']] = x['SLEVEL0_MIGRATE_LIST_COUNT']
        x = db2.fetch_assoc(r)
    return rval

# -----------------------------------------------------------------------------
def get_next_nsobj_id(cfg):
    """
    Read the TCC table in the HPSSIC database to get the next nsobject id. If
    the table does not exist, we create it and set the next object id to 0.
    What is stored in the table is the last object id we've seen. We increment
    it and return the next object id we expect to handle.
    """
    tabname = cfg.get(sectname, 'table_name')
    db = CrawlDBI.DBI()
    if not db.table_exists(table=tabname):
        db.create(table=tabname,
                  fields=['next_nsobj_id integer'])
        db.insert(table=tabname,
                  fields=['next_nsobj_id'],
                  data=[0])
        rval = 0
    else:
        rows = db.select(table=tabname,
                         fields=['next_nsobj_id'])
        rval = rows[0][0]

    db.close()
    return rval + 1
        
# -----------------------------------------------------------------------------
def tcc_report(bitfile, cosinfo):
    """
    The bitfile appears to not have the right number of copies. We're going to
    write its information out to a report for manual followup.
    """
    # Compute the bitfile's path
    bfp = get_bitfile_path(bitfile)
    rpt = "%5d %5d %s" % (cosinfo[bitfile['BFATTR_COS_ID']],
                          bitfile['SC_COUNT'],
                          bfp)
    util.log(rpt)
    try:
        tcc_report._f.write(rpt + "\n")
    except AttributeError:
        cfg = CrawlConfig.get_config()
        rptfname = cfg.get(sectname, 'report_file')
        tcc_report._f = open(rptfname, 'w')
        tcc_report._f.write(rpt + "\n")


# -----------------------------------------------------------------------------
def update_next_nsobj_id(cfg, value):
    """
    Update the next nsobject id in the HPSSIC database.
    """
    tabname = cfg.get(sectname, 'table_name')
    db = CrawlDBI.DBI()
    db.update(table=tabname, fields=['next_nsobj_id'], data=[(value,)])
    db.close()
    
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    main(CrawlConfig.get_config())
