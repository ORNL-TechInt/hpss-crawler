import base64
import CrawlConfig
import ibm_db as db2
import os
import util

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
        util.env_update(cfg)
        cfgname = cfg.get('db2', 'db_cfg_name')
        subname = cfg.get('db2', 'db_sub_name')
        dbhost = cfg.get('db2', 'hostname')
        dbport = cfg.get('db2', 'port')
        dbuser = cfg.get('db2', 'username')
        dbpwd = base64.b64decode(cfg.get('db2', 'password'))
        cxnstr = ("database=%s;" % cfgname +
                  "hostname=%s;" % dbhost +
                  "port=%s;" % dbport +
                  "uid=%s;" % dbuser +
                  "pwd=%s;" % dbpwd)
        db2cxn._db['cfg'] = db2.connect(cxnstr, "", "")
        cxnstr = ("database=%s;" % subname +
                  "hostname=%s;" % dbhost +
                  "port=%s;" % dbport +
                  "uid=%s;" % dbuser +
                  "pwd=%s;" % dbpwd)
        db2cxn._db['subsys'] = db2.connect(cxnstr, "", "")
        rval = db2cxn._db[dbsel]
    return rval
        
# -----------------------------------------------------------------------------
def get_bitfile_path(bitfile):
    """
    Given a bitfile id, walk back up the tree in HPSS to generate the bitfile's
    path
    """
    db = db2cxn('subsys')

    if bitfile.startswith("x'") or bitfile.startswith('x"'):
        sql = """
              select parent_id, name from hpss.nsobject where bitfile_id = %s
              """ % bitfile
    else:
        sql = """
              select parent_id, name from hpss.nsobject where bitfile_id = %s
              """ % hexstr(bitfile)

    util.log("Query: %s" % sql)
    r = db2.exec_immediate(db, sql)
    x = db2.fetch_assoc(r)
    bfl = []
    while (x):
        bfl.append(x)
        x = db2.fetch_assoc(r)

    if 1 < len(bfl):
        raise StandardError("Multiple objects found for bf %s" %
                            hexstr(bitfile))

    x = bfl[0]
    rval = ''
    while x:
        if rval == '':
            rval = x['NAME']
        else:
            rval = os.path.join(x['NAME'], rval)
        sql = """
              select parent_id, name from hpss.nsobject where object_id = %s
              """ % x['PARENT_ID']
        r = db2.exec_immediate(db, sql)
        x = db2.fetch_assoc(r)

    return rval

# -----------------------------------------------------------------------------
def get_bitfile_set(cfg, first_nsobj_id, limit):
    """
    Get a collection of bitfiles from DB2 returning a dict. The bitfiles in the
    set begin with object_id first_nsobj_id and end with the one before
    last_nsobj_id.

    get items between object_id LOW and HIGH:
          select A.object_id,
                 B.bfid, B.bfattr_cos_id, B.bfattr_create_time,
                 count(C.storage_class) as sc_count
          from hpss.nsobject A, hpss.bitfile B, hpss.bftapeseg C
          where A.bitfile_id = B.bfid and B.bfid = C.bfid and
                 B.bfattr_data_len > 0 and C.bf_offset = 0 and
                 ? <= A.object_id and A.object_id < ?
          group by A.object_id, B.bfid, B.bfattr_cos_id, B.bfattr_create_time

    get N items beginning at object_id LOW:
          "select A.object_id,
                 B.bfid, B.bfattr_cos_id, B.bfattr_create_time,
                 count(C.storage_class) as sc_count
          from hpss.nsobject A, hpss.bitfile B, hpss.bftapeseg C
          where A.bitfile_id = B.bfid and B.bfid = C.bfid and
                 B.bfattr_data_len > 0 and C.bf_offset = 0 and
                 ? <= A.object_id
          group by A.object_id, B.bfid, B.bfattr_cos_id, B.bfattr_create_time
          fetch first %d rows only" % limit
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
    r = db2.execute(stmt, (first_nsobj_id, first_nsobj_id+50))
    x = db2.fetch_assoc(stmt)
    while (x):
        rval.append(x)
        x = db2.fetch_assoc(stmt)
    return rval

# -----------------------------------------------------------------------------
def get_cos_info():
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
def hexstr(bfid):
    """
    Convert a raw bitfile id into a hexadecimal string as presented by DB2.
    """
    rval = "x'" + hexstr_uq(bfid) + "'"
    return rval

# -----------------------------------------------------------------------------
def hexstr_uq(bfid):
    """
    Convert a raw bitfile id into an unquoted hexadecimal string as presented
    by DB2.
    """
    rval = "".join(["%02x" % ord(c) for c in list(bfid)])
    return rval.upper()

# -----------------------------------------------------------------------------
def sectname():
    return 'tape-copy-checker'

# -----------------------------------------------------------------------------
def tcc_report(bitfile, cosinfo):
    """
    The bitfile appears to not have the right number of copies. We're going to
    write its information out to a report for manual followup.
    """
    fmt = "%7s %8s %8s %s\n"
    # Compute the bitfile's path
    bfp = get_bitfile_path(bitfile['BFID'])
    rpt = fmt % (bitfile['BFATTR_COS_ID'],
                 str(cosinfo[bitfile['BFATTR_COS_ID']]),
                 str(bitfile['SC_COUNT']),
                 bfp)
    util.log(rpt)
    try:
        tcc_report._f.write(rpt)
        tcc_report._f.flush()
    except AttributeError:
        cfg = CrawlConfig.get_config()
        rptfname = cfg.get(sectname(), 'report_file')
        tcc_report._f = open(rptfname, 'a')
        tcc_report._f.write(fmt % ("COS", "Ccopies", "Fcopies", "Filepath"))
        tcc_report._f.write(rpt)
        tcc_report._f.flush()

