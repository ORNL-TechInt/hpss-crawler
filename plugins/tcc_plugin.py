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
import time
import util

sectname = 'tcc'
# -----------------------------------------------------------------------------
def main(cfg):
    """
    Tape Copy Checker retrieves the necessary information from the DB2 database
    to find files where the number of copies stored may not match the number
    called for by the COS.
    """
    # retrieve configuration items as needed
    how_many = int(cfg.get_d(sectname, 'operations', 10))
    CrawlConfig.log("tape-copy-checker: firing up for %d items" % how_many)
    
    # retrieve COS info
    cosinfo = tcc_common.get_cos_info()
    # for cos_id in cosinfo:
    #     CrawlConfig.log("%d => %d" % (int(cos_id), int(cosinfo[cos_id])))

    # get the nsobject_id of the next bitfile to process from mysql
    next_nsobj_id = get_next_nsobj_id(cfg)
    CrawlConfig.log("next nsobject id = %d" % next_nsobj_id)
    
    # fetch the next N bitfiles from DB2
    CrawlConfig.log("looking for nsobject ids between %d and %d"
             % (next_nsobj_id, next_nsobj_id+how_many-1))
    bfl = tcc_common.get_bitfile_set(cfg,
                                     int(next_nsobj_id),
                                     how_many)
    
    CrawlConfig.log("got %d bitfiles" % len(bfl))

    if len(bfl) == 0:
        next_nsobj_id += how_many
        update_next_nsobj_id(cfg, next_nsobj_id)
    else:
        # for each bitfile, if it does not have the right number of copies,
        # report it
        for bf in bfl:
            if bf['SC_COUNT'] != cosinfo[bf['BFATTR_COS_ID']]:
                tcc_common.tcc_report(bf, cosinfo)
                CrawlConfig.log("%s %s %d != %d" %
                         (bf['OBJECT_ID'],
                          tcc_common.hexstr(bf['BFID']),
                          bf['SC_COUNT'],
                          cosinfo[bf['BFATTR_COS_ID']]))
            elif cfg.getboolean(sectname, 'verbose'):
                CrawlConfig.log("%s %s %d == %d" %
                         (bf['OBJECT_ID'],
                          tcc_common.hexstr(bf['BFID']),
                          bf['SC_COUNT'],
                          cosinfo[bf['BFATTR_COS_ID']]))
            
            last_obj_id = int(bf['OBJECT_ID'])
            next_nsobj_id = last_obj_id + 1
            update_next_nsobj_id(cfg, next_nsobj_id)

        CrawlConfig.log("last nsobject in range: %d" % last_obj_id)
        
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
                  data=[1])
        rval = 1
    else:
        rows = db.select(table=tabname,
                         fields=['next_nsobj_id'])
        rval = int(rows[0][0])
        if rval < 1:
            rval = 1

    db.close()
    return rval
        
# -----------------------------------------------------------------------------
def update_next_nsobj_id(cfg, value):
    """
    Update the next nsobject id in the HPSSIC database.
    """
    if (not hasattr(update_next_nsobj_id, '_max_obj_id') or
        (60 < time.time() - update_next_nsobj_id._when)):
        dbname = {'hpss-dev01': 'subsys',
                  'hpss-crawler01': 'hsubsys1'}[util.hostname()]
        H = CrawlDBI.DBI(dbtype='db2', dbname=dbname)
        result = H.select(table='nsobject',
                          fields=['max(object_id) as max_obj_id'])
        H.close()
        update_next_nsobj_id._max_obj_id = int(result[0]['MAX_OBJ_ID'])
        update_next_nsobj_id._when = time.time()
        CrawlConfig.log("max object id = %d at %s" %
                 (update_next_nsobj_id._max_obj_id,
                  time.strftime("%Y.%m%d %H:%M:%S",
                                time.localtime(update_next_nsobj_id._when))))

    max_obj_id = update_next_nsobj_id._max_obj_id
    if max_obj_id < value:
        value = 1
    CrawlConfig.log("storing next object id: %d" % value)

    tabname = cfg.get(sectname, 'table_name')
    db = CrawlDBI.DBI()
    db.update(table=tabname, fields=['next_nsobj_id'], data=[(value,)])
    db.close()

# -----------------------------------------------------------------------------
if __name__ == '__main__':
    main(CrawlConfig.get_config())
