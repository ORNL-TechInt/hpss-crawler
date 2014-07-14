#!/usr/bin/env python
import base64
from hpssic import CrawlConfig
from hpssic import CrawlDBI
from hpssic import hpss
import os
import pdb
import pprint
import re
import sys
from hpssic import tcc_lib
import time
from hpssic import util

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
    cosinfo = tcc_lib.get_cos_info()
    # for cos_id in cosinfo:
    #     CrawlConfig.log("%d => %d" % (int(cos_id), int(cosinfo[cos_id])))

    # get the nsobject_id of the next bitfile to process from mysql
    next_nsobj_id = get_next_nsobj_id(cfg)
    CrawlConfig.log("next nsobject id = %d" % next_nsobj_id)

    # fetch the next N bitfiles from DB2
    CrawlConfig.log("looking for nsobject ids between %d and %d"
                    % (next_nsobj_id, next_nsobj_id+how_many-1))
    bfl = tcc_lib.get_bitfile_set(cfg,
                                  int(next_nsobj_id),
                                  how_many)

    CrawlConfig.log("got %d bitfiles" % len(bfl))

    if len(bfl) == 0:
        for oid in range(next_nsobj_id, next_nsobj_id+how_many):
            record_checked_ids(cfg, oid, oid, 1, 0)
            if cfg.getboolean(sectname, 'verbose'):
                CrawlConfig.log("Object %d is not complete" % oid)
    else:
        # for each bitfile, if it does not have the right number of copies,
        # report it
        for bf in bfl:
            correct = 1
            error = 0
            if bf['SC_COUNT'] != cosinfo[bf['BFATTR_COS_ID']]:
                tcc_lib.tcc_report(bf, cosinfo)
                correct = 0
                error = 1
                CrawlConfig.log("%s %s %d != %d" %
                                (bf['OBJECT_ID'],
                                 tcc_lib.hexstr(bf['BFID']),
                                 bf['SC_COUNT'],
                                 cosinfo[bf['BFATTR_COS_ID']]))
            elif cfg.getboolean(sectname, 'verbose'):
                CrawlConfig.log("%s %s %d == %d" %
                                (bf['OBJECT_ID'],
                                 tcc_lib.hexstr(bf['BFID']),
                                 bf['SC_COUNT'],
                                 cosinfo[bf['BFATTR_COS_ID']]))

            last_obj_id = int(bf['OBJECT_ID'])
            record_checked_ids(cfg, last_obj_id, last_obj_id, correct, error)

        CrawlConfig.log("last nsobject in range: %d" % last_obj_id)


# -----------------------------------------------------------------------------
def get_next_nsobj_id(cfg):
    """
    Read the TCC table in the HPSSIC database to get the next nsobject id. If
    the table does not exist, we create it and return 1 for the next object id
    to check. If the table exists but is empty, we return 1 for the next object
    id to check.
    """
    tabname = cfg.get(sectname, 'table_name')
    db = CrawlDBI.DBI()
    if not db.table_exists(table=tabname):
        rval = 1
    else:
        rows = db.select(table=tabname,
                         fields=['max(check_time)'])
        max_time = rows[0][0]
        if max_time is None:
            rval = 1
        else:
            rows = db.select(table=tabname,
                             fields=['high_nsobj_id'],
                             where='check_time = ?',
                             data=(max_time,))
            rval = int(rows[0][0]) + 1
            if highest_nsobject_id() < rval:
                rval = 1
    db.close()
    return rval


# -----------------------------------------------------------------------------
def record_checked_ids(cfg, low, high, correct, error):
    """
    Save checked NSOBJECT ids in the HPSSIC database.

    If we check a range and get no hits (i.e., no NSOBJECT ids exist in the
    range), we'll store

       (<time>, <low-id>, <high-id>, 0, 0)

    If we get a hit with the right copy count, we store it by itself as

       (<time>, <hit-id>, <hit-id>, 1, 0)

    If we get a hit with the wrong copy count, we store it by itself as

       (<time>, <hit-id>, <hit-id>, 0, 1)
    """
    tabname = cfg.get(sectname, 'table_name')
    db = CrawlDBI.DBI()

    if not db.table_exists(table=tabname):
        db.create(table=tabname,
                  fields=['check_time    integer',
                          'low_nsobj_id  integer',
                          'high_nsobj_id integer',
                          'correct       integer',
                          'error         integer'])

    ts = int(time.time())
    CrawlConfig.log("recording checked ids %d to %d at %d" % (low, high, ts))
    db.insert(table=tabname,
              fields=['check_time',
                      'low_nsobj_id',
                      'high_nsobj_id',
                      'correct',
                      'error'],
              data=[(ts, low, high, correct, error)])
    db.close()


# -----------------------------------------------------------------------------
def highest_nsobject_id():
    """
    Cache and return the largest NSOBJECT id in the DB2 database.
    """
    if any([not hasattr(highest_nsobject_id, '_max_obj_id'),
            60 < time.time() - highest_nsobject_id._when]):
        H = CrawlDBI.DBI(dbtype='db2', dbname=CrawlDBI.db2name('subsys'))
        result = H.select(table='nsobject',
                          fields=['max(object_id) as max_obj_id'])
        H.close()
        highest_nsobject_id._max_obj_id = int(result[0]['MAX_OBJ_ID'])
        highest_nsobject_id._when = time.time()
        CrawlConfig.log("max object id = %d at %s" %
                        (highest_nsobject_id._max_obj_id,
                         util.ymdhms(highest_nsobject_id._when)))

    rval = highest_nsobject_id._max_obj_id
    return rval

# -----------------------------------------------------------------------------
if __name__ == '__main__':
    main(CrawlConfig.get_config())
