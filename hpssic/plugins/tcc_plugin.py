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


# -----------------------------------------------------------------------------
def main(cfg):
    """
    Tape Copy Checker retrieves the necessary information from the DB2 database
    to find files where the number of copies stored may not match the number
    called for by the COS.
    """
    # retrieve configuration items as needed
    how_many = int(cfg.get_d(tcc_lib.sectname(), 'operations', 10))
    CrawlConfig.log("tape-copy-checker: firing up for %d items" % how_many)

    # retrieve COS info
    cosinfo = tcc_lib.get_cos_info()
    # for cos_id in cosinfo:
    #     CrawlConfig.log("%d => %d" % (int(cos_id), int(cosinfo[cos_id])))

    # get the nsobject_id of the next bitfile to process from mysql
    next_nsobj_id = tcc_lib.get_next_nsobj_id(cfg)
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
            if cfg.getboolean(tcc_lib.sectname(), 'verbose'):
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
            elif cfg.getboolean(tcc_lib.sectname(), 'verbose'):
                CrawlConfig.log("%s %s %d == %d" %
                                (bf['OBJECT_ID'],
                                 tcc_lib.hexstr(bf['BFID']),
                                 bf['SC_COUNT'],
                                 cosinfo[bf['BFATTR_COS_ID']]))

            last_obj_id = int(bf['OBJECT_ID'])
            record_checked_ids(cfg, last_obj_id, last_obj_id, correct, error)

        CrawlConfig.log("last nsobject in range: %d" % last_obj_id)


# -----------------------------------------------------------------------------
def highest_nsobject_id():
    """
    Cache and return the largest NSOBJECT id in the DB2 database. The variables
    highest_nsobject_id._max_obj_id and highest_nsobject_id._when are local to
    this function but do not lose their values between invocations.
    """
    if any([not hasattr(highest_nsobject_id, '_max_obj_id'),
            60 < time.time() - highest_nsobject_id._when]):

        highest_nsobject_id._max_obj_id = tcc_lib.max_nsobj_id()
        highest_nsobject_id._when = time.time()
        CrawlConfig.log("max object id = %d at %s" %
                        (highest_nsobject_id._max_obj_id,
                         util.ymdhms(highest_nsobject_id._when)))

    rval = highest_nsobject_id._max_obj_id
    return rval

# -----------------------------------------------------------------------------
if __name__ == '__main__':
    main(CrawlConfig.get_config())
