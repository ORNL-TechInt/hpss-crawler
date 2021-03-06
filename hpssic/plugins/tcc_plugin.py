import base64
from hpssic import CrawlConfig
from hpssic import CrawlDBI
import glob
from hpssic import hpss
import os
import pdb
import pprint
import re
import sys
from hpssic import tcc_lib
import time
from hpssic import util
from hpssic import util as U


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

    # check for priority file(s)
    pri_glob = cfg.get_d(tcc_lib.sectname(), 'priority', '')
    if pri_glob != '':
        if 0 < tcc_priority(pri_glob, cosinfo):
            return

    # get the nsobject_id of the next bitfile to process from mysql
    next_nsobj_id = tcc_lib.get_next_nsobj_id(cfg)
    CrawlConfig.log("next nsobject id = %d" % next_nsobj_id)

    # fetch the next N bitfiles from DB2
    CrawlConfig.log("looking for nsobject ids between %d and %d"
                    % (next_nsobj_id, next_nsobj_id+how_many-1))
    try:
        bfl = tcc_lib.get_bitfile_set(int(next_nsobj_id),
                                      how_many)
    except U.HpssicError as e:
        bfl = []
        pass

    CrawlConfig.log("got %d bitfiles" % len(bfl))

    errcount = 0
    if len(bfl) == 0:
        for oid in range(next_nsobj_id, next_nsobj_id+how_many):
            tcc_lib.record_checked_ids(cfg, oid, oid, 1, 0)
            if cfg.getboolean(tcc_lib.sectname(), 'verbose'):
                CrawlConfig.log("Object %d is not complete" % oid)
                errcount += 1
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
            tcc_lib.record_checked_ids(cfg,
                                       last_obj_id,
                                       last_obj_id,
                                       correct,
                                       error)
            errcount += error

        CrawlConfig.log("last nsobject in range: %d" % last_obj_id)

    return errcount


# -----------------------------------------------------------------------------
def tcc_priority(globspec, cosinfo):
    """
    Handle any files matching globspec. Return the number of files processed.
    """
    rval = 0
    cfg = CrawlConfig.get_config()
    pri_compdir = cfg.get_d(tcc_lib.sectname(), 'completed', '/tmp')
    for filepath in glob.glob(globspec):
        tcc_lib.check_file(filepath, verbose=False, plugin=True)
        cpath = U.pathjoin(pri_compdir, U.basename(filepath))
        os.rename(filepath, cpath)

    return rval


# -----------------------------------------------------------------------------
if __name__ == '__main__':
    main(CrawlConfig.get_config())
