import Alert
import Checkable
import ConfigParser
import CrawlConfig
import CrawlDBI
import Dimension
import os
import pdb
import pexpect
import sqlite3 as sql
import sys
import time
import util

# -----------------------------------------------------------------------------
def main(cfg):
    # Get stuff we need -- the logger object, hsi prompt string, dataroot,
    # etc.
    # clog = sys.modules['__main__'].get_logger()
    clog = util.get_logger()
    clog.info("checksum-verifier: firing up")
    hsi_prompt = "]:"
    plugdir = cfg.get('crawler', 'plugin-dir')
    dataroot = cfg.get('checksum-verifier', 'dataroot')
    dbfilename = cfg.get('checksum-verifier', 'dbfile')
    odds = cfg.getfloat('checksum-verifier', 'odds')
    n_ops = int(cfg.get('checksum-verifier', 'operations'))

    Checkable.Checkable.set_dbname(dbfilename)
    
    # Initialize our statistics
    (t_checksums, t_matches, t_failures) = get_stats(dbfilename)
    (checksums, matches, failures) = (0, 0, 0)

    # Fetch the list of HPSS objects that we're looking at from the
    # database
    try:
        clist = Checkable.Checkable.get_list(dbname=dbfilename)
    except CrawlDBI.DBIerror, e:
        if "no such table: checkables" in str(e):
            clog.info("checksum-verifier: calling ex_nihilo")
            Checkable.Checkable.ex_nihilo(dbname=dbfilename,
                                          dataroot=dataroot)
            clist = Checkable.Checkable.get_list(dbname=dbfilename)
        else:
            raise
    except StandardError, e:
        if 'Please call .ex_nihilo()' in str(e):
            clog.info("checksum-verifier: calling ex_nihilo")
            Checkable.Checkable.ex_nihilo(filename=dbfilename,
                                          dataroot=dataroot)
            clist = Checkable.Checkable.get_list(dbname=dbfilename)
        else:
            raise

    # We're going to process n_ops things in the HPSS namespace
    for op in range(n_ops):
        # if the list from the database is empty, there's nothing to do
        if 0 < len(clist):
            # but it's not, so grab the first item and check it
            item = clist.pop(0)
            clog.info("checksum-verifier: [%d] checking %s" %
                      (item.rowid, item.path))
            ilist = item.check(odds)

            # Expected outcomes that check can return:
            #  list of Checkables: read dir or checksummed files (may be empty)
            #  Alert:              checksum verify failed
            #  'access denied':    unaccessible directory
            #  'matched':          a checksum was verified
            #  'checksummed':      file was checksummed
            #  'skipped':          file was skipped
            #  'unavailable':      HPSS is temporarily unavailable
            #  StandardError:      invalid Checkable type (not 'f' or 'd')
            #
            if type(ilist) == str:
                if ilist == "access denied":
                    clog.info("checksum-verifier: dir %s not accessible" %
                              item.path)
                    clist.remove(item)
                elif ilist == "matched":
                    matches += 1
                    clog.info("checksum-verifier: %s checksums matched" %
                              item.path)
                elif ilist == "checksummed":
                    checksums += 1
                    clog.info("checksum-verifier: %s checksummed" % item.path)
                elif ilist == "skipped":
                    clog.info("checksum-verifier: %s skipped" % item.path)
                elif ilist == "unavailable":
                    clog.info("checksum-verifier: HPSS is not available")
                    break
                else:
                    clog.info("checksum-verifier: unexpected string returned " +
                              "from Checkable: '%s'" % ilist)
            elif type(ilist) == list:
                clog.info("checksum-verifier: in %s, found:" % item.path)
                for n in ilist:
                    clog.info("checksum-verifier: >>> %s" % str(n))
                    if 'f' == n.type:
                        clog.info("checksum-verifier: ..... checksummed")
                        checksums += 1
            elif isinstance(ilist, Checkable.Checkable):
                clog.info("checksum-verifier: file checksummed - %s, %s" %
                          (ilist.path, ilist.checksum))
                checksums += 1
            elif isinstance(ilist, Alert.Alert):
                clog.info("checksum-verifier: Alert generated: '%s'" %
                          ilist.msg())
                failures += 1
            else:
                clog.info("checksum-verifier: unexpected return val from " +
                          "Checkable.check: %s: %r" % (type(ilist), ilist))

    # Report the statistics in the log
    clog.info("checksum-verifier: files checksummed: %d; " % checksums +
              "checksums matched: %d; " % matches +
              "failures: %d" % failures)
    t_checksums += checksums
    t_matches += matches
    t_failures += failures
    clog.info("checksum-verifier: totals checksummed: %d; " % t_checksums +
              "matches: %d; " % t_matches +
              "failures: %d" % t_failures)

    # Record the current totals in the database
    update_stats(dbfilename, (t_checksums, t_matches, t_failures))

    # Report the dimension data in the log
    d = Dimension.Dimension(name='cos')
    clog.info(d.report())

stats_table = 'cvstats'
# -----------------------------------------------------------------------------
def get_stats(dbfilename):
    """
    Return the number of files checksummed, checksums matched, and checksums
    failed.
    """
    db = sql.connect(dbfilename)
    cx = db.cursor()
    cx.execute("select name from sqlite_master " +
               "where type = 'table' and name = '%s'" % stats_table)
    rows = cx.fetchall()
    if 0 == len(rows):
        rval = (0, 0, 0)
    else:
        cx.execute("""select checksums, matches, failures from %s
                      where rowid = 1""" % stats_table)
        rows = cx.fetchall()
        rval = rows[0]

    db.close()
    return rval

# -----------------------------------------------------------------------------
def update_stats(dbfilename, cmf):
    """
    Record the three values in tuple cmf in table cvstats in the database. If
    the table does not exist, create it.
    """
    db = sql.connect(dbfilename)
    cx = db.cursor()
    cx.execute("select name from sqlite_master " +
               "where type = 'table' and name = '%s'" % stats_table)
    rows = cx.fetchall()
    if 0 == len(rows):
        cx.execute("""create table %s(rowid         int,
                                      checksums     int,
                                      matches       int,
                                      failures      int)""" % stats_table)
        cx.execute("""insert into %s(rowid, checksums, matches, failures)
                                  values(     1,         0,       0,        0)
                   """ % stats_table)
    cx.execute("""update %s set checksums=?,
                                     matches=?,
                                     failures=?
                         where rowid = 1""" % stats_table,
               cmf)

    db.commit()
    db.close()

# -----------------------------------------------------------------------------
if __name__ == '__main__':
    pdb.set_trace()
    cfg = CrawlConfig.get_config()
    main(cfg)
