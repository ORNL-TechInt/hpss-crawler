import Checkable
import ConfigParser
import CrawlConfig
import os
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
    hsi_prompt = "]:"
    plugdir = cfg.get('crawler', 'plugin-dir')
    dataroot = cfg.get('checksum-verifier', 'dataroot')
    dbfilename = cfg.get('checksum-verifier', 'dbfile')
    odds = cfg.get('checksum-verifier', 'odds')
    n_ops = int(cfg.get('checksum-verifier', 'operations'))

    # Initialize our statistics
    (t_checksums, t_matches, t_failures) = get_stats(dbfilename)
    (checksums, matches, failures) = (0, 0, 0)

    # Fetch the list of HPSS objects that we're looking at from the
    # sqlite database
    try:
        clist = Checkable.Checkable.get_list()
    except StandardError, e:
        if 'Please call .ex_nihilo()' in str(e):
            Checkable.Checkable.ex_nihilo(filename=dbfilename,
                                          dataroot=dataroot)
            clist = Checkable.Checkable.get_list()
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

            # There are six expected outcomes that check can return:
            #  'matched' => a checksum was verified
            #  'skipped' => a file was skipped
            #  'access denied' => encountered a directory where we don't have
            #                     permission to go
            #  <list of Checkables> => read a directory, generated a Checkable
            #                          for each entry
            #  <Checkable> => a file checksummed was collected
            #  <Alert> => a file checksum did not match and an Alert was
            #             generated
            #  ...     => unexpected cases
            if type(ilist) == str:
                if ilist == "matched":
                    matches += 1
                    clog.info("checksum-verifier: %s checksums matched" %
                              item.path)
                elif ilist == "skipped":
                    clog.info("checksum-verifier: %s skipped" % item.path)
                elif ilist == "access denied":
                    clog.info("checksum-verifier: dir %s not accessible" %
                              item.path)
                else:
                    clog.info("checksum-verifier: unexpected string returned " +
                              "from Checkable: '%s'" % ilist)
            elif type(ilist) == list:
                clog.info("checksum-verifier: in %s, found:" % item.path)
                for n in ilist:
                    clog.info("checksum-verifier: >>> %s %s %s %f" %
                              (n.path, n.type, n.checksum,
                               n.last_check))
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

# -----------------------------------------------------------------------------
def get_stats(dbfilename):
    """
    Return the number of files checksummed, checksums matched, and checksums
    failed.
    """
    db = sql.connect(dbfilename)
    cx = db.cursor()
    cx.execute("select name from sqlite_master " +
               "where type = 'table' and name = 'distats'")
    rows = cx.fetchall()
    if 0 == len(rows):
        rval = (0, 0, 0)
    else:
        cx.execute("""select checksums, matches, failures from distats
                      where rowid = 1""")
        rows = cx.fetchall()
        rval = rows[0]

    db.close()
    return rval

# -----------------------------------------------------------------------------
def update_stats(dbfilename, cmf):
    """
    Record the three values in tuple cmf in table distats in the database. If
    the table does not exist, create it.
    """
    db = sql.connect(dbfilename)
    cx = db.cursor()
    cx.execute("select name from sqlite_master " +
               "where type = 'table' and name = 'distats'")
    rows = cx.fetchall()
    if 0 == len(rows):
        cx.execute("""create table distats(rowid         int,
                                           checksums     int,
                                           matches       int,
                                           failures      int)""")
        cx.execute("""insert into distats(rowid, checksums, matches, failures)
                                  values(     1,         0,       0,        0)
                   """)
    cx.execute("""update distats set checksums=?,
                                     matches=?,
                                     failures=?
                         where rowid = 1""", cmf)

    db.commit()
    db.close()
