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
    # Get stuff we need -- the logger object, dataroot, etc.
    util.log("firing up")
    plugdir = cfg.get('crawler', 'plugin-dir')
    dataroot = util.csv_list(cfg.get('checksum-verifier', 'dataroot'))
    odds = cfg.getfloat('checksum-verifier', 'odds')
    n_ops = int(cfg.get('checksum-verifier', 'operations'))

    # Initialize our statistics
    (t_checksums, t_matches, t_failures) = get_stats()
    (checksums, matches, failures) = (0, 0, 0)

    # Fetch the list of HPSS objects that we're looking at from the
    # database
    try:
        clist = Checkable.Checkable.get_list(odds)
    except CrawlDBI.DBIerror, e:
        sqlite_msg = "no such table: checkables"
        mysql_msg = "Table '.*' doesn't exist"
        if util.rgxin(sqlite_msg, str(e)) or util.rgxin(mysql_msg, str(e)):
            util.log("calling ex_nihilo")
            Checkable.Checkable.ex_nihilo(dataroot=dataroot)
            clist = Checkable.Checkable.get_list(odds)
        else:
            raise
    except StandardError, e:
        if 'Please call .ex_nihilo()' in str(e):
            util.log("calling ex_nihilo")
            Checkable.Checkable.ex_nihilo(dataroot=dataroot)
            clist = Checkable.Checkable.get_list(odds)
        else:
            raise

    # We're going to process n_ops things in the HPSS namespace
    for op in range(n_ops):
        # if the list from the database is empty, there's nothing to do
        if 0 < len(clist):
            # but it's not, so grab the first item and check it
            item = clist.pop(0)
            util.log("[%d] checking %s" % (item.rowid, item))
            ilist = item.check()

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
                    util.log("dir %s not accessible" % item.path)
                    # clist.remove(item)
                elif ilist == "matched":
                    matches += 1
                    util.log("%s checksums matched" % item.path)
                elif ilist == "checksummed":
                    checksums += 1
                    util.log("%s checksummed" % item.path)
                elif ilist == "skipped":
                    util.log("%s skipped" % item.path)
                elif ilist == "unavailable":
                    util.log("HPSS is not available")
                    break
                else:
                    util.log("unexpected string returned " +
                              "from Checkable: '%s'" % ilist)
            elif type(ilist) == list:
                util.log("in %s, found:" % item)
                for n in ilist:
                    util.log(">>> %s" % str(n))
                    if 'f' == n.type and n.checksum != 0:
                        util.log(".. previously checksummed")
                        checksums += 1
            elif isinstance(ilist, Checkable.Checkable):
                util.log("Checkable returned - file checksummed - %s, %s" %
                          (ilist.path, ilist.checksum))
                checksums += 1
            elif isinstance(ilist, Alert.Alert):
                util.log("Alert generated: '%s'" %
                          ilist.msg())
                failures += 1
            else:
                util.log("unexpected return val from " +
                          "Checkable.check: %s: %r" % (type(ilist), ilist))

    # Report the statistics in the log
    util.log("files checksummed: %d; " % checksums +
              "checksums matched: %d; " % matches +
              "failures: %d" % failures)
    t_checksums += checksums
    t_matches += matches
    t_failures += failures
    util.log("totals checksummed: %d; " % t_checksums +
              "matches: %d; " % t_matches +
              "failures: %d" % t_failures)

    # Record the current totals in the database
    update_stats((t_checksums, t_matches, t_failures))

    # Report the dimension data in the log
    d = Dimension.Dimension(name='cos')
    util.log(d.report())

stats_table = 'cvstats'
# -----------------------------------------------------------------------------
def get_stats():
    """
    Return the number of files checksummed, checksums matched, and checksums
    failed.
    """
    db = CrawlDBI.DBI()
    if db.table_exists(table=stats_table):
        rows = db.select(table=stats_table,
                         fields=["checksums", "matches", "failures"],
                         where="rowid = 1")
        rval = rows[0]
    else:
        rval = (0, 0, 0)
    db.close()
    return rval

# -----------------------------------------------------------------------------
def update_stats(cmf):
    """
    Record the three values in tuple cmf in table cvstats in the database. If
    the table does not exist, create it.
    """
    db = CrawlDBI.DBI()
    if not db.table_exists(table=stats_table):
        db.create(table=stats_table,
                  fields=["rowid int",
                          "checksums int",
                          "matches int",
                          "failures int",])
        db.insert(table=stats_table,
                  fields=["rowid", "checksums", "matches", "failures"],
                  data=[(1, 0, 0, 0)])

    db.update(table=stats_table,
              fields=["checksums", "matches", "failures"],
              data=[cmf],
              where="rowid = 1")
    db.close()
                  
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    cfg = CrawlConfig.get_config()
    main(cfg)
