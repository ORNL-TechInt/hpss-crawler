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
    CrawlConfig.log("firing up")
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
        clist = Checkable.Checkable.get_list(odds, rootlist=dataroot)
    except CrawlDBI.DBIerror, e:
        sqlite_msg = "no such table: checkables"
        mysql_msg = "Table '.*' doesn't exist"
        if util.rgxin(sqlite_msg, str(e)) or util.rgxin(mysql_msg, str(e)):
            CrawlConfig.log("calling ex_nihilo")
            Checkable.Checkable.ex_nihilo(dataroot=dataroot)
            clist = Checkable.Checkable.get_list(odds)
        else:
            raise
    except StandardError, e:
        if 'Please call .ex_nihilo()' in str(e):
            CrawlConfig.log("calling ex_nihilo")
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
            CrawlConfig.log("[%d] checking %s" % (item.rowid, item))
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
                    CrawlConfig.log("dir %s not accessible" % item.path)
                    # clist.remove(item)
                elif ilist == "matched":
                    matches += 1
                    CrawlConfig.log("%s checksums matched" % item.path)
                elif ilist == "checksummed":
                    # checksums += 1
                    CrawlConfig.log("%s checksummed" % item.path)
                elif ilist == "skipped":
                    CrawlConfig.log("%s skipped" % item.path)
                elif ilist == "unavailable":
                    CrawlConfig.log("HPSS is not available")
                    break
                else:
                    CrawlConfig.log("unexpected string returned " +
                              "from Checkable: '%s'" % ilist)
            elif type(ilist) == list:
                CrawlConfig.log("in %s, found:" % item)
                for n in ilist:
                    CrawlConfig.log(">>> %s" % str(n))
                    if 'f' == n.type and n.checksum != 0:
                        CrawlConfig.log(".. previously checksummed")
                        # checksums += 1
            elif isinstance(ilist, Checkable.Checkable):
                CrawlConfig.log("Checkable returned - file checksummed - %s, %s" %
                          (ilist.path, ilist.checksum))
                # checksums += 1
            elif isinstance(ilist, Alert.Alert):
                CrawlConfig.log("Alert generated: '%s'" %
                          ilist.msg())
                failures += 1
            else:
                CrawlConfig.log("unexpected return val from " +
                          "Checkable.check: %s: %r" % (type(ilist), ilist))

    # Report the statistics in the log
    # ** For checksums, we report the current total minus the previous
    # ** For matches and failures, we counted them up during the iteration
    # ** See the description of update_stats for why we don't store total
    #    checksums
    p_checksums = t_checksums
    t_matches += matches
    t_failures += failures
    update_stats((t_matches, t_failures))

    (t_checksums, t_matches, t_failures) = get_stats()
    CrawlConfig.log("files checksummed: %d; " % (t_checksums - p_checksums) +
              "checksums matched: %d; " % matches +
              "failures: %d" % failures)
    CrawlConfig.log("totals checksummed: %d; " % t_checksums +
              "matches: %d; " % t_matches +
              "failures: %d" % t_failures)

    # Report the dimension data in the log
    d = Dimension.Dimension(name='cos')
    CrawlConfig.log(d.report())

stats_table = 'cvstats'
# -----------------------------------------------------------------------------
def get_stats():
    """
    Return the number of files checksummed, checksums matched, and checksums
    failed.

    Matches and failures are stored in the cvstats table but total checksum
    count is retrieved from the checkables table by counting records with
    checksum = 1. This avoids discrepancies where the checksum count in cvstats
    might get out of synch with the records in checkables.
    """
    db = CrawlDBI.DBI()
    if db.table_exists(table="checkables"):
        rows = db.select(table='checkables',
                         fields=["count(path)"],
                         where="checksum = 1")
        checksums = rows[0][0]
    else:
        checksums = 0

    if db.table_exists(table=stats_table):
        rows = db.select(table=stats_table,
                         fields=["matches", "failures"],
                         where="rowid = 1")
        (matches, failures) = rval = rows[0]
    else:
        (matches, failures) = (0, 0)
    
    db.close()
    return (checksums, matches, failures)

# -----------------------------------------------------------------------------
def update_stats(cmf):
    """
    Record the values in tuple cmf in table cvstats in the database. If the
    table does not exist, create it.
    """
    db = CrawlDBI.DBI()
    if not db.table_exists(table=stats_table):
        db.create(table=stats_table,
                  fields=["rowid int",
                          "matches int",
                          "failures int",])
        db.insert(table=stats_table,
                  fields=["rowid", "matches", "failures"],
                  data=[(1, 0, 0)])

    db.update(table=stats_table,
              fields=["matches", "failures"],
              data=[cmf],
              where="rowid = 1")
    db.close()
                  
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    cfg = CrawlConfig.get_config()
    main(cfg)
