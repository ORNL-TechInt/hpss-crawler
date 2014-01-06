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
    # clog = util.get_logger()
    util.log("checksum-verifier: firing up")
    hsi_prompt = "]:"
    plugdir = cfg.get('crawler', 'plugin-dir')
    dataroot = util.csv_list(cfg.get('checksum-verifier', 'dataroot'))
    # dbfilename = cfg.get('checksum-verifier', 'dbfile')
    odds = cfg.getfloat('checksum-verifier', 'odds')
    n_ops = int(cfg.get('checksum-verifier', 'operations'))

    # Checkable.Checkable.set_dbname(dbfilename)
    
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
            util.log("checksum-verifier: calling ex_nihilo")
            Checkable.Checkable.ex_nihilo(dataroot=dataroot)
            clist = Checkable.Checkable.get_list(odds)
        else:
            raise
    except StandardError, e:
        if 'Please call .ex_nihilo()' in str(e):
            util.log("checksum-verifier: calling ex_nihilo")
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
            util.log("checksum-verifier: [%d] checking %s" %
                      (item.rowid, item))
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
                    util.log("checksum-verifier: dir %s not accessible" %
                              item.path)
                    # clist.remove(item)
                elif ilist == "matched":
                    matches += 1
                    util.log("checksum-verifier: %s checksums matched" %
                              item.path)
                elif ilist == "checksummed":
                    checksums += 1
                    util.log("checksum-verifier: %s checksummed" % item.path)
                elif ilist == "skipped":
                    util.log("checksum-verifier: %s skipped" % item.path)
                elif ilist == "unavailable":
                    util.log("checksum-verifier: HPSS is not available")
                    break
                else:
                    util.log("checksum-verifier: unexpected string returned " +
                              "from Checkable: '%s'" % ilist)
            elif type(ilist) == list:
                util.log("checksum-verifier: in %s, found:" % item)
                util.log("checksum-verifier: %s" % str(ilist))
                for n in ilist:
                    util.log("checksum-verifier: >>> %s" % str(n))
                    if 'f' == n.type and n.checksum != 0:
                        util.log("checksum-verifier: ..... checksummed")
                        checksums += 1
            elif isinstance(ilist, Checkable.Checkable):
                util.log("checksum-verifier: Checkable returned - file checksummed - %s, %s" %
                          (ilist.path, ilist.checksum))
                checksums += 1
            elif isinstance(ilist, Alert.Alert):
                util.log("checksum-verifier: Alert generated: '%s'" %
                          ilist.msg())
                failures += 1
            else:
                util.log("checksum-verifier: unexpected return val from " +
                          "Checkable.check: %s: %r" % (type(ilist), ilist))

    # Report the statistics in the log
    util.log("checksum-verifier: files checksummed: %d; " % checksums +
              "checksums matched: %d; " % matches +
              "failures: %d" % failures)
    t_checksums += checksums
    t_matches += matches
    t_failures += failures
    util.log("checksum-verifier: totals checksummed: %d; " % t_checksums +
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
    pdb.set_trace()
    cfg = CrawlConfig.get_config()
    main(cfg)
