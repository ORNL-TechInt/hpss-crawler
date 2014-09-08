from hpssic import Alert
from hpssic import Checkable
from hpssic import CrawlConfig
from hpssic import CrawlDBI
from hpssic import cv_lib
from hpssic import Dimension
import os
import pdb
import pexpect
import sys
import time
from hpssic import util

plugin_name = 'cv'


# -----------------------------------------------------------------------------
def main(cfg):
    # Get stuff we need -- the logger object, dataroot, etc.
    CrawlConfig.log("firing up")
    plugdir = cfg.get('crawler', 'plugin-dir')
    dataroot = util.csv_list(cfg.get(plugin_name, 'dataroot'))
    odds = cfg.getfloat(plugin_name, 'odds')
    n_ops = int(cfg.get(plugin_name, 'operations'))

    # Initialize our statistics
    (t_checksums, t_matches, t_failures) = get_stats()
    (checksums, matches, failures) = (0, 0, 0)

    # Fetch the list of HPSS objects that we're looking at from the
    # database
    try:
        clist = Checkable.Checkable.get_list(odds, rootlist=dataroot)
    except CrawlDBI.DBIerror, e:
        if any([util.rgxin(msg, str(e))
                for msg in ["no such table: checkables",
                            "Table '.*' doesn't exist"]]):
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
                CrawlConfig.log("Checkable returned - file checksummed" +
                                " - %s, %s" % (ilist.path, ilist.checksum))
                # checksums += 1
            elif isinstance(ilist, Alert.Alert):
                CrawlConfig.log("Alert generated: '%s'" %
                                ilist.msg())
                failures += 1
            else:
                CrawlConfig.log("unexpected return val from " +
                                "Checkable.check: %s: %r" %
                                (type(ilist), ilist))

    # Report the statistics in the log
    # ** For checksums, we report the current total minus the previous
    # ** For matches and failures, we counted them up during the iteration
    # ** See the description of get_stats for why we don't store total
    #    checksums
    p_checksums = t_checksums
    t_matches += matches
    t_failures += failures
    cv_lib.update_stats((t_matches, t_failures))

    (t_checksums, t_matches, t_failures) = get_stats()
    CrawlConfig.log("files checksummed: %d; " % (t_checksums - p_checksums) +
                    "checksums matched: %d; " % matches +
                    "failures: %d" % failures)
    CrawlConfig.log("totals checksummed: %d; " % t_checksums +
                    "matches: %d; " % t_matches +
                    "failures: %d" % t_failures)

    # Report the dimension data in the log
    d = Dimension.Dimension(name='cos')
    t = Dimension.Dimension(name='cart')
    CrawlConfig.log(d.report())
    CrawlConfig.log(t.report())


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
    checksums = cv_lib.get_checksum_count()
    (matches, failures) = cv_lib.get_match_fail_count()
    return(checksums, matches, failures)


# -----------------------------------------------------------------------------
if __name__ == '__main__':
    cfg = CrawlConfig.get_config()
    main(cfg)
