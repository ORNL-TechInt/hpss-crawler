import Checkable
import ConfigParser
import CrawlConfig
import os
import pexpect
import sqlite3 as sql
import sys
import time

def main(cfg):
    clog = sys.modules['__main__'].get_logger()
    # clog.info("drill-instructor: cwd = %s" % os.getcwd())
    hsi_prompt = "]:"

    plugdir = cfg.get('crawler', 'plugin-dir')
    dataroot = cfg.get('drill-instructor', 'dataroot')
    dbfilename = cfg.get('drill-instructor', 'dbfile')
    odds = cfg.get('drill-instructor', 'odds')
    # clog.info("drill-instructor: dataroot = %s" % dataroot)
    # clog.info("drill-instructor: dbfile = %s" % dbfilename)

    (t_checksums, t_matches, t_failures) = get_stats(dbfilename)
    (checksums, matches, failures) = (0, 0, 0)
    try:
        clist = Checkable.Checkable.get_list()
    except StandardError, e:
        if 'Please call .ex_nihilo()' in str(e):
            Checkable.Checkable.ex_nihilo(filename=dbfilename,
                                          dataroot=dataroot)
            clist = Checkable.Checkable.get_list()
        else:
            raise

    n_ops = int(cfg.get('drill-instructor', 'operations'))
        
    for op in range(n_ops):
        if 0 < len(clist):
            item = clist.pop(0)
            clog.info("drill-instructor: [%d] checking %s" %
                      (item.rowid, item.path))
            ilist = item.check(odds)
            if type(ilist) == str:
                if ilist == "matched":
                    matches += 1
                    clog.info("drill-instructor: %s checksums matched" % item.path)
                elif ilist == "skipped":
                    clog.info("drill-instructor: %s skipped" % item.path)
                elif ilist == "access denied":
                    clog.info("drill-instructor: dir %s not accessible" %
                              item.path)
                else:
                    clog.info("drill-instructor: unexpected string returned " +
                              "from Checkable: '%s'" % ilist)
            elif type(ilist) == list:
                clog.info("drill-instructor: in %s, found:" % item.path)
                for n in ilist:
                    clog.info("drill-instructor: >>> %s %s %s %f" %
                              (n.path, n.type, n.checksum,
                               n.last_check))
            elif isinstance(ilist, Checkable.Checkable):
                clog.info("drill-instructor: file checksummed - %s, %s" %
                          (ilist.path, ilist.checksum))
                checksums += 1
            elif isinstance(ilist, Alert.Alert):
                clog.info("drill-instructor: Alert generated: '%s'" %
                          ilist.msg())
                failures += 1
            else:
                clog.info("drill-instructor: unexpected return val from " +
                          "Checkable.check: %s: %r" % (type(ilist), ilist))

    clog.info("drill-instructor: files checksummed: %d; " % checksums +
              "checksums matched: %d; " % matches +
              "failures: %d" % failures)
    t_checksums += checksums
    t_matches += matches
    t_failures += failures
    clog.info("drill-instructor: totals checksummed: %d; " % t_checksums +
              "matches: %d; " % t_matches +
              "failures: %d" % t_failures)
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
