import CrawlConfig
import CrawlDBI
import pdb
import re
import sys
import tcc_lib
import time
import util

# -----------------------------------------------------------------------------
def age(table,
        start=None,
        end=None,
        count=False,
        output=None,
        path=False,
        mark=False):
    """
    Retrieve and return (count of) records older than end and younger than
    start. The result is written to output. If path is True, age_report will
    compute the bitfile pathname and report it as well. If mark is True, we
    update the mpra table with the date/time of the newest record reported.

    Strict less than compares is the right thing to do. We record the last time
    reported in the mpra recent table. We've reported all the records with that
    time. We're looking into the past, so any new records added cannot have
    that time -- they're being added in the present when timestamps have larger
    values. So we want to start with the next one after the last one reported.
    """
    cfg = CrawlConfig.get_config()
    opened = True
    if output is None:
        f = open(cfg.get('mpra', 'report_file'), 'a')
    elif type(output) == str:
        f = open(output, 'a')
    elif type(output) == file:
        f = output
        opened = False
    else:
        raise StandardError("output type must be 'str' or 'file' ")

    if util.hostname() == 'hpss-dev01':
        dbname = 'subsys'
    elif util.hostname() == 'hpss-crawler01':
        dbname = 'hsubsys1'

    db = CrawlDBI.DBI(dbtype='db2', dbname=dbname)
    if start is not None and end is not None:
        dbargs = {'where': '? < record_create_time and record_create_time < ?',
                  'data': (start, end)}
    elif start is None and end is not None:
        dbargs = {'where': 'record_create_time < ?',
                  'data': (end, )}
    elif start is not None and end is None:
        dbargs = {'where': '? < record_create_time',
                  'data': (start, )}
    # if both start and end are None, we leave the select unconstrained
    
    if count:
        dbargs['fields'] = ['count(*)']
    else:
        dbargs['orderby'] = 'record_create_time'

    try:
        dbargs['table'] = {'migr': 'bfmigrrec',
                           'purge': 'bfpurgerec'}[table]
    except KeyError:
        dbargs['table'] = 'bfmigrrec'

    rows = db.select(**dbargs)
    recent = 0
    rval = len(rows)
    if count:
        age_report(table, int(time.time()) - end, count, rows, f, path)
    elif 0 < len(rows):
        for row in rows:
            if recent < row['RECORD_CREATE_TIME']:
                recent = row['RECORD_CREATE_TIME']

        age_report(table, int(time.time()) - recent, count, rows, f, path)

    if mark:
        mpra_record_recent(table,
                           start,
                           recent if 0 < recent else end,
                           len(rows))

    if opened:
        f.close()

    return rval

# -----------------------------------------------------------------------------
def age_report(table, age, count, result, f, path=False):
    """
    Generate the report based on the data passed in
    """
    if count:
        f.write("%s records older than %s: %d\n"
                % (table, dhms(age), result[0]['1']))
    elif table == 'migr':
        f.write("Migration Records Older Than %s\n" % dhms(age))
        f.write("%-67s %-18s %s\n" % ("BFID", "Created", "MigrFails"))
        for row in result:
            f.write("%s %s %9d\n" % (CrawlDBI.DBIdb2.hexstr(row['BFID']),
                                    util.ymdhms(row['RECORD_CREATE_TIME']),
                                    row['MIGRATION_FAILURE_COUNT']))
            if path:
                path = tcc_lib.get_bitfile_path(row['BFID'])
                f.write("   %s\n" % path)
    elif table == 'purge':
        f.write("Purge Records Older Than %s\n" % dhms(age))
        f.write("%-67s %-18s\n" % ("BFID", "Created"))
        for row in result:
            f.write("%s %s\n" % (CrawlDBI.DBIdb2.hexstr(row['BFID']),
                                    util.ymdhms(row['RECORD_CREATE_TIME'])))
            if path:
                path = tcc_lib.get_bitfile_path(row['BFID'])
                f.write("   %s\n" % path)

# -----------------------------------------------------------------------------
def idivrem(secs, div):
    return(int(secs)/div, int(secs) % div)

# -----------------------------------------------------------------------------
def dhms(age_s):
    (days, rem) = idivrem(age_s, 24*3600)
    (hours, rem) = idivrem(rem, 3600)
    (minutes, seconds) = idivrem(rem, 60)
    return("%dd-%02d:%02d:%02d" % (days, hours, minutes, seconds))

# -----------------------------------------------------------------------------
def mpra_record_recent(type, start, end, hits):
    """
    Record the most recent record reported so we don't report records
    repeatedly. However, if recent is not later than the time already stored,
    we don't want to update it.
    """
    db = CrawlDBI.DBI()
    if not db.table_exists(table='mpra'):
        CrawlConfig.log("Creating mpra table")
        db.create(table='mpra',
                  fields=['type          text',
                          'scan_time     integer',
                          'start_time    integer',
                          'end_time      integer',
                          'hits          integer'])

    db.insert(table='mpra',
              fields=['type', 'scan_time', 'start_time', 'end_time', 'hits'],
              data=[(type, int(time.time()), int(start), int(end), hits)])

# -----------------------------------------------------------------------------
def mpra_fetch_recent(type):
    """
    Retrieve and return the most recent record reported so we don't report the
    same record repeatedly
    """
    db = CrawlDBI.DBI()
    if not db.table_exists(table='mpra'):
        CrawlConfig.log("Fetch from not existent mpra table -- return 0")
        return 0

    rows = db.select(table='mpra',
                     fields=['scan_time, end_time'],
                     where='type = ?',
                     data=(type,))
    last_end_time = -1
    max_scan_time = 0
    for r in rows:
        if max_scan_time < r[0]:
            max_scan_time = r[0]
            last_end_time = r[1]
            

    if last_end_time < 0:
        CrawlConfig.log("No '%s' value in mpra -- returning 0" % type)
        return 0
    else:
        CrawlConfig.log("Fetch '%s' from mpra table -- return %d" %
                 (type, last_end_time))
        return last_end_time

# -----------------------------------------------------------------------------
def xplocks(output=None, mark=False):
    """
    Look for expired purge locks in bfpurgerec.
    """
    cfg = CrawlConfig.get_config()
    now = time.time()
    hits = 0

    opened = True
    if output is None:
        f = open(cfg.get('mpra', 'report_file'), 'a')
    elif type(output) == str:
        f = open(output, 'a')
    elif type(output) == file:
        f = output
        opened = False
    else:
        raise StandardError("output type must be 'str' or 'file' ")

    if util.hostname() == 'hpss-dev01':
        dbname_s = 'subsys'
    elif util.hostname() == 'hpss-crawler01':
        dbname_s = 'hsubsys1'

    dbs = CrawlDBI.DBI(dbtype='db2', dbname=dbname_s)

    lock_min = cfg.getint('mpra', 'lock_duration')

    rows = dbs.select(table='bfpurgerec',
                      fields=['bfid', 'record_lock_time'],
                      where='record_lock_time <> 0')
    if 0 < len(rows):
        f.write("Expired Purge Locks\n")
        for r in rows:
            if (lock_min * 60) < (now - r['RECORD_LOCK_TIME']):
                hits += 1
                f.write("   %s  %s\n" % (CrawlDBI.DBI.db2.hexstr(r['BFID']),
                                         util.ymdhms(r['RECORD_LOCK_TIME'])))

    if mark:
        mpra_record_recent('purge', 0, 0, hits)
        
    if opened:
        f.close()

    return hits
