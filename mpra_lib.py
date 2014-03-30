import CrawlConfig
import CrawlDBI
import re
import sys
import tcc_common
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

    try:
        dbargs['table'] = {'migr': 'bfmigrrec',
                           'purge': 'bfpurgerec'}[table]
    except KeyError:
        dbargs['table'] = 'bfmigrrec'

    rows = db.select(**dbargs)
    age_report(table, int(time.time()) - end, count, rows, f, path)

    if mark:
        recent = 0
        for row in rows:
            if recent < row['RECORD_CREATE_TIME']:
                recent = row['RECORD_CREATE_TIME']
        mpra_record_recent(table, recent)

    if opened:
        f.close()

# -----------------------------------------------------------------------------
def age_report(table, age, count, result, f, path=False):
    """
    mark: If True, record the most recent record reported in our scribble
    database
    """
    if count:
        f.write("%s records older than %s: %d\n"
                % (table, age, result[0]['1']))
    elif table == 'migr':
        f.write("Migration Records Older Than %s Seconds\n" % age)
        f.write("%-67s %-18s %s\n" % ("BFID", "Created", "MigrFails"))
        for row in result:
            f.write("%s %s %d\n" % (CrawlDBI.DBIdb2.hexstr(row['BFID']),
                                    util.ymdhms(row['RECORD_CREATE_TIME']),
                                    row['MIGRATION_FAILURE_COUNT']))
            if path:
                path = tcc_common.get_bitfile_path(row['BFID'])
                f.write("   %s\n" % path)
    elif table == 'purge':
        f.write("Purge Records Older Than %s Seconds" % age)
        f.write("%-67s %-18s\n" % ("BFID", "Created"))
        for row in result:
            f.write("%s %s\n" % (CrawlDBI.DBIdb2.hexstr(row['BFID']),
                                    util.ymdhms(row['RECORD_CREATE_TIME'])))
            if path:
                path = tcc_common.get_bitfile_path(row['BFID'])
                f.write("   %s\n" % path)

# -----------------------------------------------------------------------------
def age_seconds(agespec):
    """
    Convert a specification like 10S, 5 M, 7d, etc., to a number of seconds
    """
    mult = {'S': 1, 'M': 60, 'H': 3600,
            'd': 3600*24, 'm': 30*3600*24, 'Y': 365*3600*24}
    [(mag, unit)] = re.findall("\s*(\d+)\s*(S|M|H|d|m|Y)", agespec)
    return int(mag) * mult[unit]

# -----------------------------------------------------------------------------
def mpra_record_recent(type, recent):
    """
    Record the most recent record reported so we don't report records
    repeatedly
    """
    db = CrawlDBI.DBI()
    if not db.table_exists(table='mpra'):
        util.log("Creating mpra table")
        db.create(table='mpra',
                  fields=['recent_time   integer',
                          'type          text'])
    rows = db.select(table='mpra',
                     fields=['recent_time'],
                     where='type = ?',
                     data=(type,))
    if len(rows) < 1:
        util.log("Insert into mpra table: (%s, %d)" % (type, recent))
        db.insert(table='mpra',
                  fields=['type', 'recent_time'],
                  data=[(type, recent)])
    else:
        util.log("Update mpra table with (%s, %d)" % (type, recent))
        db.update(table='mpra',
                  fields=['recent_time'],
                  where='type = ?',
                  data=[(recent, type)])

# -----------------------------------------------------------------------------
def mpra_fetch_recent(type):
    """
    Retrieve and return the most recent record reported so we don't report the
    same record repeatedly
    """
    db = CrawlDBI.DBI()
    if not db.table_exists(table='mpra'):
        util.log("Fetch from not existent mpra table -- return 0")
        return 0

    rows = db.select(table='mpra',
                     fields=['max(recent_time)'],
                     where='type = ?',
                     data=(type,))

    if rows[0][0] is None:
        util.log("No '%s' value in mpra -- returning 0" % type)
        return 0
    else:
        util.log("Fetch '%s' from mpra table -- return %d" %
                 (type, rows[0][0]))
        return rows[0][0]
