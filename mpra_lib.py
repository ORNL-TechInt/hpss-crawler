import CrawlConfig
import CrawlDBI
import re
import sys
import tcc_common
import time
import util

# -----------------------------------------------------------------------------
def age(table, age=None, count=False, output=None, path=False):
    """
    Retrieve and return (count of) records past a certain age.
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

    cfg.set('dbi', 'dbtype', 'db2')
    cfg.set('dbi', 'tbl_prefix', 'hpss')
    if util.hostname() == 'hpss-dev01':
        cfg.set('dbi', 'dbname', 'subsys')
    elif util.hostname() == 'hpss-crawler01':
        cfg.set('dbi', 'dbname', 'hsubsys1')

    if age is None or age == '':
        age = cfg.get('mpra', 'age')
    age_epoch = int(time.time()) - age_seconds(age)

    db = CrawlDBI.DBI(cfg=cfg)
    dbargs = {'where': 'record_create_time < ?',
              'data': (age_epoch,)}
    if count:
        dbargs['fields'] = ['count(*)']

    try:
        dbargs['table'] = {'migr': 'bfmigrrec',
                           'purge': 'bfpurgerec'}[table]
    except KeyError:
        dbargs['table'] = 'bfmigrrec'

    rows = db.select(**dbargs)
    age_report(table, age, count, rows, f, path)

    if opened:
        f.close()

# -----------------------------------------------------------------------------
def age_report(table, age, count, result, f, path=False):

    if count:
        f.write("%s records older than %s: %d\n"
                % (table, age, result[0]['1']))
    elif table == 'migr':
        f.write("Migration Records Older Than %s\n" % age)
        f.write("%-67s %-18s %s\n" % ("BFID", "Created", "MigrFails"))
        for row in result:
            f.write("%s %s %d\n" % (CrawlDBI.DBIdb2.hexstr(row['BFID']),
                                    util.ymdhms(row['RECORD_CREATE_TIME']),
                                    row['MIGRATION_FAILURE_COUNT']))
            if path:
                path = tcc_common.get_bitfile_path(row['BFID'])
                f.write("   %s\n" % path)
    elif table == 'purge':
        f.write("Purge Records Older Than %s" % age)
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

