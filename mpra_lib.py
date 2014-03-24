import CrawlConfig
import CrawlDBI
import re
import time
import util

# -----------------------------------------------------------------------------
def age(table, age=None, count=False):
    """
    Retrieve and return (count of) records past a certain age.
    """
    cfg = CrawlConfig.get_config()
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
    if count:
        return rows[0]['1']
    else:
        return rows

# -----------------------------------------------------------------------------
def age_seconds(agespec):
    """
    Convert a specification like 10S, 5 M, 7d, etc., to a number of seconds
    """
    mult = {'S': 1, 'M': 60, 'H': 3600,
            'd': 3600*24, 'm': 30*3600*24, 'Y': 365*3600*24}
    [(mag, unit)] = re.findall("\s*(\d+)\s*(S|M|H|d|m|Y)", agespec)
    return int(mag) * mult[unit]

