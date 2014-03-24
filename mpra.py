#!/usr/bin/env python
import CrawlConfig
import CrawlDBI
import mpra_lib
import optparse
import pdb
import re
import time
import toolframe
import util

# -----------------------------------------------------------------------------
def mpra_age(args):
    """age - list the records in table BFMIGRREC or BFPURGEREC older than age

    usage: mpra age -t [migr|purge] -a/--age N[S|M|H|d|m|Y] [-c/--count]

    Report migration records (or a count of them) older than the age indicated.
    """
    p = optparse.OptionParser()
    p.add_option('-a', '--age',
                 action='store', default='', dest='age',
                 help='report records older than this')
    p.add_option('-c', '--count',
                 action='store_true', default=False, dest='count',
                 help='report record counts rather than records')
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-t', '--table',
                 action='store', default='', dest='table',
                 help='which table to age')
    (o, a) = p.parse_args(args)

    if o.debug: pdb.set_trace()
    if o.table == '':
        o.table = 'migr'
        
    result = mpra_lib.age(o.table, o.age, o.count)
    if o.count:
        print("Records found: %d" % result)
    elif o.table.lower() != 'purge':
        for row in result:
            print("%s %s %d" % (CrawlDBI.DBIdb2.hexstr(row['BFID']),
                                util.ymdhms(row['RECORD_CREATE_TIME']),
                                row['MIGRATION_FAILURE_COUNT']))
    else:
        for row in result:
            print("%s %s" % (CrawlDBI.DBIdb2.hexstr(row['BFID']),
                             util.ymdhms(row['RECORD_CREATE_TIME'])))

# -----------------------------------------------------------------------------
def mpra_migr_recs(args):
    """migr_recs - list the records in table BFMIGRREC

    usage: mpra migr_recs [-l/limit N]
                          [-b/--before DATE-TIME]
                          [-a/--after DATE-TIME]
                                 
    with -l N, only report the first N records

    with -b DATE-TIME, only report the records with create times before
    DATE-TIME.

    with -a DATE-TIME, only report the records with create times after
    DATE-TIME.
    """
    p = optparse.OptionParser()
    p.add_option('-c', '--count',
                 action='store_true', default=False, dest='count',
                 help='report record counts rather than records')
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-l', '--limit',
                 action='store', default='', dest='limit',
                 help='how many records to fetch')
    p.add_option('-b', '--before',
                 action='store', default='', dest='before',
                 help='fetch records from before the date/time')
    p.add_option('-a', '--after',
                 action='store', default='', dest='after',
                 help='fetch records from after the date/time')
    (o, a) = p.parse_args(args)

    if o.debug: pdb.set_trace()

    cfg = CrawlConfig.get_config()
    cfg.set('dbi', 'dbtype', 'db2')
    cfg.set('dbi', 'tbl_prefix', 'hpss')
    if util.hostname() == 'hpss-dev01':
        cfg.set('dbi', 'dbname', 'subsys')
    elif util.hostname() == 'hpss-crawler01':
        cfg.set('dbi', 'dbname', 'hsubsys1')

    db = CrawlDBI.DBI(cfg=cfg)

    dbargs = {'table': 'bfmigrrec'}
    
    if o.limit == '' and o.before == '' and o.after == '':
        dbargs['limit'] = 30

    elif o.limit == '' and o.before == '' and o.after != '':
        dbargs['where'] = '? < record_create_time'
        dbargs['data'] = (util.epoch(o.after),)
        
    elif o.limit == '' and o.before != '' and o.after == '':
        dbargs['where'] = 'record_create_time < ?'
        dbargs['data'] = (util.epoch(o.before),)

    elif o.limit == '' and o.before != '' and o.after != '':
        dbargs['where'] = '? < record_create_time and record_create_time < ?'
        dbargs['data'] = (util.epoch(o.after), util.epoch(o.before))
        
    elif o.limit != '' and o.before == '' and o.after == '':
        dbargs['limit'] = int(o.limit)

    elif o.limit != '' and o.before == '' and o.after != '':
        dbargs['limit'] = int(o.limit)
        dbargs['where'] = '? < record_create_time'
        dbargs['data'] = (util.epoch(o.after),)
        
    elif o.limit != '' and o.before != '' and o.after == '':
        dbargs['limit'] = int(o.limit)
        dbargs['where'] = 'record_create_time < ?'
        dbargs['data'] = (util.epoch(o.before),)

    elif o.limit != '' and o.before != '' and o.after != '':
        dbargs['limit'] = int(o.limit)
        dbargs['where'] = '? < record_create_time and record_create_time < ?'
        dbarsg['data'] = (util.epoch(o.after), util.epoch(o.before))

    if o.count:
        dbargs['fields'] = ['count(*)']
        
    rows = db.select(**dbargs)
    for row in rows:
        if o.count:
            print("Records found: %d" % row['1'])
        else:
            print("%s %s %d" % (CrawlDBI.DBIdb2.hexstr(row['BFID']),
                                util.ymdhms(row['RECORD_CREATE_TIME']),
                                row['MIGRATION_FAILURE_COUNT']))

# -----------------------------------------------------------------------------
def mpra_unique_times(args):
    """unique_times - list unique record create times in table BFMIGRREC

    usage: mpra unique_times
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-l', '--limit',
                 action='store', default='', dest='limit',
                 help='how many records to fetch')
    p.add_option('-b', '--before',
                 action='store', default='', dest='before',
                 help='fetch records from before the date/time')
    p.add_option('-a', '--after',
                 action='store', default='', dest='after',
                 help='fetch records from after the date/time')
    (o, a) = p.parse_args(args)

    if o.debug: pdb.set_trace()

    cfg = CrawlConfig.get_config()
    cfg.set('dbi', 'dbtype', 'db2')
    cfg.set('dbi', 'tbl_prefix', 'hpss')
    if util.hostname() == 'hpss-dev01':
        cfg.set('dbi', 'dbname', 'subsys')
    elif util.hostname() == 'hpss-crawler01':
        cfg.set('dbi', 'dbname', 'hsubsys1')

    db = CrawlDBI.DBI(cfg=cfg)
    rows = db.select(table='bfmigrrec',
                     fields=['unique(record_create_time)'])
                     
    for row in rows:
        print("%s" % (util.ymdhms(row['RECORD_CREATE_TIME'])))

# -----------------------------------------------------------------------------
def mpra_purge_recs(args):
    """purge_recs - list the records in table BFPURGEREC

    usage: mpra purge_recs [-l N]

    with -l N, only report the first N records
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-D', '--db',
                 action='store', default='', dest='dbsect',
                 help='which database to access')
    (o, a) = p.parse_args(args)

    if o.debug: pdb.set_trace()

# -----------------------------------------------------------------------------
toolframe.tf_launch('mpra', __name__)
