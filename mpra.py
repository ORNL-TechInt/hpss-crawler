#!/usr/bin/env python
import crawl_lib
import CrawlConfig
import CrawlDBI
import mpra_lib
import optparse
import pdb
import re
import sys
import time
import toolframe
import util

# -----------------------------------------------------------------------------
def mpra_age(args):
    """age - list the records in table BFMIGRREC or BFPURGEREC older than age

    usage: mpra age -t [migr|purge] -a/--age N[S|M|H|d|m|Y] [-c/--count]

    Report migration records (or a count of them) older than the age indicated.

    --age N        -- report records older than N
    --before D     -- report records from before date D
    --start S      -- report records with timestamps larger than S
    --end E        -- report recs with timestampes smaller than E
    """
    p = optparse.OptionParser()
    p.add_option('-a', '--age',
                 action='store', default='', dest='age',
                 help='report records older than this')
    p.add_option('-b', '--before',
                 action='store', default='', dest='before',
                 help='report records from before this epoch')
    p.add_option('-c', '--count',
                 action='store_true', default=False, dest='count',
                 help='report record counts rather than records')
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-e', '--end',
                 action='store', default='', dest='end',
                 help='ending epoch time')
    p.add_option('-p', '--path',
                 action='store_true', default=False, dest='path',
                 help='report paths as well as bitfile IDs')
    p.add_option('-s', '--start',
                 action='store', default='0', dest='start',
                 help='starting epoch time')
    p.add_option('-t', '--table',
                 action='store', default='', dest='table',
                 help='which table to age')
    (o, a) = p.parse_args(args)

    if o.debug: pdb.set_trace()

    cfg = CrawlConfig.get_config()
    if o.age and o.before:
        raise StandardError("--age and --before are mutually exclusive")
    elif o.age and '' != o.end:
        raise StandardError("--age and --end are mutually exclusive")
    elif o.before and '' != o.end:
        raise StandardError("--before and --end are mutually exclusive")
    elif o.before:
        end = time.mktime(time.strptime(o.before, "%Y.%m%d"))
    elif o.age:
        end = time.time() - cfg.to_seconds(o.age)
    elif o.end:
        end = int(o.end)

    start = int(o.start)
    if o.table == '':
        o.table = 'migr'

    print("%d, %d" % (start, end))
    result = mpra_lib.age(o.table, start, end, o.count, sys.stdout, path=o.path)

# -----------------------------------------------------------------------------
def mpra_date_age(args):
    """date_age - convert a date in the past to an age from now

    usage: mpra date_age YYYY.mmdd
                                 
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(args)

    if o.debug: pdb.set_trace()

    format_l = ['%Y.%m%d %H:%M:%S',
                '%Y.%m%d %H:%M',
                '%Y.%m%d %H',
                '%Y.%m%d',]
    done = False
    fmt = format_l.pop(0)
    while not done:
        try:
            then = int(time.mktime(time.strptime(a[0], fmt)))
            done = True
        except ValueError:
            try:
                fmt = format_l.pop(0)
            except IndexError:
                print("Can't parse date/time '%s'" % a[0])
                sys.exit(1)
    
    # then = int(time.mktime(time.strptime(a[0], "%Y.%m%d")))
    now = int(time.time())
    age = now - then
    print("days: %d" % (age/(24*3600)))
    print("hours: %d" % (age/3600))
    print("minutes: %d" % (age/60))
    print("seconds: %d" % (age))
    print("%s" % mpra_lib.dhms(age))

# -----------------------------------------------------------------------------
def mpra_epoch(args):
    """epoch - convert an epoch time to YYYY.mmdd HH:MM:SS

    usage: mpra epoch 1327513752 ...
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(args)

    if o.debug: pdb.set_trace()

    for epoch in a:
        print(time.strftime("%Y.%m%d %H:%M:%S", time.localtime(float(epoch))))
    
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
    if util.hostname() == 'hpss-dev01':
        dbname = 'subsys'
    elif util.hostname() == 'hpss-crawler01':
        dbname = 'hsubsys1'

    db = CrawlDBI.DBI(dbtype='db2', dbname=dbname)

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
def mpra_times(args):
    """times - list (unique) record create times in table BFMIGRREC

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
    p.add_option('-u', '--unique',
                 action='store', default='', dest='unique',
                 help='count unique timestamps')
    (o, a) = p.parse_args(args)

    if o.debug: pdb.set_trace()

    if util.hostname() == 'hpss-dev01':
        dbname = 'subsys'
    elif util.hostname() == 'hpss-crawler01':
        dbname = 'hsubsys1'
        
    if o.unique:
        fields = ['unique(record_create_time)']
    else:
        fields = ['record_create_time']

    db = CrawlDBI.DBI(dbtype='db2', dbname=dbname)
    rows = db.select(table='bfmigrrec',
                     fields=fields,
                     orderby='record_create_time')

    last = ''
    count = 0
    for row in rows:
        ymd = util.ymdhms(row['RECORD_CREATE_TIME'])[0:10]
        count += 1
        if ymd != last:
            if last != '':
                print("%s (%d)" % (last, count))
            last = ymd
            count = 1
    if 0 < count:
        print("%s (%d)" % (last, count))

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
def mpra_reset(args):
    """reset - drop the mpra table and remove mpra_report.txt

    usage: mpra reset

    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(args)

    if o.debug: pdb.set_trace()

    cfg = CrawlConfig.get_config()
    db = CrawlDBI.DBI()
    db.drop(table='mpra')

    filename = cfg.get('mpra', 'report_file')
    util.conditional_rm(filename)

# -----------------------------------------------------------------------------
def mpra_simplug(args):
    """simplug - simulate the plugin

    usage: mpra simplug

    """
    crawl_lib.simplug('mpra', args)

# -----------------------------------------------------------------------------
toolframe.tf_launch('mpra', __name__)
