#!/usr/bin/env python
import crawl_lib
import CrawlConfig
import CrawlDBI
import dbschem
import messages as MSG
import mpra_lib
import optparse
import pdb
import re
import rpt_lib
import sys
import time
import toolframe
import util


# -----------------------------------------------------------------------------
def mprf_age(args):
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
                 action='store', default='', dest='start',
                 help='starting epoch time')
    p.add_option('-t', '--table',
                 action='store', default='', dest='table',
                 help='which table to age')
    (o, a) = p.parse_args(args)

    if o.debug:
        pdb.set_trace()

    cfg = CrawlConfig.get_config()
    start = 0
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
        end = util.epoch(o.end)

    if o.start:
        start = util.epoch(o.start)
    if o.table == '':
        o.table = 'migr'

    print("%d, %d" % (start, end))
    mpra_lib.age(o.table, start, end, o.count, sys.stdout, path=o.path)


# -----------------------------------------------------------------------------
def mprf_date_age(args):
    """date_age - convert a date in the past to an age from now

    usage: mpra date_age YYYY.mmdd
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(args)

    if o.debug:
        pdb.set_trace()

    format_l = ['%Y.%m%d %H:%M:%S',
                '%Y.%m%d %H:%M',
                '%Y.%m%d %H',
                '%Y.%m%d',
                ]
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
def mprf_epoch(args):
    """epoch - convert a YYYY.mmdd HH:MM:SS to an epoch time

    usage: mpra epoch 2014.0201.10.27.53
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(args)

    if o.debug:
        pdb.set_trace()

    for ymd in a:
        print(int(util.epoch(ymd)))


# -----------------------------------------------------------------------------
def mprf_history(args):
    """history - report contents of the mpra table

    usage: mpra history [-s/--since] <date/time>
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-s', '--since',
                 action='store', default='', dest='since',
                 help='only report records since ...')

    (o, a) = p.parse_args(args)

    if o.debug:
        pdb.set_trace()

    n_since = util.epoch(o.since) if o.since else 0

    report = rpt_lib.get_mpra_report(last_rpt_time=n_since)
    print(report)


# -----------------------------------------------------------------------------
def mprf_ymd(args):
    """ymd - convert an epoch time to YYYY.mmdd HH:MM:SS

    usage: mpra ymd 1327513752 ...
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(args)

    if o.debug:
        pdb.set_trace()

    for epoch in a:
        print(time.strftime("%Y.%m%d %H:%M:%S", time.localtime(float(epoch))))


# -----------------------------------------------------------------------------
def mprf_migr_recs(args):
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

    if o.debug:
        pdb.set_trace()

    cfg = CrawlConfig.get_config()

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
    else:
        dbargs['fields'] = ['bfid',
                            'record_create_time',
                            'migration_failure_count']

    dbargs['orderby'] = 'record_create_time'

    rows = mpra_lib.lookup_migr_recs(**dbargs)
    for row in rows:
        if o.count:
            print("Records found: %d" % row['1'])
        else:
            print("%s %s %d" % (CrawlDBI.DBIdb2.hexstr(row['BFID']),
                                util.ymdhms(row['RECORD_CREATE_TIME']),
                                row['MIGRATION_FAILURE_COUNT']))


# -----------------------------------------------------------------------------
def mprf_times(args):
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

    if o.debug:
        pdb.set_trace()

    if o.unique:
        fields = ['unique(record_create_time)']
    else:
        fields = ['record_create_time']

    dbargs = {'table': 'bfmigrrec',
              'fields': fields,
              'orderby': 'record_creaet_time'}
    rows = mpra_lib.lookup_migr_recs(**dbargs)

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
def mprf_purge_recs(args):
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

    if o.debug:
        pdb.set_trace()


# -----------------------------------------------------------------------------
def mprf_reset(args):
    """reset - drop the mpra table and remove mpra_report.txt

    usage: mpra reset

    """
    p = optparse.OptionParser()
    p.add_option('-c', '--cfg',
                 action='store', default='', dest='config',
                 help='config file name')
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-f', '--force',
                 action='store_true', default=False, dest='force',
                 help='force the operation')
    (o, a) = p.parse_args(args)

    if o.debug:
        pdb.set_trace()

    if not o.force:
        answer = raw_input(MSG.all_mpra_data_lost)
        if answer[0].lower() != "y":
            raise SystemExit()

    cfg = CrawlConfig.get_config(o.config)

    dbschem.drop_table(cfg=cfg, table='mpra')

    filename = cfg.get('mpra', 'report_file')
    util.conditional_rm(filename)


# -----------------------------------------------------------------------------
def mprf_simplug(args):
    """simplug - simulate the plugin

    usage: mpra simplug

    Debugging (-d) is provided by crawl_lib.simplug()
    """
    crawl_lib.simplug('mpra', args)


# ------------------------------------------------------------------------------
def mprf_syspath(argv):
    """syspath - dump python's sys.path array

    usage: mpra syspath
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(argv)

    if o.debug:
        pdb.set_trace()

    for item in sys.path:
        print("    " + item)


# -----------------------------------------------------------------------------
def mprf_xplocks(args):
    """xplocks - run the xpired lock query against bfpurgerec

    usage: mpra xplocks
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(args)

    if o.debug:
        pdb.set_trace()

    mpra_lib.xplocks(output=sys.stdout)

# -----------------------------------------------------------------------------
toolframe.tf_launch('mpra', __name__)
