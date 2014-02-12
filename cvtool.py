#!/usr/bin/env python
import CrawlConfig
import CrawlDBI
import hpss
import optparse
import pdb
import pexpect
import toolframe

prefix = "cv"
H = None

# -----------------------------------------------------------------------------
def cv_report(argv):
    """cvreport - show the checksum verifier database status

    select count(*) from checkables where type = 'f';
    select count(*) from checkables where checksum <> 0;
    """
    p = optparse.OptionParser()
    p.add_option('-c', '--cfg',
                 action='store', default='', dest='config',
                 help='config file name')
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-p', '--prefix',
                 action='store', default='', dest='prefix',
                 help='table name prefix')
    p.add_option('-v', '--verbose',
                 action='store_true', default=False, dest='verbose',
                 help='pass verbose flag to HSI object')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug: pdb.set_trace()

    if o.config != '':
        cfgname = o.config
    else:
        cfgname = 'crawl.cfg'
    cfg = CrawlConfig.get_config(cfgname)

    if o.prefix != '':
        cfg.set('dbi', 'tbl_prefix', o.prefix)

    db = CrawlDBI.DBI(cfg=cfg)
    # d = {}
    pop = db.select(table="checkables",
                       fields=["cos", "count(*)"],
                       where="type = 'f'",
                       groupby="cos")
    # for r in pop:
    #     d[r[0]] = {'p_count': r[1]}
    d = dict((z[0], {'p_count': z[1]}) for z in pop)

    samp = db.select(table="checkables",
                     fields=["cos", "count(*)"],
                     where="checksum <> 0",
                     groupby="cos")
    # for r in samp:
    #     d[r[0]].update({'s_count': r[1]})
    map(lambda x: d[x[0]].update({'s_count': x[1]}), samp)

    ptot = sum(map(lambda x: x['p_count'], d.values()))
    stot = sum(map(lambda x: x['s_count'], d.values()))

    # for cos in d:
        # d[cos]['p_pct'] = 100.0 * d[cos]['p_count'] / ptot
        # d[cos]['s_pct'] = 100.0 * d[cos]['s_count'] / stot

    map(lambda x: x.update({'p_pct': 100.0 * x['p_count'] / ptot}), d.values())
    map(lambda x: x.update({'s_pct': 100.0 * x['s_count'] / stot}), d.values())
    
    print("%8s %8s %15s %15s" % ("Name",
                                 "Category",
                                 "==Population===",
                                 "====Sample====="))
    for cos in sorted(d.keys()):
        print("%8s %8s %7d %7.2f %7d %7.2f" % ('cos', cos,
                                               d[cos]['p_count'], d[cos]['p_pct'],
                                               d[cos]['s_count'], d[cos]['s_pct']))
    print("%8s %8s %7d %7s %7d" % (" ", "Total", ptot, " ", stot))
                                
    db.close()

# -----------------------------------------------------------------------------
def cv_nulltest(argv):
    """nulltest - how do NULL values show up when queried?

    After doing 'alter table dev_checkables add fails int', the added fields
    (fails, reported) are NULL, not 0. What does that look like when I select a
    record?
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-f', '--filename',
                 action='store', default='hpss_crawl.log', dest='filename',
                 help='name of log file')
    p.add_option('-v', '--verbose',
                 action='store_true', default=False, dest='verbose',
                 help='pass verbose flag to HSI object')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug: pdb.set_trace()

    db = CrawlDBI.DBI()

    rows = db.select(table='checkables',
                     where="rowid < 10")
    print rows
    db.close()
 
# -----------------------------------------------------------------------------
def cv_fail_reset(argv):
    """fail_reset - reset a failing path so it can be checked again

    After doing 'alter table dev_checkables add fails int', the added fields
    (fails, reported) are NULL, not 0. What does that look like when I select a
    record?
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-p', '--pathname',
                 action='store', default='', dest='pathname',
                 help='name of path to be reset')
    p.add_option('-v', '--verbose',
                 action='store_true', default=False, dest='verbose',
                 help='pass verbose flag to HSI object')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug: pdb.set_trace()

    if o.pathname == '':
        print("pathname is required")
        return
        
    db = CrawlDBI.DBI()

    db.update(table='checkables',
              fields=['fails', 'reported'],
              where="path = ?",
              data=[(0, 0, o.pathname)])

    db.close()
 
# -----------------------------------------------------------------------------
toolframe.tf_launch(prefix, __name__)
