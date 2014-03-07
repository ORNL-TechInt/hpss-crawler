#!/usr/bin/env python
import CrawlDBI
import optparse
import toolframe
import util

# -----------------------------------------------------------------------------
def mpra_migr_recs():
    """migr_recs - list the records in table BFMIGRREC

    usage: mpra migr_recs [-l N] [-b/--before DATE-TIME]
                                 [-a/--after DATE-TIME]
                                 
    with -l N, only report the first N records

    with -b DATE-TIME, only report the records with create times before
    DATE-TIME.

    with -a DATE-TIME, only report the records with create times after
    DATE-TIME.
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

    if util.hostname() == 'hpss-dev01':
        dbname = 'subsys'
    elif util.hostname() == 'hpss-crawler01':
        dbname = 'hsubsys1'
        
    db = CrawlDBI.DBI('db2', dbname=dbname)
    rows = db.select(table='bfmigrrec')
              
# -----------------------------------------------------------------------------
def mpra_purge_recs():
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
