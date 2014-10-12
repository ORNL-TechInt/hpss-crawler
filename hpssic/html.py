#!/usr/bin/env python

import crawl_lib
import CrawlConfig
import CrawlDBI
import base64
import optparse
import pdb
import rpt_lib
import time
import toolframe
import util


# -----------------------------------------------------------------------------
def htmp_report(args):
    """report - write a sample report to stdout
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(args)

    if o.debug:
        pdb.set_trace()

    print html_lib.get_report()


# -----------------------------------------------------------------------------
def htmp_simplug(args):
    """simplug - simulate the plugin

    usage: rpt simplug

    """
    crawl_lib.simplug('rpt', args)
