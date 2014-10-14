#!/usr/bin/env python

import crawl_lib
import CrawlConfig
import CrawlDBI
import base64
import html_lib
import optparse
import pdb
import rpt_lib
import time
import toolframe
import util


# -----------------------------------------------------------------------------
def htmp_report(args):
    """report - write a sample report to stdout

    usage: html report [-c config]
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-c', '--config',
                 action='store', default='', dest='config',
                 help='which config file to use')
    (o, a) = p.parse_args(args)

    if o.debug:
        pdb.set_trace()

    print html_lib.get_html_report(o.config)


# -----------------------------------------------------------------------------
def htmp_simplug(args):
    """simplug - simulate the plugin

    usage: html simplug

    """
    crawl_lib.simplug('html', args)
