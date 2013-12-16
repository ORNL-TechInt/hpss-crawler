#!/usr/bin/env python
"""
Run all the crawler unit tests

Each test class is loaded and run. No arguments are accepted. For greater
control, run each set of tests by itself.
"""
import os
import pexpect
import pdb
import sys
import testhelp
import time
import toolframe
import util

def main(args):
    # pdb.set_trace()
    start = time.time()
    logfile = 'crawl_test.log'
    tr = te = tf = 0
    for modname in ['crawl',
                    'AlertTest',
                    'Checkable',
                    'CrawlConfigTest',
                    'CrawlDBI',
                    'CrawlPlugin',
                    'Dimension',
                    'testhelp',
                    'UtilTest',
                    ]:
        script = modname + '.py'
        mod = __import__(modname)
        tlist = testhelp.all_tests(modname)
        print script + ":"
        (r, e, f) = testhelp.run_tests([0], '', tlist, 1, logfile, mod)
        tr += r
        te += e
        tf += f
    print("all tests: %0.3fs (run: %d; errors: %d; failures: %d)" %
          (time.time() - start, tr, te, tf))

if __name__ == '__main__':
    toolframe.ez_launch(main=main)
