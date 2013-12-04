#!/usr/bin/env python
import os
import pexpect
import pdb
import sys
import testhelp
import time
import toolframe

def main(args):
    # pdb.set_trace()
    start = time.time()
    logfile = 'crawl_test.log'
    tr = te = tf = 0
    for modname in ['crawl',
                    'AlertTest',
                    'Checkable',
                    'CrawlConfig',
                    'CrawlDBI',
                    'CrawlPlugin',
                    'Dimension',
                    'testhelp',
                    'util',
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
