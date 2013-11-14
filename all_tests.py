#!/usr/bin/env python
import os
import pexpect
import pdb
import sys
import toolframe
import testhelp

def main(args):
    logfile = 'crawl_test.log'
    for modname in ['crawl',
                    'AlertTest',
                    'Checkable',
                    'CrawlConfig',
                    'CrawlPlugin',
                    'testhelp',
                    'util',
                    ]:
        script = modname + '.py'
        mod = __import__(modname)
        tlist = testhelp.all_tests(modname)
        print script + ":"
        testhelp.run_tests([0], '', tlist, 1, logfile, mod)

if __name__ == '__main__':
    toolframe.ez_launch(main=main)
