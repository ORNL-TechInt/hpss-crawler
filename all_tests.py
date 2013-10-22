#!/usr/bin/env python
import os
import pexpect
import pdb
import sys
import testhelp

logfile = 'crawl_test.log'
for modname in ['crawl',
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
