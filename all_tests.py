#!/usr/bin/env python
import os
import pexpect
import pdb
import sys
import testhelp

# pdb.set_trace()
for script in ['crawl.py',
               'Checkable.py',
               'CrawlConfig.py',
               'CrawlPlugin.py',
               'testhelp.py',
               'util.py',
              ]:
    modname = script.replace(".py", "")
    mod = __import__(modname)
    tlist = testhelp.all_tests(modname)
    print script + ":"
    testhelp.run_tests([0], '', tlist, 1, "crawl_test.log", mod)
    # os.system(script)
