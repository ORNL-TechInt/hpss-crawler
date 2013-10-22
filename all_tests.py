#!/usr/bin/env python
import pexpect
for script in ['crawl.py',
               'Checkable.py',
               'CrawlConfig.py',
               'CrawlPlugin.py',
               'testhelp.py',
               'util.py',
              ]:
    print("%s:" % script)
    print pexpect.run(script),
