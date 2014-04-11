"""
This file provides common code that the various interactive tools may use.
"""

import CrawlConfig
import optparse
import pdb
import sys
import time

def simplug(plugin, args):
    """
    Common plugin simulator. May be used by the interactive tools to simulate
    running the associated plugin.
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-i', '--iterations',
                 action='store', default=1, dest='iterations', type='int',
                 help='how many iterations to run')
    (o, a) = p.parse_args(args)

    if o.debug: pdb.set_trace()
    
    cfg = CrawlConfig.get_config()
    CrawlConfig.log("starting simplug, just got config")
    sys.path.append(cfg.get('crawler', 'plugin-dir'))
    P = __import__(cfg.get(plugin, 'module'))
    P.main(cfg)
    if 1 < o.iterations:
        for count in range(o.iterations-1):
            stime = cfg.get_time(plugin, 'frequency')
            time.sleep(stime)
            P.main(cfg)
