"""
This file provides common code that the various interactive tools may use.
"""

import CrawlConfig
import CrawlDBI
import messages as MSG
import optparse
import pdb
import sys
import time


# -----------------------------------------------------------------------------
def retrieve_history(**kw):
    """
    Retrieve and return the contents of table 'history'. At some point, we may
    need to turn this into a generator so we don't try to load the whole table
    into memory at once, but for now YAGNI.
    """
    db = CrawlDBI.DBI(dbtype='crawler')
    kw['table'] = 'history'
    if 'fields' not in kw:
        kw['fields'] = ['plugin', 'runtime', 'errors']
    rows = db.select(**kw)
    db.close()
    return rows


# -----------------------------------------------------------------------------
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

    if o.debug:
        pdb.set_trace()

    cfg = CrawlConfig.get_config()
    CrawlConfig.log("starting %s simplug, just got config" % plugin)
    sys.path.append(cfg.get('crawler', 'plugin-dir'))
    modname = cfg.get(plugin, 'module')
    try:
        P = __import__(modname)
    except ImportError:
        H = __import__('hpssic.plugins.' + modname)
        P = getattr(H.plugins, modname)
    P.main(cfg)
    if 1 < o.iterations:
        for count in range(o.iterations-1):
            stime = cfg.get_time(plugin, 'frequency')
            time.sleep(stime)
            P.main(cfg)
