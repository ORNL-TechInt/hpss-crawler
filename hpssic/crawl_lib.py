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
def drop_table(cfg=None, prefix=None, table=None):
    """
    This wraps the table dropping operation.
    """
    if table is None:
        return(MSG.nothing_to_drop)

    if cfg is None:
        cfg = CrawlConfig.get_config()

    if prefix is None:
        prefix = cfg.get('dbi-crawler', 'tbl_prefix')
    else:
        cfg.set('dbi-crawler', 'tbl_prefix', prefix)

    db = CrawlDBI.DBI(dbtype="crawler")
    if not db.table_exists(table=table):
        rval = ("Table '%s_%s' does not exist" % (prefix, table))
    else:
        db.drop(table=table)
        if db.table_exists(table=table):
            rval = ("Attempt to drop table '%s_%s' failed" % (prefix, table))
        else:
            rval = ("Attempt to drop table '%s_%s' was successful" %
                    (prefix, table))

    db.close()
    return rval


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
