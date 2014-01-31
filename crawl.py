#!/usr/bin/env python
"""
Run a bunch of plug-ins which will probe the integrity of HPSS
"""
import CrawlConfig
import CrawlDBI
import CrawlPlugin
import daemon
import glob
import optparse
import os
import pdb
import pexpect
import shutil
import sys
import testhelp
import time
import toolframe
import traceback as tb
import util

exit_file = 'crawler.exit'
# ------------------------------------------------------------------------------
def crl_cfgdump(argv):
    """cfgdump - load a config file and dump its contents

    usage: crawl cfgdump -c <filename> [--to stdout|log] [--logpath <path>]
    """
    p = optparse.OptionParser()
    p.add_option('-c', '--cfg',
                 action='store', default='', dest='config',
                 help='config file name')
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-t', '--to',
                 action='store', default='', dest='target',
                 help='specify where to send the output')
    p.add_option('-l', '--logpath',
                 action='store', default='', dest='logpath',
                 help='specify where to send the output')
    (o, a) = p.parse_args(argv)

    if o.debug: pdb.set_trace()


    if o.config == '':    o.config = 'crawl.cfg'
    if o.target == '':    o.target = 'stdout'

    cfg = CrawlConfig.get_config(o.config)
    dumpstr = cfg.dump()

    if o.target == 'stdout':
        print dumpstr
    elif o.target == 'log':
        log = util.get_logger(o.logpath, cfg)
        for line in dumpstr.split("\n"):
            log.info(line)

# ------------------------------------------------------------------------------
def crl_cleanup(argv):
    """cleanup - remove any test directories left behind

    usage: crawl cleanup

    Looks for and removes /tmp/hpss-crawl.* recursively.
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-n', '--dryrun',
                 action='store_true', default=False, dest='dryrun',
                 help='see what would happen')
    (o, a) = p.parse_args(argv)

    if o.debug: pdb.set_trace()
    
    testdirs = glob.glob("/tmp/hpss-crawl.*")
    for td in testdirs:
        if o.dryrun:
            print("would do 'shutil.rmtree(%s)'" % td)
        else:
            shutil.rmtree(td)

# ------------------------------------------------------------------------------
def crl_cvreport(argv):
    """cvreport - show the database status

    select count(*) from checkables where type = 'f';
    select count(*) from checkables where checksum <> 0;
    select sum(p_count), sum(s_count) from dimension;
    select checksum from cvstats;
    """
    p = optparse.OptionParser()
    p.add_option('-c', '--cfg',
                 action='store', default='', dest='config',
                 help='config file name')
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-p', '--prefix',
                 action='store', default='', dest='prefix',
                 help='table name prefix')
    (o, a) = p.parse_args(argv)

    if o.debug: pdb.set_trace()

    cfg = CrawlConfig.get_config('crawl.cfg')
    if o.prefix != '':
        cfg.set('dbi', 'tbl_prefix', o.prefix)
        
    db = CrawlDBI.DBI(cfg=cfg)
    rows = db.select(table="checkables",
                     fields=["count(*)"],
                     where="type = 'f'")
    (files_in_pop) = rows[0][0]

    rows = db.select(table="checkables",
                     fields=["count(*)"],
                     where="checksum <> 0")
    (files_in_sample) = rows[0][0]

    rows = db.select(table="dimension",
                     fields=["sum(p_count)", "sum(s_count)"])
    (dim_p_count, dim_s_count) = rows[0]

    rows = db.select(table="cvstats",
                     fields=[])
    (total_checksums) = rows[0][1]

    pflag = sflag = cflag = ""
    if files_in_pop != dim_p_count:
        pflag = "!"
    if files_in_sample != dim_s_count:
        sflag = "!"
    if total_checksums != files_in_sample:
        cflag = "!"
        
    print("%15s %10s  %10s" % (" ", "Population", "Sample"))
    print("%15s %10d  %10d" % ("checkables", files_in_pop, files_in_sample))
    print("%15s %10d%1s %10d%1s" % ("dimension", dim_p_count, pflag, dim_s_count, sflag))
    print("%15s %10s  %10d%1s" % ("cvstats", " ", total_checksums, cflag))

    print("-----")

    rows = db.select(table="dimension",
                     fields=[])
    print("%8s %8s %15s %15s" % ("Name",
                                 "Category",
                                 "==Population===",
                                 "====Sample====="))
    psum = ssum = 0
    for r in rows:
        print("%8s %8s %7d %7.2f %7d %7.2f" % r[1:])
        psum += r[3]
        ssum += r[5]
    print("%8s %8s %7d %7s %7d" % (" ", "Total", psum, " ", ssum))
          
    db.close()
    
# ------------------------------------------------------------------------------
def crl_dbdrop(argv):
    """dbdrop - drop a database table

    usage: crawl dbdrop [-f] <table-name>

    Drop database table <table-name>
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-f', '--force',
                 action='store_true', default=False, dest='force',
                 help='proceed without confirmation')
    (o, a) = p.parse_args(argv)

    if o.debug: pdb.set_trace()

    cfg = CrawlConfig.get_config()
    tbpfx = cfg.get('dbi', 'tbl_prefix')
    tname = a[0]
    answer = raw_input("About to drop db table %s_%s. Are you sure? > " %
                       (tbpfx, tname))
    if answer[0].lower() != "y":
        sys.exit()
        
    db = CrawlDBI.DBI()
    db.drop(table=tname)
    if db.table_exists(table=tname):
        print("Attempt to drop table '%s_%s' failed" % (tbpfx, tname))
    else:
        print("Attempt to drop table '%s_%s' was successful" % (tbpfx, tname))
    db.close()
    
# ------------------------------------------------------------------------------
def crl_fire(argv):
    """fire - run a plugin

    usage: crawl fire --cfg cfgname --logpath logfname --plugin plugname
    """
    p = optparse.OptionParser()
    p.add_option('-c', '--cfg',
                 action='store', default='', dest='config',
                 help='config file name')
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-l', '--logpath',
                 action='store', default='', dest='logpath',
                 help='specify where to send the output')
    p.add_option('-p', '--plugin',
                 action='store', default='', dest='plugname',
                 help='which plugin to fire')
    (o, a) = p.parse_args(argv)
    
    if o.debug: pdb.set_trace()

    cfg = CrawlConfig.get_config(o.config)
    log = util.get_logger(o.logpath, cfg)

    if not cfg.has_section(o.plugname):
        print("No plugin named '%s' is defined")
    else:
        plugdir = cfg.get('crawler', 'plugin-dir')
        sys.path.append(plugdir)
        __import__(o.plugname)
        log.info('firing %s' % o.plugname)
        sys.modules[o.plugname].main(cfg)
    
# ------------------------------------------------------------------------------
def crl_log(argv):
    """log - write a message to the indicated log file

    usage: crawl log --log <filename> <message>
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-l', '--log',
                 action='store', default=None, dest='logfile',
                 help='specify the log file')
    (o, a) = p.parse_args(argv)
    
    if o.debug: pdb.set_trace()
    
    log = util.get_logger(o.logfile)
    log.info(" ".join(a))

# ------------------------------------------------------------------------------
def crl_start(argv):
    """start - if the crawler is not already running as a daemon, start it

    usage: crawl start

    default config file: crawl.cfg, or
                         $CRAWL_CONF, or
                         -c <filename> on command line
    default log file:    /var/log/crawl.log, or
                         $CRAWL_LOG, or
                         -l <filename> on command line
    """
    p = optparse.OptionParser()
    p.add_option('-c', '--cfg',
                 action='store', default='', dest='config',
                 help='config file name')
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-l', '--log',
                 action='store', default='', dest='logfile',
                 help='specify the log file')
    p.add_option('-C', '--context',
                 action='store', default='', dest='context',
                 help="context of crawler ('TEST' or 'PROD')")
    (o, a) = p.parse_args(argv)
    
    if o.debug: pdb.set_trace()

    if os.path.exists('crawler_pid'):
        print('crawler_pid exists. If you are sure it is not running,\n' +
              'please remove crawler_pid and try again.')
    else:
        #
        # Initialize the configuration
        #
        cfg = CrawlConfig.get_config(o.config)
        log = util.get_logger(o.logfile, cfg)
        crawler = CrawlDaemon('crawler_pid',
                              stdout="crawler.stdout",
                              stderr="crawler.stderr",
                              logger=log,
                              workdir='.')
        log.info('crl_start: calling crawler.start()')
        crawler.start()
    pass

# ------------------------------------------------------------------------------
def crl_status(argv):
    """status - report whether the crawler is running or not

    usage: crawl status
    """
    if is_running():
        cpid = util.contents('crawler_pid').strip()
        print("The crawler is running as process %s." % cpid)
    else:
        print("The crawler is not running.")
    if os.path.exists(exit_file):
        print("Termination has been requested (%s exists)" % exit_file) 

# ------------------------------------------------------------------------------
def crl_stop(argv):
    """stop - shut down the crawler daemon if it is running

    usage: crawl stop
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-l', '--log',
                 action='store', default='crawler.log', dest='logfile',
                 help='specify the log file')
    p.add_option('-C', '--context',
                 action='store', default='', dest='context',
                 help="context of crawler ('TEST' or 'PROD')")
    (o, a) = p.parse_args(argv)
    
    if o.debug: pdb.set_trace()

    testhelp.touch(exit_file)
    
# ---------------------------------------------------------------------------
def cfg_changed(cfg):
    """
    Return True if the mtime on the configuration file is more recent than the
    'loadtime' option stored in the configuration itself.
    """
    return cfg.changed()

# ------------------------------------------------------------------------------
def get_timeval(cfg, section, option, default):
    """
    Return the number of seconds indicated by the time spec, using default if
    any errors or failures occur
    """
    log = util.get_logger()
    return cfg.gettime(section, option, default, log)

# ------------------------------------------------------------------------------
def is_running():
    """
    Return True if the crawler is running (per ps(1)) or False otherwise.
    """
    result = pexpect.run("ps -ef")
    running = False
    for line in result.split("\n"):
        if 'crawl start' in line:
            running = True
    return running
    
# ------------------------------------------------------------------------------
class CrawlDaemon(daemon.Daemon):
    """
    This class extends this daemon.Daemon to serve this application. Method
    run() gets run in the background and then calls fire() when appropriate to
    invoke a plugin.
    """
    # --------------------------------------------------------------------------
    def fire(self, plugin, cfg):
        """
        Load the plugin if necessary, then run it
        """
        try:
            plugdir = cfg.get('crawler', 'plugin-dir')
            if plugdir not in sys.path:
                sys.path.append(plugdir)
            if plugin not in sys.modules.keys():
                __import__(plugin)
            sys.modules[plugin].main(cfg)
        except:
            tbstr = tb.format_exc()
            for line in tbstr.split('\n'):
                self.dlog("crawl: '%s'" % line)

    # --------------------------------------------------------------------------
    def run(self):
        """
        This routine runs in the background as a daemon. Here's where
        we fire off plug-ins as appropriate.
        """
        keep_going = True
        cfgname = ''
        plugin_d = {}
        while keep_going:
            try:
                cfg = CrawlConfig.get_config(cfgname)
                pluglstr = cfg.get('crawler', 'plugins')
                pluglist = [x.strip() for x in pluglstr.split(',')]
                for s in pluglist:
                    self.dlog('crawl: CONFIG: [%s]' % s)
                    for o in cfg.options(s):
                        self.dlog('crawl: CONFIG: %s: %s' % (o, cfg.get(s, o)))
                    if s == 'crawler':
                        continue
                    elif s in plugin_d.keys():
                        plugin_d[s].reload(cfg)
                    else:
                        plugin_d[s] = CrawlPlugin.CrawlPlugin(name=s,
                                                              cfg=cfg)

                # remove any plugins that are not in the new configuration
                for p in plugin_d.keys():
                    if p not in cfg.sections():
                        del plugin_d[p]
                
                heartbeat = cfg.get_time('crawler', 'heartbeat', 10)
                ecount = ewhen = 0
                tlimit = 7.0
                while keep_going:
                    #
                    # Fire any plugins that are due
                    #
                    try:
                        for p in plugin_d.keys():
                            if plugin_d[p].time_to_fire():
                                plugin_d[p].fire()
                    except:
                        tbstr = tb.format_exc()
                        for line in tbstr.split('\n'):
                            self.dlog("crawl: '%s'" % line)
                        ecount += 1
                        dt = time.time() - ewhen
                        if 3 < ecount and dt < tlimit:
                            self.dlog("crawl: %d exceptions in %f " %
                                      (ecount, dt) +
                                      "seconds -- shutting down")
                            keep_going = False
                        elif tlimit <= dt:
                            ewhen = time.time()
                            ecount = 1

                    #
                    # Issue the heartbeat if it's time
                    #
                    if 0 == (int(time.time()) % heartbeat):
                        self.dlog('crawl: heartbeat...')
                            
                    #
                    # If config file has changed, reload it by reseting the
                    # cached config object and breaking out of the inner loop.
                    # The first thing the outer loop does is to load the
                    # configuration
                    #
                    if cfg.changed():
                        cfgname = cfg.get('crawler', 'filename')
                        CrawlConfig.get_config(reset=True)
                        break

                    #
                    # Check for the exit signal
                    #
                    if util.conditional_rm(exit_file):
                        self.dlog('crawl: shutting down')
                        keep_going = False

                    #
                    # We cycle once per second so we can detect if the user
                    # asks us to stop or if the config file changes and needs
                    # to be reloaded
                    #
                    time.sleep(1.0)

            except:
                # if we get an exception, write the traceback to the log file
                tbstr = tb.format_exc()
                for line in tbstr.split('\n'):
                    self.dlog("crawl: '%s'" % line)
                keep_going = False

# ------------------------------------------------------------------------------
toolframe.tf_launch("crl", __name__)
