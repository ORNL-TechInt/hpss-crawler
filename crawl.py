#!/usr/bin/env python
"""
crawl - Run a bunch of plug-ins which will probe the integrity of HPSS
"""
import CrawlConfig
import CrawlPlugin
import copy
import daemon
import glob
import logging
import optparse
import os
import pdb
import pexpect
import re
import shutil
import socket
import stat
import sys
import testhelp
import time
import toolframe
import traceback as tb
import unittest

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

    cfg = get_config(o.config)
    dumpstr = cfg.dump()
    
    if o.target == 'stdout':
        print dumpstr
    elif o.target == 'log':
        log = get_logger(o.logpath, cfg)
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

    cfg = get_config(o.config)
    log = get_logger(o.logpath, cfg)

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
    
    log = get_logger(o.logfile)
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
        cfg = get_config(o.config)
        log = get_logger(o.logfile, cfg)
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
        cpid = contents('crawler_pid').strip()
        print("The crawler is running as process %s." % cpid)
    else:
        print("The crawler is not running.")
    
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

    testhelp.touch('crawler.exit')
    
# ---------------------------------------------------------------------------
def cfg_changed(cfg):
    """
    Return True if the mtime on the configuration file is more recent than the
    'loadtime' option stored in the configuration itself.
    """
    return cfg.changed()

# ---------------------------------------------------------------------------
def contents(filepath, string=True):
    """
    Return the contents of a file as a string.
    """
    f = open(filepath, 'r')
    if string:
        rval = "".join(f.readlines())
    else:
        rval = f.readlines()
    f.close()
    return rval

# ------------------------------------------------------------------------------
def get_config(cfname='', reset=False):
    """
    Open the config file based on cfname, $CRAWL_CONF, or the default, in that
    order. Construct a CrawlConfig object, cache it, and return it. Subsequent
    calls will retrieve the cached object unless reset=True, in which case the
    old object is destroyed and a new one is constructed.

    Note that values in the default dict passed to CrawlConfig.CrawlConfig
    must be strings.
    """
    if reset:
        try:
            del get_config._config
        except AttributeError:
            pass
        return
    
    try:
        rval = get_config._config
    except AttributeError:
        if cfname == '':
            envval = os.getenv('CRAWL_CONF')
            if None != envval:
                cfname = envval
    
        if cfname == '':
            cfname = 'crawl.cfg'

        if not os.path.exists(cfname):
            raise StandardError("%s does not exist" % cfname)
        elif not os.access(cfname, os.R_OK):
            raise StandardError("%s is not readable" % cfname)
        rval = CrawlConfig.CrawlConfig({'fire': 'no',
                                        'frequency': '3600',
                                        'heartbeat': '10'})
        rval.read(cfname)
        rval.set('crawler', 'filename', cfname)
        rval.set('crawler', 'loadtime', str(time.time()))
        get_config._config = rval
        
    return rval

# ------------------------------------------------------------------------------
def get_timeval(cfg, section, option, default):
    """
    Return the number of seconds indicated by the time spec, using default if
    any errors or failures occur
    """
    log = get_logger()
    return cfg.gettime(section, option, default, log)

# ------------------------------------------------------------------------------
def get_logger(cmdline='', cfg=None, reset=False):
    """
    Return the logging object for this process. Instantiate it if it
    does not exist already.
    """
    if reset:
        try:
            del get_logger._logger
        except AttributeError:
            pass
        return

    envval = os.getenv('CRAWL_LOG')

    if cmdline != '':
        filename = cmdline
    elif cfg != None:
        try:
            filename = cfg.get('crawler', 'logpath')
        except:
            pass
    elif envval != None:
        filename = envval
    else:
        filename = '/var/log/crawl.log'
        
    try:
        rval = get_logger._logger
    except AttributeError:
        get_logger._logger = logging.getLogger('Integrity')
        get_logger._logger.setLevel(logging.INFO)
        host = socket.gethostname().split('.')[0]
        fh = logging.handlers.RotatingFileHandler(filename,
                                                  maxBytes=10*1024*1024,
                                                  backupCount=5)
        strfmt = "%" + "(asctime)s [%s] " % host + "%" + "(message)s"
        fmt = logging.Formatter(strfmt, datefmt="%Y.%m%d %H:%M:%S")
        fh.setFormatter(fmt)

        get_logger._logger.addHandler(fh)
        get_logger._logger.info('-' * (55 - len(host)))
        rval = get_logger._logger

    return rval

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
def persistent_rm(path):
    """
    Try up to 10 times to remove a directory tree, sleeping after each attempt
    to give the file system time to respond.
    """
    done = False
    retries = 0
    while not done and retries < 10:
        try:
            shutil.rmtree(path)
            done = True
        except OSError as e:
            time.sleep(1.0)
            print("Retry %d of 10: shutil.rmtree(%s)" % (retries, path))
            retries += 1

# ------------------------------------------------------------------------------
def Crawl_setup():
    """
    Setup needed before running the tests.
    """
    if os.path.isdir(CrawlTest.testdir):
        shutil.rmtree(os.path.dirname(CrawlTest.testdir))
    os.makedirs(CrawlTest.testdir)
    # unreadable = '%s/unreadable.cfg' % (CrawlTest.testdir)
    # if os.path.exists(unreadable):
    #     os.unlink(unreadable)
        
# ------------------------------------------------------------------------------
def Crawl_cleanup():
    """
    Clean up after a sequence of tests.
    """
    if os.getcwd().endswith('/test.d'):
        os.chdir(launch_dir)
        
    if not testhelp.keepfiles():
        flist = [os.path.dirname(CrawlTest.testdir)]
        for fspec in flist:
            for fname in glob.glob(fspec):
                if os.path.isdir(fname):
                    persistent_rm(fname)
                elif os.path.exists(fname):
                    os.unlink(fname)

    if is_running():
        testhelp.touch('crawler.exit')

# ------------------------------------------------------------------------------
class CrawlDaemon(daemon.Daemon):
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
                exitfile = 'crawler.exit'
                cfg = get_config(cfgname)
                for s in cfg.sections():
                    self.dlog('crawl: CONFIG: [%s]' % s)
                    for o in cfg.options(s):
                        self.dlog('crawl: CONFIG: %s: %s' % (o, cfg.get(s, o)))
                    if s == 'crawler':
                        continue
                    elif s in plugin_d.keys():
                        plugin_d[s].reload(cfg)
                    else:
                        plugin_d[s] = CrawlPlugin.CrawlPlugin(name=s,
                                                              cfg=cfg,
                                                              logger=get_logger())

                # remove any plugins that are not in the new configuration
                for p in plugin_d.keys():
                    if p not in cfg.sections():
                        del plugin_d[p]
                
                heartbeat = cfg.get_time('crawler', 'heartbeat', 10)
                while True:
                    #
                    # Issue the heartbeat if it's time
                    #
                    if 0 == (int(time.time()) % heartbeat):
                        self.dlog('crawl: heartbeat...')
                        
                    #
                    # Check for the exit signal
                    #
                    if os.path.exists(exitfile):
                        os.unlink(exitfile)
                        self.dlog('crawl: shutting down')
                        keep_going = False
                        break

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
                            
                    #
                    # If config file has changed, reload it by reseting the
                    # cached config object and breaking out of the inner loop.
                    # The first thing the outer loop does is to load the
                    # configuration
                    #
                    if cfg.changed():
                        cfgname = cfg.get('crawler', 'filename')
                        get_config(reset=True)
                        break

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

# -----------------------------------------------------------------------------
class CrawlTest(unittest.TestCase):

    testdir = '/tmp/hpss-crawl.%d/test.d' % os.getpid()
    plugdir = '%s/plugins' % testdir
    default_cfname = 'crawl.cfg'
    env_cfname = 'envcrawl.cfg'
    exp_cfname = 'explicit.cfg'
    default_logpath = '%s/test_default_hpss_crawl.log' % testdir
    cdict = {'crawler': {'plugin-dir': '%s/plugins' % testdir,
                         'logpath': default_logpath,
                         'logsize': '5mb',
                         'logmax': '5',
                         'e-mail-recipients':
                         'tbarron@ornl.gov, tusculum@gmail.com',
                         'trigger': '<command-line>'
                         },
             'plugin_A': {'frequency': '1h',
                          'operations': '15'
                          }
             }

    # --------------------------------------------------------------------------
    def test_crawl_cfgdump_log_nopath(self):
        """
        TEST: "crawl cfgdump -c <cfgpath> --to log"
        EXP: what is written to log matches what was written to
        cfgpath. output should go to log path named in cfg.
        """
        cfname = "%s/test_crawl_cfgdump_log_n.cfg" % self.testdir
        self.write_cfg_file(cfname, self.cdict)
        cmd = 'crawl cfgdump -c %s --to log' % cfname
        result = pexpect.run(cmd)
        # print(">>>\n%s\n<<<" % result)
        self.vassert_nin("Traceback", result)
        self.assertEqual(os.path.exists(self.default_logpath), True)
        lcontent = contents(self.default_logpath)
        for section in self.cdict.keys():
            self.vassert_in('[%s]' % section, lcontent)

            for item in self.cdict[section].keys():
                self.vassert_in('%s = %s' %
                                (item, self.cdict[section][item]), lcontent)

        self.vassert_nin('heartbeat', lcontent)
        self.vassert_nin('fire', lcontent)

    # --------------------------------------------------------------------------
    def test_crawl_cfgdump_log_path(self):
        """
        TEST: "crawl cfgdump -c <cfgpath> --to log --log <logpath>"
        EXP: what is written to log matches what was written to
        cfgpath. output should go to logpath specified on command
        line.
        """
        cfname = "%s/test_crawl_cfgdump_log_p.cfg" % self.testdir
        logpath = "%s/test_local.log" % self.testdir
        self.write_cfg_file(cfname, self.cdict)
        cmd = ('crawl cfgdump -c %s --to log --logpath %s'
               % (cfname, logpath))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.assertEqual(os.path.exists(logpath), True)
        lcontent = contents(logpath)
        # print(">>>\n%s\n<<<" % result)
        for section in self.cdict.keys():
            self.vassert_in('[%s]' % section, lcontent)

            for item in self.cdict[section].keys():
                self.vassert_in('%s = %s' %
                                (item, self.cdict[section][item]), lcontent)
        
        self.vassert_nin('heartbeat', lcontent)
        self.vassert_nin('fire', lcontent)

    # --------------------------------------------------------------------------
    def test_crawl_cfgdump_nosuch(self):
        """
        TEST: "crawl cfgdump -c test.d/nosuch.cfg"
        EXP: attempting to open a nonexistent config file throws an error
        """
        cfname = '%s/nosuch.cfg' % self.testdir
        if os.path.exists(cfname):
            os.unlink(cfname)
        cmd = 'crawl cfgdump -c %s' % cfname
        result = pexpect.run(cmd)
        self.vassert_in("Traceback", result)
        self.vassert_in("%s does not exist" % cfname,
                        result)

    # --------------------------------------------------------------------------
    def test_crawl_cfgdump_read(self):
        """
        TEST: "crawl cfgdump -c test.d/unreadable.cfg"
        EXP: attempting to open an unreadable config file throws an error
        """
        cfname = '%s/unreadable.cfg' % self.testdir
        open(cfname, 'w').close()
        os.chmod(cfname, 0000)

        cmd = 'crawl cfgdump -c %s' % cfname
        result = pexpect.run(cmd)
        self.vassert_in("Traceback", result)
        self.vassert_in("%s is not readable" % cfname,
                        result)

    # --------------------------------------------------------------------------
    def test_crawl_cfgdump_stdout(self):
        """
        TEST: "crawl cfgdump -c <cfgpath> --to stdout"
        EXP: what is written to stdout matches what was written to cfgpath
        """
        cfname = "%s/test_crawl_cfgdump_stdout.cfg" % self.testdir
        self.write_cfg_file(cfname, self.cdict)
        cmd = 'crawl cfgdump -c %s --to stdout' % cfname
        result = pexpect.run(cmd)
        # print(">>>\n%s\n<<<" % result)
        for section in self.cdict.keys():
            self.vassert_in('[%s]' % section, result)

            for item in self.cdict[section].keys():
                self.vassert_in('%s = %s' %
                                (item, self.cdict[section][item]), result)
        
    # --------------------------------------------------------------------------
    def test_crawl_fire_log_path(self):
        """
        TEST: crawl fire --plugin <plugmod>
        EXP: plugin fired and output went to specified log path
        """
        cfname = "%s/test_crawl_fire_log.cfg" % self.testdir
        lfname = "%s/test_crawl_fire.log" % self.testdir
        # plugdir = '%s/plugins' % self.testdir
        plugname = 'plugin_1'
        
        # create a plug module
        self.write_plugmod(self.plugdir, plugname)
        
        # add the plug module to the config
        t = CrawlConfig.CrawlConfig()
        t.load_dict(self.cdict)
        t.add_section(plugname)
        t.set(plugname, 'frequency', '1m')
        f = open(cfname, 'w')
        t.crawl_write(f)
        f.close()
        
        # carry out the test
        cmd = ('crawl fire -c %s --plugin %s --logpath %s' %
               (cfname, plugname, lfname))
        result = pexpect.run(cmd)

        # verify that command ran successfully
        self.vassert_nin("Traceback", result)
        
        # test.d/plugins/plugin_1.py should exist
        if not plugname.endswith('.py'):
            plugname += '.py'
        self.assertEqual(os.path.exists('%s/%s' % (self.plugdir, plugname)), True)
        
        # test.d/fired should exist and contain 'plugin plugin_1 fired'
        filename = '%s/fired' % self.testdir
        self.assertEqual(os.path.exists(filename), True)
        self.vassert_in('plugin plugin_1 fired', contents(filename))
        
        # lfname should exist and contain specific strings
        self.assertEqual(os.path.exists(lfname), True)
        self.vassert_in('firing plugin_1', contents(lfname))
    
    # --------------------------------------------------------------------------
    def test_crawl_log(self):
        """
        TEST: "crawl log --log filename message" will write message to filename
        """
        lfname = '%s/test_crawl.log' % self.testdir
        msg = "this is a test log message"
        cmd = "crawl log --log %s %s" % (lfname, msg)
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_in(msg, contents(lfname))
        
    # --------------------------------------------------------------------------
    def test_crawl_start(self):
        """
        TEST: 'crawl start' should fire up a daemon crawler which will exit
        when file 'crawler.exit' is touched. Verify that crawler_pid exists
        while crawler is running and that it is removed when it stops.
        """
        cfgpath = '%s/test_start.cfg' % self.testdir
        logpath = '%s/test_start.log' % self.testdir
        self.write_cfg_file(cfgpath, self.cdict)
        self.write_plugmod(self.plugdir, 'plugin_A')
        cmd = ('crawl start --log %s --cfg %s --context TEST'
               % (logpath, cfgpath))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin("crawler_pid", result)

        self.assertEqual(is_running(), True)
        self.assertEqual(os.path.exists('crawler_pid'), True)
        self.assertEqual(os.path.exists(logpath), True)
        self.assertEqual('leaving daemonize' in contents(logpath), True)
        
        testhelp.touch('crawler.exit')

        time.sleep(1)
        self.assertEqual(is_running(), False)
        self.assertEqual(os.path.exists('crawler_pid'), False)
                
    # --------------------------------------------------------------------------
    def test_crawl_start_already(self):
        """
        TEST: If the crawler is already running, decline to run a second copy.
        """
        cfgpath = '%s/test_start.cfg' % self.testdir
        logpath = '%s/test_start.log' % self.testdir
        self.write_cfg_file(cfgpath, self.cdict)
        cmd = ('crawl start --log %s --cfg %s --context TEST'
               % (logpath, cfgpath))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin("crawler_pid", result)

        self.assertEqual(is_running(), True)

        result = pexpect.run(cmd)
        self.assertEqual('crawler_pid exists' in result, True)
        
        testhelp.touch('crawler.exit')

        time.sleep(1)
        self.assertEqual(is_running(), False)
        
    # --------------------------------------------------------------------------
    def test_crawl_start_cfg(self):
        """
        TEST: 'crawl start' should fire up a daemon crawler which will exit
        when file 'crawler.exit' is touched. Verify that the correct config
        file is loaded.
        """
        cfgpath = '%s/test_stcfg.cfg' % self.testdir
        logpath = '%s/test_stcfg.log' % self.testdir
        xdict = self.cdict
        xdict['other_plugin'] = {'unplanned': 'silver',
                                 'simple': 'check for this'}
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(self.plugdir, 'plugin_A')
        self.write_plugmod(self.plugdir, 'other_plugin')
        cmd = ('crawl start --log %s --cfg %s --context TEST'
               % (logpath, cfgpath))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin("crawler_pid", result)

        self.assertEqual(is_running(), True)
        self.assertEqual(os.path.exists('crawler_pid'), True)
        self.assertEqual(os.path.exists(logpath), True)
        self.assertEqual('crawl: CONFIG: [other_plugin]' in
                         contents(logpath),
                         True)
        self.assertEqual('crawl: CONFIG: unplanned: silver' in
                         contents(logpath),
                         True)
        self.assertEqual('crawl: CONFIG: simple: check for this' in
                         contents(logpath),
                         True)
        
        testhelp.touch('crawler.exit')

        time.sleep(1)
        self.assertEqual(is_running(), False)
        self.assertEqual(os.path.exists('crawler_pid'), False)
                
    # --------------------------------------------------------------------------
    def test_crawl_start_fire(self):
        """
        TEST: 'crawl start' should fire up a daemon crawler which will exit
        when file 'crawler.exit' is touched. Verify that at least one plugin
        fires and produces some output.
        """
        cfgpath = '%s/test_fire.cfg' % self.testdir
        logpath = '%s/test_fire.log' % self.testdir
        xdict = copy.deepcopy(self.cdict)
        xdict['other'] = {'frequency': '1s', 'fire': 'true'}
        xdict['crawler']['verbose'] = 'true'
        del xdict['plugin_A']
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(self.plugdir, 'other')
        cmd = ('crawl start --log %s --cfg %s --context TEST'
               % (logpath, cfgpath))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin("crawler_pid", result)

        self.assertEqual(is_running(), True)
        self.assertEqual(os.path.exists('crawler_pid'), True)
        self.assertEqual(os.path.exists(logpath), True)
        self.assertEqual('leaving daemonize' in contents(logpath), True)
        time.sleep(2)
        self.assertEqual('other: firing' in contents(logpath), True,
                         "Log file does not indicate plugin was fired")
        self.assertEqual(os.path.exists('%s/fired' % self.testdir), True,
                         "File %s/fired does not exist" % self.testdir)
        self.assertEqual('plugin other fired\n',
                         contents('%s/fired' % self.testdir),
                         "Contents of %s/fired is not right" % self.testdir)
        
        testhelp.touch('crawler.exit')

        time.sleep(1)
        self.assertEqual(is_running(), False)
        self.assertEqual(os.path.exists('crawler_pid'), False)
                
    # --------------------------------------------------------------------------
    def test_crawl_status(self):
        """
        TEST: 'crawl status' should report the crawler status correctly.
        """
        logpath = '%s/test_start.log' % self.testdir
        cfgpath = '%s/test_status.cfg' % self.testdir
        self.write_cfg_file(cfgpath, self.cdict)
        cmd = 'crawl status'
        result = pexpect.run(cmd)
        self.assertEqual(result.strip(), "The crawler is not running.")
        
        cmd = ('crawl start --log %s --cfg %s --context TEST'
               % (logpath, cfgpath))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin("crawler_pid", result)

        self.assertEqual(is_running(), True)
        self.assertEqual(os.path.exists('crawler_pid'), True)

        cmd = 'crawl status'
        result = pexpect.run(cmd)
        self.assertEqual('The crawler is running as process' in result,
                         True)

        cmd = 'crawl stop --log %s --context TEST' % (logpath)
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        time.sleep(1)
        
        self.assertEqual(is_running(), False)
        self.assertEqual(os.path.exists('crawler_pid'), False)

        cmd = 'crawl status'
        result = pexpect.run(cmd)
        self.assertEqual(result.strip(), "The crawler is not running.")

    # --------------------------------------------------------------------------
    def test_crawl_stop(self):
        """
        TEST: 'crawl stop' should cause a running daemon to shut down.
        """
        logpath = '%s/test_start.log' % self.testdir
        cfgpath = '%s/test_start.cfg' % self.testdir
        self.write_cfg_file(cfgpath, self.cdict)
        self.write_plugmod(self.plugdir, 'plugin_A')
        cmd = ('crawl start --log %s --cfg %s --context TEST' %
               (logpath, cfgpath))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin("crawler_pid", result)

        self.assertEqual(is_running(), True,
                         "Expected the crawler to be running but it is not")
        self.assertEqual(os.path.exists('crawler_pid'), True,
                         "File 'crawler_pid' should exist but does not")

        cmd = 'crawl stop --log %s' % (logpath)
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        time.sleep(1)
        
        self.assertEqual(is_running(), False)
        self.assertEqual(os.path.exists('crawler_pid'), False)
        
    # --------------------------------------------------------------------------
    def test_get_config_def_noread(self):
        """
        TEST: env not set, 'crawl.cfg' does exist but not readable

        EXP: get_config() or get_config('') should throw a
        StandardError about the file not existing or not being
        readable
        """
        get_config(reset=True)
        self.cd(self.testdir)
        self.clear_env()
        d = copy.deepcopy(self.cdict)
        d['crawler']['filename'] = self.default_cfname
        self.write_cfg_file(self.default_cfname, self.cdict)
        os.chmod(self.default_cfname, 0000)

        # test get_config with no argument
        got_exception = False
        try:
            cfg = get_config()
        except StandardError as e:
            got_exception = True
            self.assertEqual(str(e),
                             '%s is not readable' % self.default_cfname)
        self.assertEqual(got_exception, True)
        
        # test get_config with empty string argument
        got_exception = False
        try:
            cfg = get_config('')
        except StandardError as e:
            got_exception = True
            self.assertEqual(str(e),
                             '%s is not readable' %
                             self.default_cfname)
        self.assertEqual(got_exception, True)
        
    # --------------------------------------------------------------------------
    def test_get_config_def_nosuch(self):
        """
        TEST: env not set, 'crawl.cfg' does not exist

        EXP: get_config() or get_config('') should throw a
        StandardError about the file not existing or not being
        readable
        """
        get_config(reset=True)
        self.cd(self.testdir)
        self.clear_env()
        if os.path.exists(self.default_cfname):
            os.unlink(self.default_cfname)

        # test with no argument
        got_exception = False
        try:
            cfg = get_config()
        except StandardError as e:
            got_exception = True
            self.assertEqual(str(e),
                             '%s does not exist' %
                             self.default_cfname)
        self.assertEqual(got_exception, True)
        
        # test with empty string argument
        got_exception = False
        try:
            cfg = get_config('')
        except StandardError as e:
            got_exception = True
            self.assertEqual(str(e),
                             '%s does not exist' %
                             self.default_cfname)
        self.assertEqual(got_exception, True)
        # tearDown will 'cd ..'
        
    # --------------------------------------------------------------------------
    def test_get_config_def_ok(self):
        """
        TEST: env not set, 'crawl.cfg' does exist =>

        EXP: get_config() or get_config('') should load the config
        """
        get_config(reset=True)
        self.cd(self.testdir)
        self.clear_env()
        d = copy.deepcopy(self.cdict)
        d['crawler']['filename'] = self.default_cfname
        self.write_cfg_file(self.default_cfname, d)
        os.chmod(self.default_cfname, 0644)

        got_exception = False
        try:
            cfg = get_config()
        except:
            got_exception = True
        self.assertEqual(got_exception, False)
        self.assertEqual(cfg.get('crawler', 'filename'), self.default_cfname)
        self.assertEqual(cfg.filename, self.default_cfname)
        
        got_exception = False
        try:
            cfg = get_config('')
        except:
            got_exception = True
        self.assertEqual(got_exception, False)
        self.assertEqual(cfg.get('crawler', 'filename'), self.default_cfname)
        self.assertEqual(cfg.filename, self.default_cfname)
        
    # --------------------------------------------------------------------------
    def test_get_config_env_noread(self):
        """
        TEST: env CRAWL_CONF='envcrawl.cfg', envcrawl.cfg exists but
        is not readable

        EXP: get_config(), get_config('') should throw a StandardError
        about the file not existing or not being readable
        """
        get_config(reset=True)
        self.cd(self.testdir)
        os.environ['CRAWL_CONF'] = self.env_cfname
        d = copy.deepcopy(self.cdict)
        d['crawler']['filename'] = self.env_cfname
        self.write_cfg_file(self.env_cfname, d)
        os.chmod(self.env_cfname, 0000)

        got_exception = False
        try:
            cfg = get_config()
        except StandardError as e:
            got_exception = True
            self.assertEqual(str(e),
                             '%s is not readable' %
                             self.env_cfname)
        self.assertEqual(got_exception, True)
        
        got_exception = False
        try:
            cfg = get_config('')
        except StandardError as e:
            got_exception = True
            self.assertEqual(str(e),
                             '%s is not readable' %
                             self.env_cfname)
        self.assertEqual(got_exception, True)

        
    # --------------------------------------------------------------------------
    def test_get_config_env_nosuch(self):
        """
        TEST: env CRAWL_CONF='envcrawl.cfg', envcrawl.cfg does not exist
        
        EXP: get_config(), get_config('') should throw a StandardError
        about the file not existing or not being readable
        """
        get_config(reset=True)
        self.cd(self.testdir)
        os.environ['CRAWL_CONF'] = self.env_cfname
        if os.path.exists(self.env_cfname):
            os.unlink(self.env_cfname)

        got_exception = False
        try:
            cfg = get_config()
        except StandardError as e:
            got_exception = True
            self.assertEqual(str(e),
                             '%s does not exist' %
                             self.env_cfname)
        self.assertEqual(got_exception, True)
        
        got_exception = False
        try:
            cfg = get_config('')
        except StandardError as e:
            got_exception = True
            self.assertEqual(str(e),
                             '%s does not exist' %
                             self.env_cfname)
        self.assertEqual(got_exception, True)

    # --------------------------------------------------------------------------
    def test_get_config_env_ok(self):
        """
        TEST: env CRAWL_CONF='envcrawl.cfg', envcrawl.cfg exists and
        is readable

        EXP: get_config(), get_config('') should load the config
        """
        get_config(reset=True)
        self.cd(self.testdir)
        os.environ['CRAWL_CONF'] = self.env_cfname
        d = copy.deepcopy(self.cdict)
        d['crawler']['filename'] = self.env_cfname
        self.write_cfg_file(self.env_cfname, d)
        os.chmod(self.env_cfname, 0644)

        got_exception = False
        try:
            cfg = get_config()
        except:
            got_exception = True
        self.assertEqual(got_exception, False)
        self.assertEqual(cfg.get('crawler', 'filename'), self.env_cfname)
        
        got_exception = False
        try:
            cfg = get_config('')
        except:
            got_exception = True
        self.assertEqual(got_exception, False)
        self.assertEqual(cfg.get('crawler', 'filename'), self.env_cfname)

    # --------------------------------------------------------------------------
    def test_get_config_exp_noread(self):
        """
        TEST: env CRAWL_CONF='envcrawl.cfg', envcrawl.cfg exists and is
              readable, unreadable explicit.cfg exists

        EXP: get_config('explicit.cfg') should should throw a
             StandardError about the file not existing or not being
             readable
        """
        get_config(reset=True)
        self.cd(self.testdir)
        os.environ['CRAWL_CONF'] = self.env_cfname
        d = copy.deepcopy(self.cdict)
        d['crawler']['filename'] = self.env_cfname
        self.write_cfg_file(self.env_cfname, d)
        os.chmod(self.env_cfname, 0644)

        d = copy.deepcopy(self.cdict)
        d['crawler']['filename'] = self.exp_cfname
        self.write_cfg_file(self.exp_cfname, d)
        os.chmod(self.exp_cfname, 0000)

        got_exception = False
        try:
            cfg = get_config(self.exp_cfname)
        except StandardError as e:
            got_exception = True
            self.assertEqual(str(e),
                             '%s is not readable' %
                             self.exp_cfname)
        self.assertEqual(got_exception, True)

    # --------------------------------------------------------------------------
    def test_get_config_exp_nosuch(self):
        """
        TEST: env CRAWL_CONF='envcrawl.cfg', envcrawl.cfg exists and
              is readable, explicit.cfg does not exist
              
        EXP: get_config('explicit.cfg') should throw a StandardError
             about the file not existing or not being readable
        """
        get_config(reset=True)
        self.cd(self.testdir)
        os.environ['CRAWL_CONF'] = self.env_cfname
        d = copy.deepcopy(self.cdict)
        d['crawler']['filename'] = self.env_cfname
        self.write_cfg_file(self.env_cfname, d)
        os.chmod(self.env_cfname, 0644)

        if os.path.exists(self.exp_cfname):
            os.unlink(self.exp_cfname)

        got_exception = False
        try:
            cfg = get_config(self.exp_cfname)
        except StandardError as e:
            got_exception = True
            self.assertEqual(str(e),
                             '%s does not exist' %
                             self.exp_cfname)
        self.assertEqual(got_exception, True)

    # --------------------------------------------------------------------------
    def test_get_config_exp_ok(self):
        """
        TEST: env CRAWL_CONF='envcrawl.cfg', envcrawl.cfg exists and is
              readable, readable explicit.cfg does exist

        EXP: get_config('explicit.cfg') should load the explicit.cfg
        """
        get_config(reset=True)
        self.cd(self.testdir)
        os.environ['CRAWL_CONF'] = self.env_cfname
        d = copy.deepcopy(self.cdict)
        d['crawler']['filename'] = self.env_cfname
        self.write_cfg_file(self.env_cfname, d)
        os.chmod(self.env_cfname, 0644)

        d = copy.deepcopy(self.cdict)
        d['crawler']['filename'] = self.exp_cfname
        self.write_cfg_file(self.exp_cfname, d)
        os.chmod(self.exp_cfname, 0644)

        got_exception = False
        try:
            cfg = get_config(self.exp_cfname)
        except StandardError as e:
            got_exception = True
        self.assertEqual(got_exception, False)
        self.assertEqual(cfg.get('crawler', 'filename'), self.exp_cfname)

    # --------------------------------------------------------------------------
    def test_get_logger_config(self):
        """
        TEST: Call get_logger('', cfg) with an empty path and a config object

        EXP: Attempts to log to cfg.get('crawler', 'logpath')
        """
        get_logger(reset=True)
        t = copy.deepcopy(self.cdict)
        logpath = '%s/test_get_logger_config.log' % self.testdir
        t['crawler']['logpath'] = logpath
        if os.path.exists(logpath):
            os.unlink(logpath)
        self.assertEqual(os.path.exists(logpath), False,
                         '%s should not exist but does' % logpath)

        cfg = CrawlConfig.CrawlConfig()
        cfg.load_dict(t)
        lobj = get_logger('', cfg)

        self.assertEqual(os.path.exists(logpath), True,
                         '%s should exist but does not' % logpath)
        self.assertNotEqual(lobj, None)
        
    # --------------------------------------------------------------------------
    def test_get_logger_default(self):
        """
        TEST: Call get_logger() with no argument

        EXP: Attempts to log to '/var/log/crawl.log'
        """
        get_logger(reset=True)
        got_exception = False
        try:
            lobj = get_logger()
        except IOError as e:
            self.assertEqual('Permission denied' in str(e), True)
            self.assertEqual('/var/log/crawl.log' in str(e), True)
            got_exception = True

        if os.getuid() != 0 and got_exception == False:
            self.fail('Only root should be able to write to the '
                      + 'default log file')
        elif os.getuid() == 0 and got_exception == True:
            self.fail('Root should be able to write to the default log file')
        
    # --------------------------------------------------------------------------
    def test_get_logger_path(self):
        """
        TEST: Call get_logger() with a pathname

        EXP: Attempts to log to pathname
        """
        get_logger(reset=True)
        logpath = '%s/test_get_logger_path.log' % self.testdir
        if os.path.exists(logpath):
            os.unlink(logpath)
        self.assertEqual(os.path.exists(logpath), False,
                         '%s should not exist but does' % logpath)
        lobj = get_logger(logpath)
        self.assertEqual(os.path.exists(logpath), True,
                         '%s should exist but does not' % logpath)
        
    # --------------------------------------------------------------------------
    def test_get_timeval(self):
        """
        TEST: various calls to CrawlConfig.get_time()

        EXP: correct number of seconds returned
        """
        os.environ['CRAWL_LOG'] = '%s/test_get_timeval.log' % self.testdir
        t = self.dict2cfg(copy.deepcopy(self.cdict))
        result = t.get_time('plugin_A', 'frequency', 1900)
        self.assertEqual(type(result), int,
                         'type of CrawlConfig.get_time result should be %s but is %s (%s)'
                         % ('int', type(result), str(result)))
        self.assertEqual(result, 3600,
                         'CrawlConfig.get_time() got %s wrong: %d'
                         % (t.get('plugin_A', 'frequency'), result))

        t.set('plugin_A', 'frequency', '5')
        result = t.get_time('plugin_A', 'frequency', 1900)
        self.assertEqual(result, 5,
                         'CrawlConfig.get_time() got %s wrong: %d'
                         % (t.get('plugin_A', 'frequency'), result))

        t.set('plugin_A', 'frequency', '5min')
        result = t.get_time('plugin_A', 'frequency', 1900)
        self.assertEqual(result, 300,
                         'CrawlConfig.get_time() got %s wrong: %d'
                         % (t.get('plugin_A', 'frequency'), result))
        
        t.set('plugin_A', 'frequency', '3 days')
        result = t.get_time('plugin_A', 'frequency', 1900)
        self.assertEqual(result, 3 * 24 * 3600,
                         'CrawlConfig.get_time() got %s wrong: %d'
                         % (t.get('plugin_A', 'frequency'), result))
        
        t.set('plugin_A', 'frequency', '2     w')
        result = t.get_time('plugin_A', 'frequency', 1900)
        self.assertEqual(result, 2 * 7 * 24 * 3600,
                         'CrawlConfig.get_time() got %s wrong: %d'
                         % (t.get('plugin_A', 'frequency'), result))
        
        t.set('plugin_A', 'frequency', '4 months')
        result = t.get_time('plugin_A', 'frequency', 1900)
        self.assertEqual(result, 4 * 30 * 24 * 3600,
                         'CrawlConfig.get_time() got %s wrong: %d'
                         % (t.get('plugin_A', 'frequency'), result))
        
        t.set('plugin_A', 'frequency', '8 y')
        result = t.get_time('plugin_A', 'frequency', 1900)
        self.assertEqual(result, 8 * 365 * 24 * 3600,
                         'CrawlConfig.get_time() got %s wrong: %d'
                         % (t.get('plugin_A', 'frequency'), result))
        
        del os.environ['CRAWL_LOG']

    # --------------------------------------------------------------------------
    def test_map_time_unit(self):
        """
        TEST: return value from map_time_unit should reflect the number of
              seconds in the indicated unit or 1 if unit not known

        EXP: expected return values encoded in umap
        """
        os.environ['CRAWL_LOG'] = '%s/test_map_time_unit.log' % self.testdir
        umap = {'s': 1, 'sec': 1, 'second': 1, 'seconds': 1,
                'm': 60, 'min': 60, 'minute': 60, 'minutes': 60,
                'h': 3600, 'hr': 3600, 'hour': 3600, 'hours': 3600,
                'd': 24 * 3600, 'day': 24 * 3600, 'days': 24 * 3600,
                'w': 7 * 24 * 3600, 'week': 7 * 24 * 3600,
                'weeks': 7 * 24 * 3600,
                'month': 30 * 24 * 3600, 'months': 30 * 24 * 3600,
                'y': 365 * 24 * 3600, 'year': 365 * 24 * 3600,
                'years': 365 * 24 * 3600,
                }
        cfg = CrawlConfig.CrawlConfig()
        for unit in umap.keys():
            result = cfg.map_time_unit(unit)
            self.assertEqual(result, umap[unit])

            unit += '_x'
            result = cfg.map_time_unit(unit)
            self.assertEqual(result, 1)
            
        del os.environ['CRAWL_LOG']

    # --------------------------------------------------------------------------
    def cd(self, dirname):
        try:
            os.chdir(dirname)
        except OSError as e:
            if 'No such file or directory' in str(e):
                os.makedirs(dirname)
                os.chdir(dirname)
            else:
                raise

    # --------------------------------------------------------------------------
    def clear_env(self):
        try:
            x = os.environ['CRAWL_CFG']
            del os.environ['CRAWL_CFG']
        except KeyError:
            pass
        
    # --------------------------------------------------------------------------
    def dict2cfg(self, d):
        """
        Load the contents of a two layer dictionary into a ConfigParser object
        !@! to be replaced by crawl_config.load_dict()
        """
        rval = CrawlConfig.CrawlConfig()
        rval.load_dict(d)
        return rval
    
    # --------------------------------------------------------------------------
    def tearDown(self):
        if os.getcwd().endswith('/test.d'):
            os.chdir(launch_dir)

    # --------------------------------------------------------------------------
    def vassert_in(self, expected, actual):
        """
        If expected does not occur in actual, report it as an error.
        """
        if not expected in actual:
            self.fail('\n"""\n%s\n"""\n\n   NOT FOUND IN\n\n"""\n%s\n"""' %
                      (expected, actual))

    # ------------------------------------------------------------------------
    def vassert_nin(self, expected, actual):
        """
        If expected occurs in actual, report it as an error.
        """
        if expected in actual:
            self.fail('\n"""\n%s\n"""\n\n   SHOULD NOT BE IN\n\n"""\n%s\n"""' %
                      (expected, actual))

    # ------------------------------------------------------------------------
    def write_cfg_file(self, fname, cfgdict):
        """
        Write a config file for testing. Put the 'crawler' section first.
        Complain if the 'crawler' section is not present.
        """
        if (not isinstance(cfgdict, dict) and
            not isinstance(cfgdict, CrawlConfig.CrawlConfig)):
            
            raise StandardError("cfgdict has invalid type %s" % type(cfgdict))
        
        elif isinstance(cfgdict, dict):
            cfg = CrawlConfig.CrawlConfig()
            cfg.load_dict(cfgdict)

        elif isinstance(cfgdict, CrawlConfig.CrawlConfig):
            cfg = cfgdict
            
        if 'crawler' not in cfg.sections():
            raise StandardError("section 'crawler' missing from test config file")
        
        f = open(fname, 'w')
        cfg.write(f)
        f.close()

    # ------------------------------------------------------------------------
    def write_plugmod(self, plugdir, plugname):
        """
        Create a plugin module to test firing
        """
        if not os.path.exists(plugdir):
            os.makedirs(plugdir)

        if plugname.endswith('.py'):
            plugname = re.sub(r'\.py$', '', plugname)

        plugfname = plugname + '.py'

        f = open('%s/%s' % (plugdir, plugfname), 'w')
        f.write("#!/bin/env python\n")
        f.write("def main(cfg):\n")
        f.write("    q = open('%s/fired', 'w')\n" % self.testdir)
        f.write(r"    q.write('plugin %s fired\n')" % plugname)
        f.write("\n")
        f.write("    q.close()\n")
        f.close()
        
    # ------------------------------------------------------------------------
    def tearDown(self):
        if is_running():
            testhelp.touch('crawler.exit')
            time.sleep(1.0)
            
        if is_running():
            result = pexpect.run("ps -ef")
            for line in result.split("\n"):
                if 'crawl start' in line:
                    pid = line.split()[1]
                    print("pid = %s <- kill this" % pid)

        if os.path.exists('crawler_pid'):
            os.unlink('crawler_pid')
        
# ------------------------------------------------------------------------------
launch_dir = os.path.abspath(os.path.dirname(sys.argv[0]))
toolframe.tf_launch("crl",
                    setup_tests=Crawl_setup,
                    cleanup_tests=Crawl_cleanup,
                    testclass='CrawlTest',
                    logfile='crawl_test.log')
