#!/usr/bin/python
"""
crawl - Run a bunch of plug-ins which will probe the integrity of HPSS
"""
import ConfigParser
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
import sys
import testhelp
import time
import toolframe
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

    section_l = cfg.sections()
    
    if o.target == 'stdout':
        for section in section_l:
            print("[%s]" % section)
            for option in cfg.options(section):
                print("%s = %s" % (option, cfg.get(section, option)))
    elif o.target == 'log':
        log = get_logger(o.logpath, cfg)
        for section in section_l:
            log.info("[%s]" % section)
            for option in cfg.options(section):
                log.info("%s = %s" % (option, cfg.get(section, option)))
        
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
    lfname = cfg.get('crawler', 'logpath')
    log = get_logger(o.logpath, cfg)

    if not cfg.has_section(o.plugname):
        print("No plugin named '%s' is defined")
    else:
        plugdir = cfg.get('crawler', 'plugin_dir')
        sys.path.append(plugdir)
        __import__(o.plugname)
        log.info('firing %s' % o.plugname)
        sys.modules[o.plugname].main()
    
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

    if os.path.exists('crawler_pid'):
        print('crawler_pid exists. If you are sure it is not running,\n' +
              'please remove crawler_pid and try again.')
    else:
        crawler = CrawlDaemon('crawler_pid',
                              stdout="crawler.stdout",
                              stderr="crawler.stderr",
                              logger=get_logger(o.logfile),
                              workdir='.')
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
def get_config(cfname=''):
    if cfname == '':
        envval = os.getenv('CRAWL_CONF')
        if None != envval:
            cfname = envval
    
    if cfname == '':
        cfname = 'crawl.cfg'

    if not os.access(cfname, os.R_OK):
        raise StandardError("%s does not exist or is not readable" % cfname)
    rval = ConfigParser.SafeConfigParser()
    rval.read(cfname)
    return rval

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

    filename = '/var/log/crawl.log'
    if cmdline != '':
        filename = cmdline
    elif cfg != None:
        try:
            filename = cfg.get('crawler', 'logpath')
        except:
            pass

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
def Crawl_setup():
    """
    Setup needed before running the tests.
    """
    if not os.path.isdir(Crawl.testdir):
        os.mkdir(Crawl.testdir)
    
# ------------------------------------------------------------------------------
def Crawl_cleanup():
    """
    Clean up after a sequence of tests.
    """
    if not testhelp.keepfiles():
        flist = ['test_crawl.log',
                 'test_start.log',
                 'test_*.cfg',
                 'test_*.log',
                 'test.d'
                 ]
        for fspec in flist:
            for fname in glob.glob(fspec):
                if os.path.isdir(fname):
                    shutil.rmtree(fname)
                elif os.path.exists(fname):
                    os.unlink(fname)

    if is_running():
        testhelp.touch('crawler.exit')

    if os.getcwd().endswith('/test.d'):
        os.chdir('..')
        
# ------------------------------------------------------------------------------
class CrawlDaemon(daemon.Daemon):
    # --------------------------------------------------------------------------
    def run(self):
        """
        This routine runs in the background as a daemon. Here's where
        we fire off plug-ins as appropriate.
        """
        exitfile = 'crawler.exit'
        while True:
            time.sleep(1)

            if os.path.exists(exitfile):
                os.unlink(exitfile)
                self.dlog('crawler shutting down')
                break

            if 0 == int(time.time()) % 10:
                self.dlog('crawler is running at %s'
                          % time.strftime('%Y.%m%d %H:%M:%S'))

# ------------------------------------------------------------------------------
class Crawl(unittest.TestCase):

    testdir = 'test.d'
    default_cfname = 'crawl.cfg'
    env_cfname = 'envcrawl.cfg'
    exp_cfname = 'explicit.cfg'
    default_logpath = '%s/test_default_hpss_crawl.log' % testdir
    cfg = {'crawler': {'plugin_dir': '%s/plugins' % testdir,
                       'logpath': default_logpath,
                       'logsize': '5mb',
                       'logmax': '5',
                       'e-mail_recipients':
                       'tbarron@ornl.gov, tusculum@gmail.com',
                       'trigger': '<command-line>'
                       },
           'plugin-A': {'frequency': '1h',
                        'operations': '15'
                        }
           }

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
        self.vassert_in("%s does not exist or is not readable" % cfname,
                        result)

    # --------------------------------------------------------------------------
    def test_crawl_cfgdump_log_nopath(self):
        """
        TEST: "crawl cfgdump -c <cfgpath> --to log"
        EXP: what is written to log matches what was written to
        cfgpath. output should go to log path named in cfg.
        """
        cfname = "%s/test_crawl_cfgdump_log_n.cfg" % self.testdir
        self.write_cfg_file(cfname, self.cfg)
        cmd = 'crawl cfgdump -c %s --to log' % cfname
        result = pexpect.run(cmd)
        # print(">>>\n%s\n<<<" % result)
        self.vassert_nin("Traceback", result)
        self.assertEqual(os.path.exists(self.default_logpath), True)
        lcontent = contents(self.default_logpath)
        for section in self.cfg.keys():
            self.vassert_in('[%s]' % section, lcontent)

            for item in self.cfg[section].keys():
                self.vassert_in('%s = %s' %
                                (item, self.cfg[section][item]), lcontent)
        
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
        self.write_cfg_file(cfname, self.cfg)
        cmd = ('crawl cfgdump -c %s --to log --logpath %s'
               % (cfname, logpath))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.assertEqual(os.path.exists(logpath), True)
        lcontent = contents(logpath)
        # print(">>>\n%s\n<<<" % result)
        for section in self.cfg.keys():
            self.vassert_in('[%s]' % section, lcontent)

            for item in self.cfg[section].keys():
                self.vassert_in('%s = %s' %
                                (item, self.cfg[section][item]), lcontent)
        
    # --------------------------------------------------------------------------
    def test_crawl_cfgdump_stdout(self):
        """
        TEST: "crawl cfgdump -c <cfgpath> --to stdout"
        EXP: what is written to stdout matches what was written to cfgpath
        """
        cfname = "%s/test_crawl_cfgdump_stdout.cfg" % self.testdir
        self.write_cfg_file(cfname, self.cfg)
        cmd = 'crawl cfgdump -c %s --to stdout' % cfname
        result = pexpect.run(cmd)
        # print(">>>\n%s\n<<<" % result)
        for section in self.cfg.keys():
            self.vassert_in('[%s]' % section, result)

            for item in self.cfg[section].keys():
                self.vassert_in('%s = %s' %
                                (item, self.cfg[section][item]), result)
        
    # --------------------------------------------------------------------------
    def test_crawl_fire_log_path(self):
        """
        TEST: crawl fire --plugin <plugmod>
        EXP: plugin fired and output went to specified log path
        """
        cfname = "%s/test_crawl_fire_log.cfg" % self.testdir
        lfname = "%s/test_crawl_fire.log" % self.testdir
        plugdir = '%s/plugins' % self.testdir
        plugname = 'plugin_1'
        
        # create a plug module
        self.write_plugmod(plugdir, plugname)
        
        # add the plug module to the config
        t = copy.deepcopy(self.cfg)
        t[plugname] = {}
        t[plugname]['frequency'] = '1m'
        self.write_cfg_file(cfname, t)

        # carry out the test
        cmd = ('crawl fire -c %s --plugin %s --logpath %s' %
               (cfname, plugname, lfname))
        result = pexpect.run(cmd)

        # verify that command ran successfully
        self.vassert_nin("Traceback", result)
        
        # test.d/plugins/plugin_1.py should exist
        if not plugname.endswith('.py'):
            plugname += '.py'
        self.assertEqual(os.path.exists('%s/%s' % (plugdir, plugname)), True)
        
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
        cmd = 'crawl start --log test_start.log --context TEST'
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)

        self.assertEqual(is_running(), True)
        self.assertEqual(os.path.exists('crawler_pid'), True)

        testhelp.touch('crawler.exit')

        time.sleep(1)
        self.assertEqual(is_running(), False)
        self.assertEqual(os.path.exists('crawler_pid'), False)
                
    # --------------------------------------------------------------------------
    def test_crawl_start_already(self):
        """
        TEST: If the crawler is already running, decline to run a second copy.
        """
        cmd = 'crawl start --log test_start.log --context TEST'
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)

        self.assertEqual(is_running(), True)

        result = pexpect.run(cmd)
        self.assertEqual('crawler_pid exists' in result, True)
        
        testhelp.touch('crawler.exit')

        time.sleep(1)
        self.assertEqual(is_running(), False)
        
    # --------------------------------------------------------------------------
    def test_crawl_status(self):
        """
        TEST: 'crawl status' should report the crawler status correctly.
        """
        logpath = '%s/test_start.log' % self.testdir
        cmd = 'crawl status'
        result = pexpect.run(cmd)
        self.assertEqual(result.strip(), "The crawler is not running.")
        
        cmd = 'crawl start --log %s --context TEST' % (logpath)
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)

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
        cmd = 'crawl start --log %s --context TEST' % (logpath)
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)

        self.assertEqual(is_running(), True)
        self.assertEqual(os.path.exists('crawler_pid'), True)

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
        self.cd(self.testdir)
        self.clear_env()
        d = copy.deepcopy(self.cfg)
        d['crawler']['filename'] = self.default_cfname
        self.write_cfg_file(self.default_cfname, self.cfg)
        os.chmod(self.default_cfname, 0000)

        got_exception = False
        try:
            cfg = get_config()
        except StandardError as e:
            got_exception = True
            self.assertEqual(str(e),
                             '%s does not exist or is not readable' %
                             self.default_cfname)
        self.assertEqual(got_exception, True)
        
        got_exception = False
        try:
            cfg = get_config('')
        except StandardError as e:
            got_exception = True
            self.assertEqual(str(e),
                             '%s does not exist or is not readable' %
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
        self.cd(self.testdir)
        self.clear_env()
        if os.path.exists(self.default_cfname):
            os.unlink(self.default_cfname)

        got_exception = False
        try:
            cfg = get_config()
        except StandardError as e:
            got_exception = True
            self.assertEqual(str(e),
                             '%s does not exist or is not readable' %
                             self.default_cfname)
        self.assertEqual(got_exception, True)
        
        got_exception = False
        try:
            cfg = get_config('')
        except StandardError as e:
            got_exception = True
            self.assertEqual(str(e),
                             '%s does not exist or is not readable' %
                             self.default_cfname)
        self.assertEqual(got_exception, True)
        # tearDown will 'cd ..'
        
    # --------------------------------------------------------------------------
    def test_get_config_def_ok(self):
        """
        TEST: env not set, 'crawl.cfg' does exist =>

        EXP: get_config() or get_config('') should load the config
        """
        self.cd(self.testdir)
        self.clear_env()
        d = copy.deepcopy(self.cfg)
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

        got_exception = False
        try:
            cfg = get_config('')
        except:
            got_exception = True
        self.assertEqual(got_exception, False)
        self.assertEqual(cfg.get('crawler', 'filename'), self.default_cfname)
        
        
    # --------------------------------------------------------------------------
    def test_get_config_env_noread(self):
        """
        TEST: env CRAWL_CONF='envcrawl.cfg', envcrawl.cfg exists but
        is not readable

        EXP: get_config(), get_config('') should throw a StandardError
        about the file not existing or not being readable
        """
        self.cd(self.testdir)
        os.environ['CRAWL_CONF'] = self.env_cfname
        d = copy.deepcopy(self.cfg)
        d['crawler']['filename'] = self.env_cfname
        self.write_cfg_file(self.env_cfname, d)
        os.chmod(self.env_cfname, 0000)

        got_exception = False
        try:
            cfg = get_config()
        except StandardError as e:
            got_exception = True
            self.assertEqual(str(e),
                             '%s does not exist or is not readable' %
                             self.env_cfname)
        self.assertEqual(got_exception, True)
        
        got_exception = False
        try:
            cfg = get_config('')
        except StandardError as e:
            got_exception = True
            self.assertEqual(str(e),
                             '%s does not exist or is not readable' %
                             self.env_cfname)
        self.assertEqual(got_exception, True)

        
    # --------------------------------------------------------------------------
    def test_get_config_env_nosuch(self):
        """
        TEST: env CRAWL_CONF='envcrawl.cfg', envcrawl.cfg does not exist
        
        EXP: get_config(), get_config('') should throw a StandardError
        about the file not existing or not being readable
        """
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
                             '%s does not exist or is not readable' %
                             self.env_cfname)
        self.assertEqual(got_exception, True)
        
        got_exception = False
        try:
            cfg = get_config('')
        except StandardError as e:
            got_exception = True
            self.assertEqual(str(e),
                             '%s does not exist or is not readable' %
                             self.env_cfname)
        self.assertEqual(got_exception, True)

    # --------------------------------------------------------------------------
    def test_get_config_env_ok(self):
        """
        TEST: env CRAWL_CONF='envcrawl.cfg', envcrawl.cfg exists and
        is readable

        EXP: get_config(), get_config('') should load the config
        """
        self.cd(self.testdir)
        os.environ['CRAWL_CONF'] = self.env_cfname
        d = copy.deepcopy(self.cfg)
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
        self.cd(self.testdir)
        os.environ['CRAWL_CONF'] = self.env_cfname
        d = copy.deepcopy(self.cfg)
        d['crawler']['filename'] = self.env_cfname
        self.write_cfg_file(self.env_cfname, d)
        os.chmod(self.env_cfname, 0644)

        d = copy.deepcopy(self.cfg)
        d['crawler']['filename'] = self.exp_cfname
        self.write_cfg_file(self.exp_cfname, d)
        os.chmod(self.exp_cfname, 0000)

        got_exception = False
        try:
            cfg = get_config(self.exp_cfname)
        except StandardError as e:
            got_exception = True
            self.assertEqual(str(e),
                             '%s does not exist or is not readable' %
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
        self.cd(self.testdir)
        os.environ['CRAWL_CONF'] = self.env_cfname
        d = copy.deepcopy(self.cfg)
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
                             '%s does not exist or is not readable' %
                             self.exp_cfname)
        self.assertEqual(got_exception, True)

    # --------------------------------------------------------------------------
    def test_get_config_exp_ok(self):
        """
        TEST: env CRAWL_CONF='envcrawl.cfg', envcrawl.cfg exists and is
              readable, readable explicit.cfg does exist

        EXP: get_config('explicit.cfg') should load the explicit.cfg
        """
        self.cd(self.testdir)
        os.environ['CRAWL_CONF'] = self.env_cfname
        d = copy.deepcopy(self.cfg)
        d['crawler']['filename'] = self.env_cfname
        self.write_cfg_file(self.env_cfname, d)
        os.chmod(self.env_cfname, 0644)

        d = copy.deepcopy(self.cfg)
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
        t = copy.deepcopy(self.cfg)
        logpath = '%s/test_get_logger_config.log' % self.testdir
        t['crawler']['logpath'] = logpath
        if os.path.exists(logpath):
            os.unlink(logpath)
        self.assertEqual(os.path.exists(logpath), False,
                         '%s should not exist but does' % logpath)

        lobj = get_logger('', self.dict2cfg(t))

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
    def cd(self, dirname):
        try:
            os.chdir(dirname)
        except OSError as e:
            if 'No such file or directory' in str(e):
                os.mkdir(dirname)
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
        """
        rval = ConfigParser.SafeConfigParser()
        for s in sorted(d.keys()):
            rval.add_section(s)
            for o in sorted(d[s].keys()):
                rval.set(s, o, d[s][o])
        return rval
    
    # --------------------------------------------------------------------------
    def tearDown(self):
        if os.getcwd().endswith('/test.d'):
            os.chdir('..')

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
        if 'crawler' not in cfgdict.keys():
            raise StandardError("section 'crawler' missing from test config file")

        dname = os.path.dirname(fname)
        if dname != '' and not os.path.exists(dname):
            os.mkdir(dname)
        
        f = open(fname, 'w')
        section_l = cfgdict.keys()
        section_l.remove('crawler')
        section_l = ['crawler'] + section_l

        for section in section_l:
            f.write("[%s]\n" % section)
            for item in cfgdict[section].keys():
                f.write("%s = %s\n" % (item, cfgdict[section][item]))
            f.write("\n")
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
        f.write("def main():\n")
        f.write("    q = open('test.d/fired', 'w')\n")
        f.write(r"    q.write('plugin %s fired\n')" % plugname)
        f.write("\n")
        f.write("    q.close()\n")
        f.close()
        
# ------------------------------------------------------------------------------
toolframe.tf_launch("crl",
                    setup_tests=Crawl_setup,
                    cleanup_tests=Crawl_cleanup,
                    testclass='Crawl',
                    logfile='crawl_test.log')
