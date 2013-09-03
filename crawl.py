#!/usr/bin/python
"""
crawl - Run a bunch of plug-ins which will probe the integrity of HPSS
"""
import daemon
import logging
import optparse
import os
import pdb
import pexpect
import re
import socket
import sys
import testhelp
import time
import toolframe
import unittest

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
    pass

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
              'please remove crawler_pid.')
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
def get_logger(filename="/var/log/integrity.log"):
    """
    Return the logging object for this process. Instantiate it if it
    does not exist already.
    """
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
def Crawl_cleanup():
    """
    Clean up after a sequence of tests.
    """
    if not testhelp.keepfiles():
        flist = ['test_crawl.log',
                 'test_start.log'
                 ]
        for fname in flist:
            try:
                os.unlink(fname)
            except:
                pass

    if is_running():
        testhelp.touch('crawler.exit')
        
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

    # --------------------------------------------------------------------------
    def test_crawl_log(self):
        """
        TEST: "crawl log --log filename message" will write message to filename
        """
        lfname = 'test_crawl.log'
        msg = "this is a test log message"
        cmd = "crawl log --log %s %s" % (lfname, msg)
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_in(msg, contents(lfname))
        
    # --------------------------------------------------------------------------
    def test_crawl_start(self):
        """
        TEST: 'crawl start' should fire up a daemon crawler which will exit
        when file 'crawler.exit' is touched. Verify that crawler_pid is exists
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
        cmd = 'crawl status'
        result = pexpect.run(cmd)
        self.assertEqual(result.strip(), "The crawler is not running.")
        
        cmd = 'crawl start --log test_start.log --context TEST'
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)

        self.assertEqual(is_running(), True)
        self.assertEqual(os.path.exists('crawler_pid'), True)

        cmd = 'crawl status'
        result = pexpect.run(cmd)
        self.assertEqual('The crawler is running as process' in result,
                         True)

        cmd = 'crawl stop --log test_start.log --context TEST'
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
        cmd = 'crawl start --log test_start.log --context TEST'
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)

        self.assertEqual(is_running(), True)
        self.assertEqual(os.path.exists('crawler_pid'), True)

        cmd = 'crawl stop --log test_start.log'
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        time.sleep(1)
        
        self.assertEqual(is_running(), False)
        self.assertEqual(os.path.exists('crawler_pid'), False)
        
# ------------------------------------------------------------------------------
toolframe.tf_launch("crl",
                    cleanup_tests=Crawl_cleanup,
                    testclass='Crawl',
                    logfile='crawl_test.log')
