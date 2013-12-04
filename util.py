#!/usr/bin/env python

import logging
import os
import pdb
import socket
import sys
import toolframe
import unittest

# -----------------------------------------------------------------------------
def contents(filename):
    """
    Return the contents of the file as a string
    """
    f = open(filename, 'r')
    rval = f.read()
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
def get_logger(cmdline='', cfg=None, reset=False, soft=False):
    """
    Return the logging object for this process. Instantiate it if it
    does not exist already.

    cmdline contains the log file name from the command line if one was
    specified.

    cfg contains a configuration object or None.

    reset == True means that the caller wants to close any currently open log
    file and open a new one rather than returning the one that's already open.

    soft == True means that if a logger does not exist, we don't want to open
    one but return None instead. Normally, if a logger does not exist, we
    create it.
    """
    if reset:
        try:
            l = get_logger._logger
            for h in l.handlers:
                h.close()
            del get_logger._logger
        except AttributeError:
            pass

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
        if soft:
            return None
        get_logger._logger = setup_logging(filename, 'crawl')
        rval = get_logger._logger

    return rval

# -----------------------------------------------------------------------------
def line_quote(value):
    if type(value) == str and value.startswith("'"):
        rv = value.strip("'")
    elif type(value) == str and value.startswith('"'):
        rv = value.strip('"')
    else:
        rv = value
    return '\n"""\n%s\n"""' % str(rv)

# -----------------------------------------------------------------------------
def my_name():
    """
    Return the caller's name
    """
    return sys._getframe(1).f_code.co_name

# -----------------------------------------------------------------------------
def setup_logging(logfile='',
                  logname='crawl',
                  maxBytes=10*1024*1024,
                  backupCount=5,
                  bumper=True):
    """
    Create a new logger and return the object
    """
    if logfile == '':
        raise StandardError("setup_logging: No log file name provided")
    
    rval = logging.getLogger(logname)
    rval.setLevel(logging.INFO)
    host = socket.gethostname().split('.')[0]
    if rval.handlers != [] and logfile != rval.handlers[0].baseFilename:
        rval.handlers[0].close()
        del rval.handlers[0]
    if rval.handlers == []:
        fh = logging.handlers.RotatingFileHandler(logfile,
                                                  maxBytes=10*1024*1024,
                                                  backupCount=5)
        strfmt = "%" + "(asctime)s [%s] " % host + "%" + "(message)s"
        fmt = logging.Formatter(strfmt, datefmt="%Y.%m%d %H:%M:%S")
        fh.setFormatter(fmt)

        rval.addHandler(fh)
    if bumper:
        rval.info('-' * (55 - len(host)))
    return rval

# -----------------------------------------------------------------------------
class UtilTest(unittest.TestCase):
    # -------------------------------------------------------------------------
    def test_content(self):
        x = contents('./util.py')
        self.assertEqual(type(x), str,
                         "Expected a string but got a %s" % type(x))
        expected = 'def contents('
        self.assertTrue(expected in x,
                      "Expected to find '%s' in \"\"\"\n%s\n\"\"\"" %
                      (expected, x))

    # -------------------------------------------------------------------------
    def test_line_quote(self):
        exp = '\n"""\nabc\n"""'
        act = line_quote('abc')
        self.assertEqual(exp, act,
                         "Expected %s, got %s" % (exp, act))

        exp = '\n"""\nabc\n"""'
        act = line_quote("'abc'")
        self.assertEqual(exp, act,
                         "Expected %s, got %s" % (exp, act))
                      
        exp = '\n"""\nabc\n"""'
        act = line_quote('"abc"')
        self.assertEqual(exp, act,
                         "Expected %s, got %s" % (exp, act))

    # -------------------------------------------------------------------------
    def test_my_name(self):
        actual = my_name()
        expected = 'test_my_name'
        self.assertEqual(expected, actual,
                         "Expected '%s' to match '%s'" %
                         (expected, actual))

# -----------------------------------------------------------------------------
if __name__ == '__main__':
    toolframe.ez_launch(test='UtilTest',
                        logfile='crawl_test.log')
