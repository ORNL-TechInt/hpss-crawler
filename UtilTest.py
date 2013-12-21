#!/usr/bin/env python
"""
Tests for util.py
"""
import CrawlConfig
import logging
import os
import pdb
import socket
import sys
import testhelp
import time
import toolframe
import unittest
import util

# -----------------------------------------------------------------------------
def setUpModule():
    """
    Set up for testing
    """
    testhelp.module_test_setup(UtilTest.testdir)

# -----------------------------------------------------------------------------
def tearDownModule():
    """
    Clean up after testing
    """
    util.conditional_rm("/tmp/crawl.log")
    testhelp.module_test_teardown(UtilTest.testdir)
    
# -----------------------------------------------------------------------------
class UtilTest(testhelp.HelpedTestCase):
    """
    Tests for util.py
    """
    testdir = './test.d'
    
    # -------------------------------------------------------------------------
    def test_csv_list(self):
        """
        csv_list() called with whitespace should return an empty list
        csv_list() on whitespace with a comma in it => ['', '']
        csv_list() on 'a, b   , c' => ['a', 'b', 'c']
        """
        self.expected([''], util.csv_list(""))
        self.expected([''], util.csv_list("     "))
        self.expected(['', ''], util.csv_list("  , "))
        self.expected(['abc', ''], util.csv_list("  abc, "))
        self.expected(['a', 'b', 'c'], util.csv_list(" a,b ,  c  "))
                       
    # -------------------------------------------------------------------------
    def test_content(self):
        """
        contents() is supposed to read and return the contents of a file as a
        string.
        """
        x = util.contents('./util.py')
        self.assertEqual(type(x), str,
                         "Expected a string but got a %s" % type(x))
        expected = 'def contents('
        self.assertTrue(expected in x,
                      "Expected to find '%s' in \"\"\"\n%s\n\"\"\"" %
                      (expected, x))

    # -------------------------------------------------------------------------
    def test_get_logger_00(self):
        """
        With no logger cached, reset=False and soft=False should create a
        new logger. If a logger has been created, this case should return the
        cached logger.
        """
        # throw away any logger that has been set
        util.get_logger(reset=True, soft=True)
        
        # get_logger(..., reset=False, soft=False) should create a new one
        actual = util.get_logger(cmdline='%s/util.log' % self.testdir,
                                 reset=False, soft=False)
        self.assertTrue(isinstance(actual, logging.Logger),
                      "Expected logging.Logger, got %s" % (actual))
        self.expected(os.path.abspath("%s/util.log" % self.testdir),
                      actual.handlers[0].baseFilename)

        # now ask for a logger with a different name, with reset=False,
        # soft=False. Since one has already been created, the new name should
        # be ignored and we should get back the one already cached.
        util.get_logger(cmdline='%s/util_foobar.log' % self.testdir,
                        reset=False, soft=False)
        self.assertTrue(isinstance(actual, logging.Logger),
                      "Expected logging.Logger, got %s" % (actual))
        self.expected(os.path.abspath("%s/util.log" % self.testdir),
                      actual.handlers[0].baseFilename)
        
    # -------------------------------------------------------------------------
    def test_get_logger_01(self):
        """
        With no logger cached, reset=False and soft=True should not create a
        new logger. If a logger has been created, this case should return the
        cached logger.
        """
        # throw away any logger that has been set
        util.get_logger(reset=True, soft=True)
        
        # then see what happens with reset=False, soft=True
        actual = util.get_logger(cmdline='%s/util.log' % self.testdir,
                                 reset=False, soft=True)
        self.expected(None, actual)

        # now create a logger
        util.get_logger(cmdline='%s/util.log' % self.testdir)
        # now reset=False, soft=True should return the one just created
        actual = util.get_logger(reset=False, soft=True)
        self.assertTrue(isinstance(actual, logging.Logger),
                      "Expected logging.Logger, got %s" % (actual))
        self.expected(os.path.abspath("%s/util.log" % self.testdir),
                      actual.handlers[0].baseFilename)
        
    # -------------------------------------------------------------------------
    def test_get_logger_10(self):
        """
        Calling get_logger with reset=True, soft=False should get rid of the
        previously cached logger and make a new one.
        """
        # throw away any logger that has been set and create one to be
        # overridden
        tmp = util.get_logger(cmdline='%s/throwaway.log' % self.testdir,
                              reset=True)
                              
        # verify that it's there with the expected attributes
        self.assertTrue(isinstance(tmp, logging.Logger),
                        "Expected logging.Logger, got %s" % (tmp))
        self.expected(1, len(tmp.handlers))
        self.expected(os.path.abspath("%s/throwaway.log" % self.testdir),
                      tmp.handlers[0].baseFilename)

        # now override it
        actual = util.get_logger(cmdline='%s/util.log' % self.testdir,
                                 reset=True, soft=False)
        # and verify that it got replaced
        self.assertTrue(isinstance(actual, logging.Logger),
                        "Expected logging.Logger, got %s" % (actual))
        self.expected(1, len(actual.handlers))
        self.expected(os.path.abspath("%s/util.log" % self.testdir),
                      actual.handlers[0].baseFilename)
        
    # -------------------------------------------------------------------------
    def test_get_logger_11(self):
        """
        Calling get_logger with both reset=True and soft=True should throw away
        any cached logger and return None without creating a new one.
        """
        exp = None
        actual = util.get_logger(reset=True, soft=True)
        self.expected(exp, actual)
        
    # -------------------------------------------------------------------------
    def test_get_logger_cfg(self):
        """
        Call get_logger with a config that specifies non default values for log
        file name, log file size, and max log files on disk. Verify that the
        resulting logger has the correct parameters.
        """
        cfname = "%s/%s.cfg" % (self.testdir, util.my_name())
        lfname = "%s/%s.log" % (self.testdir, util.my_name())
        cdict = {'crawler': {'logpath': lfname,
                             'logsize': '17mb',
                             'logmax': '13'
                             }
                 }
        c = CrawlConfig.CrawlConfig()
        c.load_dict(cdict)

        # reset any logger that has been initialized
        util.get_logger(reset=True, soft=True)

        # now ask for one that matches the configuration
        l = util.get_logger(cfg=c)

        # and check that it has the right handler
        self.expected(1, len(l.handlers))
        self.expected(os.path.abspath(lfname), l.handlers[0].stream.name)
        self.expected(17*1000*1000, l.handlers[0].maxBytes)
        self.expected(13, l.handlers[0].backupCount)

    # -------------------------------------------------------------------------
    def test_get_logger_nocfg(self):
        """
        Call get_logger with no cmdline or cfg arguments and make sure the
        resulting logger has the correct parameters.
        """
        # reset any logger that has been initialized
        util.get_logger(reset=True, soft=True)

        # now ask for a default logger
        l = util.get_logger()

        # and check that it has the right handler
        self.expected(1, len(l.handlers))
        self.expected("/tmp/crawl.log", l.handlers[0].stream.name)
        self.expected(10*1024*1024, l.handlers[0].maxBytes)
        self.expected(5, l.handlers[0].backupCount)

    # -------------------------------------------------------------------------
    def test_line_quote(self):
        """
        line_quote is supposed to wrap a string in line-based quote marks
        (three double quotes in a row) on separate lines. Any single or double
        quotes wrapping the incoming string are stripped off in the output.
        """
        exp = '\n"""\nabc\n"""'
        act = util.line_quote('abc')
        self.assertEqual(exp, act,
                         "Expected %s, got %s" % (exp, act))

        exp = '\n"""\nabc\n"""'
        act = util.line_quote("'abc'")
        self.assertEqual(exp, act,
                         "Expected %s, got %s" % (exp, act))
                      
        exp = '\n"""\nabc\n"""'
        act = util.line_quote('"abc"')
        self.assertEqual(exp, act,
                         "Expected %s, got %s" % (exp, act))

    # -------------------------------------------------------------------------
    def test_my_name(self):
        """
        Return the name of the calling function.
        """
        actual = util.my_name()
        expected = 'test_my_name'
        self.assertEqual(expected, actual,
                         "Expected '%s' to match '%s'" %
                         (expected, actual))

# -----------------------------------------------------------------------------
if __name__ == '__main__':
    toolframe.ez_launch(test='UtilTest',
                        logfile='crawl_test.log')
