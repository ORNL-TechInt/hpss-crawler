#!/usr/bin/env python
"""
Tests for util.py
"""
from hpssic import CrawlConfig
import logging
import os
import pdb
import re
import sys
from hpssic import testhelp
from hpssic import toolframe
from hpssic import util

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
    if not testhelp.keepfiles():
        util.conditional_rm("/tmp/crawl.log")
    testhelp.module_test_teardown(UtilTest.testdir)
    
# -----------------------------------------------------------------------------
# def logErr(record):
#     raise

# -----------------------------------------------------------------------------
class UtilTest(testhelp.HelpedTestCase):
    """
    Tests for util.py
    """
    testdir = testhelp.testdata(__name__)
    
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
        self.expected(['xyz'], util.csv_list("xyz"))
        self.expected(['abc', ''], util.csv_list("  abc, "))
        self.expected(['a', 'b', 'c'], util.csv_list(" a,b ,  c  "))
                       
    # -------------------------------------------------------------------------
    def test_content(self):
        """
        contents() is supposed to read and return the contents of a file as a
        string.
        """
        # x = util.contents('hpssic/util.py')
        filename = sys.modules['hpssic.util'].__file__.replace(".pyc", ".py")
        x = util.contents(filename)
        self.assertEqual(type(x), str,
                         "Expected a string but got a %s" % type(x))
        expected = 'def contents('
        self.assertTrue(expected in x,
                      "Expected to find '%s' in \"\"\"\n%s\n\"\"\"" %
                      (expected, x))

    # --------------------------------------------------------------------------
    def test_date_end(self):
        """
        Given a file containing several log records, return the timestamp on
        the last one.
        """
        tdata = ["This line should be ignored\n",
                 "2014.0412 12:25:50 This is not the timestamp to return\n",
                 "2014.0430 19:30:00 This should not be returned\n",
                 "2014.0501 19:30:00 Return this one\n",
                 "We want plenty of data here at the end of the file\n",
                 "with no timestamp so we'll be forced to read\n",
                 "backward a time or two, not just find the timestamp\n",
                 "on the first read so we exercise revread.\n"]
        tfilename = "%s/%s.data" % (self.testdir, util.my_name())
        f = open(tfilename, 'w')
        f.writelines(tdata)
        f.close()

        self.expected("2014.0501", util.date_end(tfilename))
        
    # --------------------------------------------------------------------------
    def test_date_start(self):
        """
        Given a file containing several log records (with some irrelevant
        introductory material), return the timestamp on the first one.
        """
        tdata = ["This line should be ignored\n",
                 "2014.0412 12:25:50 This is the timestamp to return\n",
                 "2014.0430 19:30:00 This should not be returned\n"]
        tfilename = "%s/%s.data" % (self.testdir, util.my_name())
        f = open(tfilename, 'w')
        f.writelines(tdata)
        f.close()

        self.expected("2014.0412", util.date_start(tfilename))


    # --------------------------------------------------------------------------
    # def test_date_parse(self):
    #     """
    #     
    #     """
        
    # --------------------------------------------------------------------------
    def test_env_add_folded_none(self):
        """
        TEST: add to an undefined environment variable from a folded [env]
        entry

        EXP: the value gets set to the payload with the whitespace squeezed out
        """
        sname = 'env'
        evname = 'UTIL_TEST'
        add = "four:\n   five:\n   six"
        exp = re.sub("\n\s*", "", add)

        # make sure the target env variable is not defined
        if evname in os.environ:
            del os.environ[evname]
            
        # create a config object with an 'env' section and a '+' option
        cfg = CrawlConfig.CrawlConfig()
        cfg.add_section(sname)
        cfg.set(sname, evname, '+' + add)
        
        # pass the config object to util.env_update()
        util.env_update(cfg)
        
        # verify that the variable was set to the expected value
        self.expected(exp, os.environ[evname])
        
        # raise testhelp.UnderConstructionError()
    
    # --------------------------------------------------------------------------
    def test_env_add_folded_pre(self):
        """
        TEST: add to a preset environment variable from a folded [env]
        entry

        EXP: the value gets set to the payload with the whitespace squeezed out
        """
        sname = 'env'
        evname = 'UTIL_TEST'
        pre_val = "one:two:three"
        add = "four:\n   five:\n   six"
        exp = ":".join([pre_val, re.sub("\n\s*", "", add)])

        # make sure the target env variable has the expected value
        os.environ[evname] = pre_val
            
        # create a config object with an 'env' section and a folded '+' option
        cfg = CrawlConfig.CrawlConfig()
        cfg.add_section(sname)
        cfg.set(sname, evname, '+' + add)
        
        # pass the config object to util.env_update()
        util.env_update(cfg)
        
        # verify that the variable was set to the expected value
        self.expected(exp, os.environ[evname])
        
        # raise testhelp.UnderConstructionError()
    
    # --------------------------------------------------------------------------
    def test_env_add_none(self):
        """
        TEST: add to an undefined environment variable from [env] entry

        EXP: the value gets set to the payload
        """
        sname = 'env'
        evname = 'UTIL_TEST'
        add = "four:five:six"
        exp = add

        # make sure the target env variable is not defined
        if evname in os.environ:
            del os.environ[evname]
            
        # create a config object with an 'env' section and a '+' option
        cfg = CrawlConfig.CrawlConfig()
        cfg.add_section(sname)
        cfg.set(sname, evname, '+' + add)
        
        # pass the config object to util.env_update()
        util.env_update(cfg)
        
        # verify that the variable was set to the expected value
        self.expected(exp, os.environ[evname])
        
        # raise testhelp.UnderConstructionError()
    
    # --------------------------------------------------------------------------
    def test_env_add_pre(self):
        """
        TEST: add to a predefined environment variable from [env] entry

        EXP: payload is appended to the old value
        """
        sname = 'env'
        evname = 'UTIL_TEST'
        pre_val = "one:two:three"
        add = "four:five:six"
        exp = ":".join([pre_val, add])

        # make sure the target env variable is set to a known value
        os.environ[evname] = pre_val

        # create a config object with an 'env' section and a '+' option
        cfg = CrawlConfig.CrawlConfig()
        cfg.add_section(sname)
        cfg.set(sname, evname, "+" + add)
        
        # pass the config object to util.env_update()
        util.env_update(cfg)
        
        # verify that the target env variable now contains both old and added
        # values
        self.expected(exp, os.environ[evname])

        # raise testhelp.UnderConstructionError()
        
    # --------------------------------------------------------------------------
    def test_env_set_folded_none(self):
        """
        TEST: set undefined environment variable from a folded [env] entry
        unconditionally

        EXP: the value gets set
        """
        sname = 'env'
        evname = 'UTIL_TEST'
        newval = "one:\n   two:\n   three"
        exp = re.sub("\n\s*", "", newval)

        # make sure the target env variable is not defined
        if evname in os.environ:
            del os.environ[evname]

        # create a config object with an 'env' section and a non-'+' option
        cfg = CrawlConfig.CrawlConfig()
        cfg.add_section(sname)
        cfg.set(sname, evname, newval)
        
        # pass the config object to util.env_update()
        util.env_update(cfg)
        
        # verify that the variable was set to the expected value
        self.expected(exp, os.environ[evname])
    
    # --------------------------------------------------------------------------
    def test_env_set_pre_folded(self):
        """
        TEST: set predefined environment variable from a folded [env] entry
        unconditionally

        EXP: the old value gets overwritten
        """
        sname = 'env'
        evname = 'UTIL_TEST'
        pre_val = "one:two:three"
        add = "four:\n   five:\n   six"
        exp = re.sub("\n\s*", "", add)

        # make sure the target env variable is set to a known value
        os.environ[evname] = pre_val

        # create a config object with an 'env' section and a non-'+' option
        cfg = CrawlConfig.CrawlConfig()
        cfg.add_section(sname)
        cfg.set(sname, evname, add)
        
        # pass the config object to util.env_update()
        util.env_update(cfg)
        
        # verify that the target env variable now contains the new value and
        # the old value is gone
        self.expected(exp, os.environ[evname])
        self.assertTrue(pre_val not in os.environ[evname],
                        "The old value should be gone but still seems to be " +
                        " hanging around")
        
    # --------------------------------------------------------------------------
    def test_env_set_none(self):
        """
        TEST: set undefined environment variable from [env] entry
        unconditionally

        EXP: the value gets set
        """
        sname = 'env'
        evname = 'UTIL_TEST'
        exp = "newval"

        # make sure the target env variable is not defined
        if evname in os.environ:
            del os.environ[evname]

        # create a config object with an 'env' section and a non-'+' option
        cfg = CrawlConfig.CrawlConfig()
        cfg.add_section(sname)
        cfg.set(sname, evname, exp)
        
        # pass the config object to util.env_update()
        util.env_update(cfg)
        
        # verify that the variable was set to the expected value
        self.expected(exp, os.environ[evname])
        
        # raise testhelp.UnderConstructionError()
    
    # --------------------------------------------------------------------------
    def test_env_set_pre(self):
        """
        TEST: set predefined environment variable from [env] entry
        unconditionally

        EXP: the old value gets overwritten
        """
        sname = 'env'
        evname = 'UTIL_TEST'
        pre_val = "one:two:three"
        add = "four:five:six"
        exp = add

        # make sure the target env variable is set to a known value
        os.environ[evname] = pre_val

        # create a config object with an 'env' section and a non-'+' option
        cfg = CrawlConfig.CrawlConfig()
        cfg.add_section(sname)
        cfg.set(sname, evname, add)
        
        # pass the config object to util.env_update()
        util.env_update(cfg)
        
        # verify that the target env variable now contains the new value and
        # the old value is gone
        self.expected(exp, os.environ[evname])
        self.assertTrue(pre_val not in os.environ[evname],
                        "The old value should be gone but still seems to be " +
                        " hanging around")

    # -------------------------------------------------------------------------
    def test_epoch(self):
        self.expected(1388638799, util.epoch("2014.0101 23:59:59"))
        self.expected(1388638799, util.epoch("2014.0101.23.59.59"))
        self.expected(1388638740, util.epoch("2014.0101 23:59"))
        self.expected(1388638740, util.epoch("2014.0101.23.59"))
        self.expected(1388635200, util.epoch("2014.0101 23"))
        self.expected(1388635200, util.epoch("2014.0101.23"))
        self.expected(1388552400, util.epoch("2014.0101"))
        self.expected(1388552399, util.epoch("1388552399"))
        
    # -------------------------------------------------------------------------
    def test_hostname_default(self):
        """
        Calling util.hostname() with no argument should get the short hostname
        """
        hn = util.hostname()
        self.assertFalse('.' in hn,
                         "Short hostname expected but got '%s'" % hn)

    # -------------------------------------------------------------------------
    def test_hostname_long(self):
        """
        Calling util.hostname(long=True) or util.hostname(True) should get the
        long hostanme
        """
        hn = util.hostname(long=True)
        self.assertTrue('.' in hn,
                        "Expected long hostname but got '%s'" % hn)
        hn = util.hostname(True)
        self.assertTrue('.' in hn,
                        "Expected long hostname but got '%s'" % hn)
        
    # -------------------------------------------------------------------------
    def test_hostname_short(self):
        """
        Calling util.hostname(long=False) or util.hostname(False) should get
        the short hostname
        """
        hn = util.hostname(long=False)
        self.assertFalse('.' in hn,
                         "Expected short hostname but got '%s'" % hn)
        hn = util.hostname(False)
        self.assertFalse('.' in hn,
                         "Expected short hostname but got '%s'" % hn)
        
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

    # # -------------------------------------------------------------------------
    # def test_log_default(self):
    #     """
    #     If util.log() is called with no logger already instantiated, 
    #     """
    #     # reset any logger already initialized
    #     util.get_logger(reset=True, soft=True)
    # 
    #     # now attempt to log a message to the default file
    #     msg = "This is a test log message %s"
    #     arg = "with a format specifier"
    #     if 0 == os.getuid():
    #         exp_logfile = "/var/log/crawl.log"
    #     else:
    #         exp_logfile = "/tmp/crawl.log"
    #     exp = (util.my_name() +
    #            "(%s:%d): " % (sys._getframe().f_code.co_filename,
    #                           sys._getframe().f_lineno + 2) +
    #            msg % arg)
    #     util.log(msg, arg)
    #     result = util.contents(exp_logfile)
    #     self.assertTrue(exp in result,
    #                     "Expected '%s' in %s" %
    #                     (exp, util.line_quote(result)))
    #         
    #     
    # # -------------------------------------------------------------------------
    # def test_log_simple(self):
    #     """
    #     Tests for routine util.log():
    #      - simple string in first argument
    #      - 1 % formatter in first arg
    #      - multiple % formatters in first arg
    #      - too many % formatters for args
    #      - too many args for % formatters
    #     """
    #     fpath = "%s/%s.log" % (self.testdir, util.my_name())
    #     util.get_logger(reset=True, soft=True)
    #     log = util.get_logger(cmdline=fpath)
    # 
    #     # simple string in first arg
    #     exp = util.my_name() + ": " + "This is a simple string"
    #     util.log(exp)
    #     result = util.contents(fpath)
    #     self.assertTrue(exp in result,
    #                     "Expected '%s' in %s" %
    #                     (exp, util.line_quote(result)))
    #                     
    # # -------------------------------------------------------------------------
    # def test_log_onefmt(self):
    #     # """
    #     # Tests for routine util.log():
    #     #  - simple string in first argument
    #     #  - 1 % formatter in first arg
    #     #  - multiple % formatters in first arg
    #     #  - too many % formatters for args
    #     #  - too many args for % formatters
    #     # """
    #     fpath = "%s/%s.log" % (self.testdir, util.my_name())
    #     util.get_logger(reset=True, soft=True)
    #     log = util.get_logger(cmdline=fpath)
    # 
    #     # 1 % formatter in first arg
    #     a1 = "This has a formatter and one argument: %s"
    #     a2 = "did that work?"
    #     exp = (util.my_name() +
    #            "(%s:%d): " % (util.filename(), util.lineno()+2) +
    #            a1 % a2)
    #     util.log(a1, a2)
    #     result = util.contents(fpath)
    #     self.assertTrue(exp in result,
    #                     "Expected '%s' in %s" %
    #                     (exp, util.line_quote(result)))
    # 
    # # -------------------------------------------------------------------------
    # def test_log_multfmt(self):
    #     # """
    #     # Tests for routine util.log():
    #     #  - simple string in first argument
    #     #  - 1 % formatter in first arg
    #     #  - multiple % formatters in first arg
    #     #  - too many % formatters for args
    #     #  - too many args for % formatters
    #     # """
    #     fpath = "%s/%s.log" % (self.testdir, util.my_name())
    #     util.get_logger(reset=True, soft=True)
    #     log = util.get_logger(cmdline=fpath)
    # 
    #     # multiple % formatters in first arg
    #     a1 = "Here's a string: '%s'; here's an int: %d; here's a float: %f"
    #     a2 = "zebedee"
    #     a3 = 94
    #     a4 = 23.12348293402
    #     exp = (util.my_name() +
    #            "(%s:%d): " % (util.filename(), util.lineno()+2) +
    #            a1 % (a2, a3, a4))
    #     util.log(a1, a2, a3, a4)
    #     result = util.contents(fpath)
    #     self.assertTrue(exp in result,
    #                     "Expected '%s' in %s" %
    #                     (exp, util.line_quote(result)))
    # 
    # # -------------------------------------------------------------------------
    # def test_log_toomany_fmt(self):
    #     # """
    #     # Tests for routine util.log():
    #     #  - simple string in first argument
    #     #  - 1 % formatter in first arg
    #     #  - multiple % formatters in first arg
    #     #  - too many % formatters for args
    #     #  - too many args for % formatters
    #     # """
    #     fpath = "%s/%s.log" % (self.testdir, util.my_name())
    #     util.get_logger(reset=True, soft=True)
    #     log = util.get_logger(cmdline=fpath)
    # 
    #     # this allows exceptions thrown from inside the logging handler to
    #     # propagate up so we can catch it.
    #     log.handlers[0].handleError = logErr
    #     
    #     # multiple % formatters in first arg
    #     a1 = "Here's a string: '%s'; here's an int: %d; here's a float: %f; %g"
    #     a2 = "zebedee"
    #     a3 = 94
    #     a4 = 23.12348293402
    #     exp = util.my_name() + ": " + a1 % (a2, a3, a4, 17.9)
    #     try:
    #         util.log(a1, a2, a3, a4)
    #         self.fail("Expected exception not thrown")
    #     except TypeError,e:
    #         self.assertEqual("not enough arguments for format string", str(e),
    #                          "Wrong TypeError thrown")
    # 
    #     result = util.contents(fpath)
    #     self.assertFalse(exp in result,
    #                     "Expected '%s' in %s" %
    #                     (exp, util.line_quote(result)))
    # 
    # 
    #     
    # # -------------------------------------------------------------------------
    # def test_log_toomany_args(self):
    #     # """
    #     # Tests for routine util.log():
    #     #  - simple string in first argument
    #     #  - 1 % formatter in first arg
    #     #  - multiple % formatters in first arg
    #     #  - too many % formatters for args
    #     #  - too many args for % formatters
    #     # """
    #     fpath = "%s/%s.log" % (self.testdir, util.my_name())
    #     util.get_logger(reset=True, soft=True)
    #     log = util.get_logger(cmdline=fpath)
    # 
    #     # this allows exceptions thrown from inside the logging handler to
    #     # propagate up so we can catch it.
    #     log.handlers[0].handleError = logErr
    #     
    #     # multiple % formatters in first arg
    #     a1 = "Here's a string: '%s'; here's an int: %d; here's a float: %f"
    #     a2 = "zebedee"
    #     a3 = 94
    #     a4 = 23.12348293402
    #     a5 = "friddle"
    #     exp = (util.my_name() + ": " + a1 % (a2, a3, a4))
    #     try:
    #         util.log(a1, a2, a3, a4, a5)
    #         self.fail("Expected exception not thrown")
    #     except TypeError, e:
    #         exc = "not all arguments converted during string formatting"
    #         self.assertEqual(exc, str(e),
    #                          "Expected '%s', got '%s'" % (exc, str(e)))
    #     
    #     result = util.contents(fpath)
    #     self.assertFalse(exp in result,
    #                      "Expected '%s' in %s" %
    #                      (exp, util.line_quote(result)))
        
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

    # -------------------------------------------------------------------------
    def test_rgxin(self):
        """
        Routine rgxin(needle, haystack) is analogous to the Python expression
        "needle in haystack" with needle being a regexp.
        """
        rgx = "a\(?b\)?c"
        fstring = "The quick brown fox jumps over the lazy dog"
        tstring1 = "Now we know our abc's"
        tstring2 = "With parens: a(b)c"
        self.assertTrue(util.rgxin(rgx, tstring1),
                        "'%s' should match '%s'" % (rgx, tstring1))
        self.assertTrue(util.rgxin(rgx, tstring2),
                        "'%s' should match '%s'" % (rgx, tstring2))
        self.assertFalse(util.rgxin(rgx, fstring),
                        "'%s' should NOT match '%s'" % (rgx, fstring))
                        
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    toolframe.ez_launch(test='UtilTest',
                        logfile=testhelp.testlog(__name__))
