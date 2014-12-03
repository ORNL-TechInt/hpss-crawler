"""
Extensions to python's standard unittest module

 - Add command line options in testhelp.main().

 - Test setup/teardown support for creating and removing a directory to hold
   test data.

 - Test logging (LoggingTestSuite).

 - Test selection from the command line.

 - HelpedTestCase:
    > self.expected() compares an expected and actual value and reports diffs

"""
import CrawlConfig
import CrawlDBI
import glob
import optparse
import os
import pdb
import pytest
import re
import shutil
import sys
import unittest
import util
import util as U


# -----------------------------------------------------------------------------
def all_tests(name, filter=None):
    """
    Return a list of tests in the module <name>.
    Limit the list to those which contain the string <filter>.
    """
    testclasses = []
    cases = []
    if filter is None:
        filter = 'Test'
    if type(filter) == str:
        for item in dir(sys.modules[name]):
            if filter in item:
                testclasses.append(item)
    elif type(filter) == list:
        for item in dir(sys.modules[name]):
            for f in filter:
                if f in item:
                    testclasses.append(item)

    for c in testclasses:
        cobj = getattr(sys.modules[name], c)
        for case in unittest.TestLoader().getTestCaseNames(cobj):
            skip = case.replace('test_', 'skip_', 1)
            sfunc = getattr(sys.modules[name], skip, None)
            if sfunc is None:
                cases.append(['%s.%s' % (c, case), None])
            else:
                cases.append(['%s.%s' % (c, case), skip])

    return cases


# -----------------------------------------------------------------------------
def testargs(value=''):
    """
    Cache value and return it
    """
    try:
        rval = testargs._value
    except AttributeError:
        testargs._value = value
        rval = testargs._value

    if value != '':
        testargs._value = value

    return rval


# -----------------------------------------------------------------------------
def list_tests(a, final, testlist):
    """
    Print a list of tests
    """
    if len(a) <= 1:
        for c in testlist:
            print c
            if final != '' and final in c:
                break
    else:
        for arg in a[1:]:
            for c in testlist:
                if arg in c:
                    print c
                if final != '' and final in c:
                    break


# -----------------------------------------------------------------------------
def rm_cov_warn(string):
    """
    Return the input string with the coverage warning ('Coverage.py warning: no
    data was collected') removed if present.
    """
    rval = string
    covwarn = "Coverage.py.warning:.No.data.was.collected.\r?\n?"
    if re.findall(covwarn, string):
        rval = re.sub(covwarn, "", string)
    return rval


# -----------------------------------------------------------------------------
class HelpedTestCase(unittest.TestCase):
    """
    This class adds some goodies to the standard unittest.TestCase
    """
    # -------------------------------------------------------------------------
    def assertPathPresent(self, pathname, umsg=''):
        """
        Verify that a path exists or throw an assertion error
        """
        msg = (umsg or "Expected file '%s' to exist to but it does not" %
               pathname)
        self.assertTrue(os.path.exists(pathname), msg)

    # -------------------------------------------------------------------------
    def assertPathNotPresent(self, pathname, umsg=''):
        """
        Verify that a path does not exist or throw an assertion error
        """
        msg = (umsg or "Expected file '%s' to exist to but it does not" %
               pathname)
        self.assertFalse(os.path.exists(pathname), msg)

    # -------------------------------------------------------------------------
    def assertRaisesRegex(self, exception, message, func, *args, **kwargs):
        """
        A more precise version of assertRaises so we can validate the content
        of the exception thrown.
        """
        try:
            func(*args, **kwargs)
        except exception as e:
            if type(message) == str:
                self.assertTrue(re.findall(message, str(e)),
                                "\nExpected '%s', \n     got '%s'" %
                                (message, str(e)))
            elif type(message) == list:
                self.assertTrue(any(re.findall(t, str(e)) for t in message),
                                "Expected one of '%s', got '%s'" % (message,
                                                                    str(e)))
            else:
                self.fail("message must be a string or list")
        except Exception as e:
            self.fail('Unexpected exception thrown: %s %s' % (type(e), str(e)))
        else:
            self.fail('Expected exception %s not thrown' % exception)

    # -------------------------------------------------------------------------
    def assertRaisesMsg(self, exception, message, func, *args, **kwargs):
        """
        A more precise version of assertRaises so we can validate the content
        of the exception thrown.
        """
        try:
            func(*args, **kwargs)
        except exception as e:
            if type(message) == str:
                self.assertTrue(message in str(e),
                                "\nExpected '%s', \n     got '%s'" %
                                (message, str(e)))
            elif type(message) == list:
                self.assertTrue(any(t in str(e) for t in message),
                                "Expected one of '%s', got '%s'" % (message,
                                                                    str(e)))
            else:
                self.fail("message must be a string or list")
        except Exception as e:
            self.fail('Unexpected exception thrown: %s %s' % (type(e), str(e)))
        else:
            self.fail('Expected exception %s not thrown' % exception)

    # -------------------------------------------------------------------------
    def dbname(self):
        """
        Set an sqlite database name for this test object
        """
        return self.tmpdir("test.db")

    # -------------------------------------------------------------------------
    def expected(self, expval, actual):
        """
        Compare the expected value (expval) and the actual value (actual). If
        there are differences, report them.
        """
        msg = "\nExpected: "
        if type(expval) == int:
            msg += "%d"
        elif type(expval) == float:
            msg += "%g"
        else:
            msg += "'%s'"

        msg += "\n  Actual: "
        if type(actual) == int:
            msg += "%d"
        elif type(actual) == float:
            msg += "%g"
        else:
            msg += "'%s'"

        self.assertEqual(expval, actual, msg % (expval, actual))

    # -------------------------------------------------------------------------
    def expected_in(self, exprgx, actual):
        """
        If the expected regex (exprgx) does not appear in the actual value
        (actual), report the assertion failure.
        """
        msg = "\nExpected_in: "
        if type(exprgx) == int:
            msg += "%d"
            exprgx = "%d" % exprgx
        elif type(exprgx) == float:
            msg += "%g"
            exprgx = "%g" % exprgx
        else:
            msg += "'%s'"

        msg += "\n     Actual: "
        if type(actual) == int:
            msg += "%d"
        elif type(actual) == float:
            msg += "%g"
        else:
            msg += "'%s'"

        if type(actual) == list:
            if type(exprgx) == str:
                self.assertTrue(any([util.rgxin(exprgx, x) for x in actual]),
                                msg % (exprgx, actual))
            else:
                self.assertTrue(exprgx in actual, msg % (exprgx, actual))
        else:
            self.assertTrue(util.rgxin(exprgx, actual), msg % (exprgx, actual))

    # -------------------------------------------------------------------------
    def unexpected(self, expval, actual):
        """
        Compare the expected value (expval) and the actual value (actual). If
        there are no differences, fail.
        """
        msg = "\nUnexpected: "
        if type(expval) == int:
            msg += "%d"
        elif type(expval) == float:
            msg += "%g"
        else:
            msg += "'%s'"

        msg += "\n    Actual: "
        if type(actual) == int:
            msg += "%d"
        elif type(actual) == float:
            msg += "%g"
        else:
            msg += "'%s'"

        self.assertNotEqual(expval, actual, msg % (expval, actual))

    # -------------------------------------------------------------------------
    def unexpected_in(self, exprgx, actual):
        """
        If the unexpected regex (exprgx) appears in the actual value (actual),
        report the assertion failure.
        """
        msg = "\nUnexpected_in: "
        if type(exprgx) == int:
            msg += "%d"
            exprgx = "%d" % exprgx
        elif type(exprgx) == float:
            msg += "%g"
            exprgx = "%g" % exprgx
        else:
            msg += "'%s'"

        msg += "\n     Actual: "
        if type(actual) == int:
            msg += "%d"
        elif type(actual) == float:
            msg += "%g"
        else:
            msg += "'%s'"

        if type(actual) == list:
            if type(exprgx) == str:
                self.assertFalse(all([util.rgxin(exprgx, x) for x in actual]),
                                 msg % (exprgx, actual))
            else:
                self.assertFalse(exprgx in actual, msg % (exprgx, actual))
        else:
            self.assertFalse(util.rgxin(exprgx, actual),
                             msg % (exprgx, actual))

    # -------------------------------------------------------------------------
    def noop(self):
        pass

    # ------------------------------------------------------------------------
    def has_mark(self, markname):
        """
        Return True or False to indicate whether pytest.mark *markname* is
        present on the current object.
        """
        fobj = getattr(self, self._testMethodName)
        if hasattr(fobj, markname):
            rval = True
        else:
            rval = False
        return rval

    # ------------------------------------------------------------------------
    def setUp(self):
        """
        Set self.dbgfunc to either pdb.set_trace or a no-op, depending on the
        value of the --dbg option from the command line
        """
        dbgopt = pytest.config.getoption("dbg")
        if self._testMethodName in dbgopt or "all" in dbgopt:
            self.dbgfunc = pdb.set_trace
        else:
            self.dbgfunc = lambda: None

        if self.has_mark('jenkins_fail') and os.path.exists('jenkins'):
            pytest.skip('%s fails on jenkins' % self._testMethodName)

        if self.has_mark('slow') and pytest.config.getvalue('fast'):
            pytest.skip('%s is slow' % self._testMethodName)

        for skiptag in pytest.config.getvalue("skip"):
            if skiptag in self.__class__.__name__:
                pytest.skip('Skipping %s as part of %s' %
                            (self._testMethodName, self.__class__.__name__))

    # -------------------------------------------------------------------------
    @pytest.fixture(autouse=True)
    def tmpdir_setup(self, tmpdir):
        self.pytest_tmpdir = str(tmpdir)

    # ------------------------------------------------------------------------
    def tmpdir(self, base=''):
        if base:
            rval = U.pathjoin(self.pytest_tmpdir, base)
        else:
            rval = U.pathjoin(self.pytest_tmpdir)
        return rval

    # ------------------------------------------------------------------------
    def logpath(self, basename=''):
        if basename == '':
            basename = "test_default_hpss_crawl.log"
        rval = self.tmpdir(basename)
        return rval

    # -------------------------------------------------------------------------
    def plugin_dir(self):
        return self.tmpdir("plugins")

    # ------------------------------------------------------------------------
    def write_cfg_file(self, fname, cfgdict, includee=False):
        """
        Write a config file for testing. Put the 'crawler' section first.
        Complain if the 'crawler' section is not present.
        """
        if isinstance(cfgdict, dict):
            cfg = CrawlConfig.CrawlConfig.dictor(cfgdict)
        elif isinstance(cfgdict, CrawlConfig.CrawlConfig):
            cfg = cfgdict
        else:
            raise StandardError("cfgdict has invalid type %s" % type(cfgdict))

        if 'crawler' not in cfg.sections() and not includee:
            raise StandardError("section '%s' missing from test config file" %
                                "crawler")

        with open(fname, 'w') as f:
            cfg.write(f)


# -----------------------------------------------------------------------------
def show_stdout(value=None):
    """
    Return value of global value show_stdout. Optionally set it if
    value is specified. If it is not set, the default return value is False.
    """
    global show_output
    try:
        rval = show_output
    except:
        show_output = False
        rval = show_output

    if value is not None:
        show_output = value

    return rval
