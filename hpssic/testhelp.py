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

tlogger = None


# -----------------------------------------------------------------------------
def main(args=None, filter=None, logfile=None):
    """
    If testhelp is imported, you can call testhelp.main() rather than
    unittest.main() and get the following added functionality:

    --keep/-k on command line says to keep files generated by testing for
    subsequent review.

    --list/-l doesn't run any tests but prints a list of them.

    --quiet/-q minimizes output.

    --to/-t stops at the indicated test.

    --verbose/-v increases output.
    """
    if args is None:
        args = sys.argv
    p = optparse.OptionParser()
    p.add_option('-a', '--args',
                 action='store', default='', dest='testargs',
                 help='args for test routines')
    p.add_option('-k', '--keep',
                 action='store_true', default=False, dest='keep',
                 help='keep test files')
    p.add_option('-l', '--list',
                 action='store_true', default=False, dest='list',
                 help='list tests')
    p.add_option('-q', '--quiet',
                 action='store_true', default=False, dest='quiet',
                 help='quieter')
    p.add_option('-t', '--to',
                 action='store', default='', dest='final',
                 help='run all tests up to this one')
    p.add_option('-v', '--verbose',
                 action='store_true', default=False, dest='verbose',
                 help='louder')
    (o, a) = p.parse_args(args)

    keepfiles(o.keep)
    testargs(o.testargs)

    if o.verbose:
        volume = 2
    elif o.quiet:
        volume = 0
    else:
        volume = 1

    testlist = all_tests('__main__', filter)
    if o.list:
        list_tests(a, o.final, testlist)
        o.keep = True
    else:
        run_tests(a, o.final, testlist, volume, logfile)

    return o.keep


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
def db_config(tdir, tname, cfg_d=None):
    """
    Deprecated but still used in test_Checkable and test_Dimension
    """
    cfname = '%s/%s.cfg' % (tdir, tname)
    if cfg_d is None:
        cfg_d = {'dbi-crawler': {'dbtype': 'sqlite',
                                 'dbname': '%s/test.db' % tdir,
                                 'tbl_prefix': 'test'},
                 'crawler': {'logpath': '%s/%s.log' % (tdir, tname)},
                 'cv': {'fire': 'no'}
                 }
    cfgfile(cfname, cfg_d)
    os.environ['CRAWL_CONF'] = cfname
    CrawlConfig.get_config(reset=True, soft=True)


# -----------------------------------------------------------------------------
def cfgfile(filename, data):
    """
    Turn a dict into a configuration file
    Deprecated, used in db_config()
    """
    cfg = cfgobj(data)
    f = open(filename, 'w')
    cfg.write(f)
    f.close()


# -----------------------------------------------------------------------------
def cfgobj(data):
    """
    Turn a dict into a CrawlConfig object -- this is what CrawlConfig.dictor()
    does, so this can be deprecated.
    Deprecated, used in cfgfile()
    """
    rval = CrawlConfig.CrawlConfig()
    for section in data:
        rval.add_section(section)
        for option in data[section]:
            rval.set(section, option, data[section][option])
    if 'crawler' not in data:
        rval.add_section('crawler')
        rval.set('crawler', 'verbose', 'false')
    return rval


# -----------------------------------------------------------------------------
def expectVSgot(expected, got):
    """
    Compare an expected value against an actual value and report the results
    """
    try:
        assert(expected == got)
    except AssertionError as e:
        if type(expected) == list:
            if 5 < len(expected):
                for i in range(0, len(expected)):
                    try:
                        if expected[i] != got[i]:
                            print "EXPECTED: '%s'" % expected[i]
                            print "GOT:      '%s'" % got[i]
                    except IndexError:
                        print "EXPECTED: '%s'" % expected[i]
                        print "GOT:      None"
            else:
                print "EXPECTED '%s'" % expected
                print "GOT      '%s'" % got
            raise e
        elif type(expected) == str:
            print "EXPECTED: '%s'" % expected
            print "GOT:      '%s'" % got
            raise e


# -----------------------------------------------------------------------------
def keepfiles(value=None):
    """
    Return value of global value kf_flag. Optionally set it if value
    is specified. If it is not set, the default return value is False.
    """
    global kf_flag
    try:
        rval = kf_flag
    except:
        if os.getenv("KEEPFILES"):
            kf_flag = True
        else:
            kf_flag = False
        rval = kf_flag

    if value is not None:
        kf_flag = value

    return rval


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
# def testlog(mname):
#     return "%s/crawl_test.log" % os.path.dirname(sys.modules[mname].__file__)


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
def reset_directory(dirpath, make=True, force=False):
    """
    If dirpath names a directory, remove it and optionally recreate it. This is
    used by module_test_teardown().
    """
    try:
        if not keepfiles() or force:
            CrawlConfig.log(close=True)
            if os.path.isdir(dirpath):
                shutil.rmtree(dirpath)
            if make and not os.path.exists(dirpath):
                os.makedirs(dirpath)
    except OSError:
        print glob.glob("%s/*" % dirpath)


# -----------------------------------------------------------------------------
def test_name(obj=None):
    """
    Return the caller's function name (with an optional class prefix). This is
    used in reporting test results into the log file.
    """
    z = str(obj).split()
    z.reverse()
    rval = z[0].strip('()') + '.' + z[1]
    rval = rval.replace('__main__.', '')
    return rval


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
def run_tests(a, final, testlist, volume, logfile=None, module=None):
    """
    Run the tests.
    """
    if module is None:
        module = sys.modules['__main__']
    if len(a) == 1:
        suite = LoggingTestSuite(logfile=logfile)
        for (case, skip) in testlist:
            if skip_check(skip):
                continue
            s = unittest.TestLoader().loadTestsFromName(case, module)
            suite.addTests(s)
            if final != '' and final in case:
                break
    else:
        suite = LoggingTestSuite(logfile=logfile)
        for arg in a[1:]:
            for (case, skip) in testlist:
                if skip_check(skip):
                    continue
                if arg in case:
                    s = unittest.TestLoader().loadTestsFromName(case, module)
                    suite.addTests(s)
                if final != '' and final in case:
                    break

    if 0 < len(a):
        if 'setUpModule' in dir(module):
            module.setUpModule()
        result = unittest.TextTestRunner(verbosity=volume).run(suite)
        if 'tearDownModule' in dir(module):
            module.tearDownModule()
        return(result.testsRun, len(result.errors), len(result.failures))


# -----------------------------------------------------------------------------
class LoggingTestSuite(unittest.TestSuite):
    """
    This class adds logging to the standard unittest.TestSuite.
    """
    # -------------------------------------------------------------------------
    def __init__(self, tests=(), logfile=None):
        """
        Without a logger, we behave pretty much like the standard TestSuite.
        """
        super(LoggingTestSuite, self).__init__(tests)
        self._logger = None
        if None != logfile:
            self._logger = util.setup_logging(logfile, 'TestSuite')

    # -------------------------------------------------------------------------
    def run(self, result):
        """
        Run the tests and log the results.
        """
        errs = 0
        fails = 0
        for test in self._tests:
            if result.shouldStop:
                break
            if None != self._logger:
                test(result)
                if fails < len(result.failures):
                    self._logger.info('%-50s >>> FAILED' % test_name(test))
                    fails = len(result.failures)
                elif errs < len(result.errors):
                    self._logger.info('%-50s >>> ERROR' % test_name(test))
                    errs = len(result.errors)
                else:
                    self._logger.info('%-45s PASSED' % test_name(test))
            else:
                test(result)
        return result


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
        if all([not isinstance(cfgdict, dict),
                not isinstance(cfgdict, CrawlConfig.CrawlConfig)]):

            raise StandardError("cfgdict has invalid type %s" % type(cfgdict))

        elif isinstance(cfgdict, dict):
            cfg = CrawlConfig.CrawlConfig.dictor(cfgdict)

        elif isinstance(cfgdict, CrawlConfig.CrawlConfig):
            cfg = cfgdict

        if 'crawler' not in cfg.sections() and not includee:
            raise StandardError("section '%s' missing from test config file" %
                                "crawler")

        f = open(fname, 'w')
        cfg.write(f)
        f.close()


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


# -----------------------------------------------------------------------------
def skip_check(skipfunc):
    """
    If there's a skip function for this test, skip it
    """
    if skipfunc is None:
        return False
    func = getattr(sys.modules['__main__'], skipfunc)
    rval = func()
    if rval:
        print "skipping %s" % skipfunc.replace('skip_', 'test_')
    return rval


# -----------------------------------------------------------------------------
def write_file(filename, mode=0644, content=None):
    """
    Write a file, optionally setting its permission bits. This should be in
    util.py.
    """
    f = open(filename, 'w')
    if type(content) == str:
        f.write(content)
    elif type(content) == list:
        f.writelines([x.rstrip() + '\n' for x in content])
    else:
        raise StandardError("content is not of a suitable type (%s)"
                            % type(content))
    f.close()
    os.chmod(filename, mode)


# -----------------------------------------------------------------------------
class UnderConstructionError(Exception):
    """
    This error class can be used to cause tests that are under construction to
    fail until they are completed.
    """
    def __init__(self, value=""):
        """
        The user can specify a value but the default will be 'under
        construction'
        """
        if value == '':
            self.value = 'under construction'
        else:
            self.value = value

    def __str__(self):
        """
        Human readable representaton of the error
        """
        return repr(self.value)
