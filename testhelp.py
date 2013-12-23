#!/usr/bin/env python
"""
Extensions to python's standard unittest module

 - Add command line options in testhelp.main().

 - Test setup/teardown support for creating and removing a directory to hold
   test data.

 - Test logging (LoggingTestSuite).

 - Test selection from the command line.

 - HelpedTestCase:
    > self.cd() into testdir creates testdir if it does not exist
    > self.expected() compares an expected and actual value and reports diffs
    
"""
import CrawlConfig
import logging, logging.handlers
import os
import pdb
import pexpect
import shutil
import socket
import sys
import StringIO
import toolframe
import unittest
import util

from optparse import *

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
    if args == None:
        args = sys.argv
    p = OptionParser()
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
    
    if o.verbose:
        volume = 2
    elif o.quiet:
        volume = 0
    else:
        volume = 1

    # print sys.modules.keys()
    # print sys.modules['__main__']

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
    # print("all_tests(%s, %s)" % (name, filter))
    testclasses = []
    cases = []
    if filter == None:
        filter = 'Test'
    # print("all_tests(%s, %s)" % (name, filter))
    # print dir(sys.modules[name])
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
            if sfunc == None:
                cases.append(['%s.%s' % (c, case), None])
            else:
                cases.append(['%s.%s' % (c, case), skip])

    return cases

# -----------------------------------------------------------------------------
def expectVSgot(expected, got):
    """
    Compare an expected value against an actual value and report the results
    """
    try:
        assert(expected == got)
    except AssertionError, e:
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
        kf_flag = False
        rval = kf_flag

    if value != None:
        kf_flag = value

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
def module_test_setup(dir):
    """
    Set up for testing by deleting and recreating any directories named in dir.
    The argument may be a string (a single directory) or a list (one or more
    directories).
    """
    if type(dir) == str:
        reset_directory(dir)
    elif type(dir) == list:
        for dirname in dir:
            reset_directory(dirname)
            
# -----------------------------------------------------------------------------
def module_test_teardown(dir):
    """
    Clean up after testing by removing any directories named in dir. The
    argument may be a string or a list.
    """
    if not keepfiles():
        # close and release any open logging files
        logger = util.get_logger(reset=True, soft=True)
        if type(dir) == str:
            reset_directory(dir, make=False)
        elif type(dir) == list:
            for dirname in dir:
                reset_directory(dirname, make=False)
    
# -----------------------------------------------------------------------------
def reset_directory(dirpath, make=True):
    """
    If dirpath names a directory, remove it and optionally recreate it. This is
    used by module_test_teardown().
    """
    if os.path.isdir(dirpath):
        shutil.rmtree(dirpath)
    if make and not os.path.exists(dirpath):
        os.makedirs(dirpath)
        
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
    # return str(obj).split()[0]

# -----------------------------------------------------------------------------
def run_tests(a, final, testlist, volume, logfile=None, module=None):
    """
    Run the tests.
    """
    if module == None:
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
    
    # ------------------------------------------------------------------------
    def write_cfg_file(self, fname, cfgdict, includee=False):
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
            
        if 'crawler' not in cfg.sections() and not includee:
            raise StandardError("section 'crawler' missing from test config file")
        
        f = open(fname, 'w')
        cfg.write(f)
        f.close()

# -----------------------------------------------------------------------------
class Chdir(object):
    """
    This class allows for doing the following:

        with Chdir('/some/other/directory'):
            assert(in '/some/other/directory')
            do_stuff()
        assert(back at our starting point)

    No matter what happens in do_stuff(), we're guaranteed that at the assert,
    we'll be back in the directory we started from.
    """
    # ------------------------------------------------------------------------
    def __init__(self, target):
        self.start = os.getcwd()
        self.target = target
    # ------------------------------------------------------------------------
    def __enter__(self):
        os.chdir(self.target)
        return self.target
    # ------------------------------------------------------------------------
    def __exit__(self, type, value, traceback):
        os.chdir(self.start)

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

    if value != None:
        show_output = value

    return rval

# -----------------------------------------------------------------------------
def skip_check(skipfunc):
    """
    If there's a skip function for this test, skip it
    """
    if skipfunc == None:
        return False
    func = getattr(sys.modules['__main__'], skipfunc)
    rval = func()
    if rval:
        print "skipping %s" % skipfunc.replace('skip_', 'test_')
    return rval

# -----------------------------------------------------------------------------
def touch(pathname):
    """
    Like touch(1). This should be in util.py.
    """
    open(pathname, 'a').close()

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
class TesthelpTest(unittest.TestCase):
    """
    Tests for testhelp code.
    """
    # -------------------------------------------------------------------------
    def test_all_tests(self):
        """
        The all_test() routine selects tests from a list based on the filter
        (its second argument). We sort the lists to ensure they'll match as
        long as they have the same contents.
        """
        all = ['TesthelpTest.test_all_tests',
               'TesthelpTest.test_list_tests',
               'TesthelpTest.test_expected_vs_got'].sort()
        l = all_tests('__main__').sort()
        expectVSgot(all, l)
        l = all_tests('__main__', 'no such tests')
        expectVSgot([], l)
        l = all_tests('__main__', 'helpTest').sort()
        expectVSgot(all, l)

    # -------------------------------------------------------------------------
    def test_list_tests(self):
        """
        Method redirected_list_test() tests the list_tests() routine. Depending
        on its arguments, it should select different entries from the list of
        tests in tlist. Since list_tests() writes directly to stdout, we have
        to redirect stdout to a StringIO object momentarily.
        """
        tlist = ['one', 'two', 'three', 'four', 'five']
        self.redirected_list_test([],
                                  '',
                                  tlist,
                                  "one\ntwo\nthree\nfour\nfive\n")
        self.redirected_list_test(['', 'o'],
                                  '',
                                  tlist,
                                  "one\ntwo\nfour\n")
        self.redirected_list_test(['', 'e'],
                                  '',
                                  tlist,
                                  "one\nthree\nfive\n")

    # -------------------------------------------------------------------------
    def redirected_list_test(self, args, final, testlist, expected):
        """
        Handle one of the list_tests() tests from the routine above.
        """
        s = StringIO.StringIO()
        save_stdout = sys.stdout
        sys.stdout = s
        list_tests(args, final, testlist)
        sys.stdout = save_stdout

        r = s.getvalue()
        s.close()
        self.assertEqual(expected, r,
                         "Expected '%s', got '%s'" %
                         (expected, r))

    # -------------------------------------------------------------------------
    def test_expected_vs_got(self):
        """
        Test expected_vs_got(). If expected and got match, the output should be
        empty. If they don't match, this should be reported. Again, we have to
        redirect stdout.
        """
        self.redirected_evg('', '', '')
        self.redirected_evg('one', 'two',
                            "EXPECTED: 'one'\n" +
                            "GOT:      'two'\n")

    # -------------------------------------------------------------------------
    def redirected_evg(self, exp, got, expected):
        """
        Redirect stdout, run expected_vs_got() and return the result
        """
        s = StringIO.StringIO()
        save_stdout = sys.stdout
        sys.stdout = s
        try:
            expectVSgot(exp, got)
        except AssertionError:
            pass
        r = s.getvalue()
        s.close()
        sys.stdout = save_stdout

        try:
            assert(r.startswith(expected))
        except AssertionError:
            print "expected: '''\n%s'''" % expected
            print "got:      '''\n%s'''" % r
        
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
    
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    toolframe.ez_launch(test='TesthelpTest',
                        logfile='crawl_test.log')
