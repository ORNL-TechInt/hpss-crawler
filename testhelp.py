#!/usr/bin/env python

import logging, logging.handlers
import os
import pdb
import pexpect
import socket
import sys
import unittest
import StringIO
import toolframe
import util

from optparse import *

tlogger = None

# -----------------------------------------------------------------------------
def main(args=None, filter=None, logfile=None):
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
    '''
    Return a list of tests in the module <name>.
    Limit the list to those which contain the string <filter>.
    '''
    # print("all_tests(%s, %s)" % (name, filter))
    testclasses = []
    cases = []
    if filter == None:
        filter = 'Test'
    # print("all_tests(%s, %s)" % (name, filter))
    # print dir(sys.modules[name])
    for item in dir(sys.modules[name]):
        if filter in item:
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
def into_test_dir():
    tdname = '_test.%d' % os.getpid()
    bname = os.path.basename(os.getcwd())
    if bname != tdname:
        os.mkdir(tdname)
        os.chdir(tdname)
    return tdname

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
def test_name(obj=None):
    """
    Return the caller's function name (with an optional class prefix).
    """
    z = str(obj).split()
    z.reverse()
    rval = z[0].strip('()') + '.' + z[1]
    rval = rval.replace('__main__.', '')
    return rval
    # return str(obj).split()[0]

# -----------------------------------------------------------------------------
def run_tests(a, final, testlist, volume, logfile=None, module=None):
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

# -----------------------------------------------------------------------------
class LoggingTestSuite(unittest.TestSuite):
    # -------------------------------------------------------------------------
    def __init__(self, tests=(), logfile=None):
        super(LoggingTestSuite, self).__init__(tests)
        self._logger = None
        if None != logfile:
            self._logger = util.setup_logging(logfile, 'TestSuite')
            
    # -------------------------------------------------------------------------
    def run(self, result):
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

    # -------------------------------------------------------------------------
    def expected(self, expval, actual):
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
    if skipfunc == None:
        return False
    func = getattr(sys.modules['__main__'], skipfunc)
    rval = func()
    if rval:
        print "skipping %s" % skipfunc.replace('skip_', 'test_')
    return rval

# -----------------------------------------------------------------------------
def touch(pathname):
    open(pathname, 'a').close()

# -----------------------------------------------------------------------------
def write_file(filename, mode=0644, content=None):
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
    # -------------------------------------------------------------------------
    def test_all_tests(self):
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
        self.redirected_evg('', '', '')
        self.redirected_evg('one', 'two',
                            "EXPECTED: 'one'\n" +
                            "GOT:      'two'\n")

    # -------------------------------------------------------------------------
    def redirected_evg(self, exp, got, expected):
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
    def __init__(self, value=""):
        if value == '':
            self.value = 'under construction'
        else:
            self.value = value
    def __str__(self):
        return repr(self.value)
    
# -----------------------------------------------------------------------------
global d
d = dir()
if __name__ == '__main__':
    toolframe.ez_launch(test='TesthelpTest',
                        logfile='crawl_test.log')
