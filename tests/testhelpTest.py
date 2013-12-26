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
import sys
import StringIO
import testhelp
import toolframe
import unittest

tlogger = None

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
        l = testhelp.all_tests('__main__').sort()
        testhelp.expectVSgot(all, l)
        l = testhelp.all_tests('__main__', 'no such tests')
        testhelp.expectVSgot([], l)
        l = testhelp.all_tests('__main__', 'helpTest').sort()
        testhelp.expectVSgot(all, l)

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
        testhelp.list_tests(args, final, testlist)
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

    # # -------------------------------------------------------------------------
    # def test_HelpedTestCase(self):
    #     q = testhelp.HelpedTestCase()
    #     for attr in ['expected', 'expected_in', 'write_cfg_file']:
    #         self.assertTrue(hasattr(q, attr),
    #                         "Expected %s to have attr %s" % (q, attr))

    # -------------------------------------------------------------------------
    def redirected_evg(self, exp, got, expected):
        """
        Redirect stdout, run expected_vs_got() and return the result
        """
        s = StringIO.StringIO()
        save_stdout = sys.stdout
        sys.stdout = s
        try:
            testhelp.expectVSgot(exp, got)
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
if __name__ == '__main__':
    toolframe.ez_launch(test='TesthelpTest',
                        logfile=testhelp.testlog(__name__))
