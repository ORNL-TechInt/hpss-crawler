#!/usr/bin/env python
"""
Tests for hpss.py
"""
import hpss
import testhelp
import toolframe
import traceback as tb
import util

# -----------------------------------------------------------------------------
def setUpModule():
    """
    Set up for testing
    """
    testhelp.module_test_setup(hpssTest.testdir)
    
# -----------------------------------------------------------------------------
def tearDownModule():
    """
    Clean up after testing
    """
    testhelp.module_test_teardown(hpssTest.testdir)

# -----------------------------------------------------------------------------
class hpssTest(testhelp.HelpedTestCase):
    """
    Tests for the hpss.HSI class
    """
    testdir = testhelp.testdata(__name__)
    hdir = "/home/tpb/hic_test"
    stem = "hashable"
    plist = ["%s/%s%d" % (hdir, stem, x) for x in range(1,4)]
    paths = " ".join(plist)
    
    # -------------------------------------------------------------------------
    def test_ctor_attrs(self):
        """
        Verify that a newly created HSI object has the right attributes
        """
        a = hpss.HSI(connect=False)

        for attr in ['prompt', 'verbose', 'xobj', 'lscos', 'quit', 'connect',
                     'chdir', 'lsP', 'hashcreate', 'hashlist', 'hashverify',
                     'hashdelete']:
            self.assertTrue(hasattr(a, attr),
                            "Expected %s to have attribute '%s'" %
                            (a, attr))
        
    # -------------------------------------------------------------------------
    def test_chdir_noarg(self):
        """
        Change dir in HPSS to a non directory
        """
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        try:
            result = h.chdir()
            self.fail("Expected an exception, got nothing")
        except TypeError, e:
            self.assertTrue("chdir() takes exactly 2 arguments" in str(e),
                            "Got the wrong TypeError: %s" %
                            util.line_quote(tb.format_exc()))
        except AssertionError:
            raise
        
    # -------------------------------------------------------------------------
    def test_chdir_notdir(self):
        """
        Change dir in HPSS to a non directory
        """
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        result = h.chdir("hic_test/crawler.tar")
        h.quit()

        exp = "Not a directory"
        self.assertTrue(exp in result,
                        "Expected '%s' in %s" %
                        (exp, util.line_quote(result)))

    # -------------------------------------------------------------------------
    def test_chdir_ok(self):
        """
        Successful change dir in HPSS
        """
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        result = h.chdir("hic_test")
        h.quit()

        exp = "/home/tpb/hic_test"
        self.expected_in(exp, result)

    # -------------------------------------------------------------------------
    def test_chdir_perm(self):
        """
        Change dir in HPSS to a non-accessible directory
        """
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        result = h.chdir("cli_test/unreadable")
        h.quit()

        exp = "hpss_Chdir: Access denied"
        self.assertTrue(exp in result,
                        "Expected '%s' in %s" %
                        (exp, util.line_quote(result)))

    # -------------------------------------------------------------------------
    def test_hashcreate_argbad(self):
        """
        If hashcreate gets an invalid argument (not a str or list), it should
        throw an exception
        """
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        try:
            result = h.hashcreate(32)
            self.fail("Expected an exception, got nothing")
        except hpss.HSIerror, e:
            self.assertTrue("hashcreate: Invalid argument" in str(e),
                            "Got the wrong HSIerror: %s" %
                            util.line_quote(tb.format_exc()))
        except AssertionError:
            raise
        finally:
            h.quit()

    # -------------------------------------------------------------------------
    def test_hashcreate_argnone(self):
        """
        If hashcreate gets no argument, it should throw an exception
        """
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        try:
            result = h.hashcreate()
            self.fail("Expected an exception, got nothing")
        except TypeError, e:
            self.assertTrue("hashcreate() takes exactly 2 arguments" in str(e),
                            "Got the wrong TypeError: %s" %
                            util.line_quote(tb.format_exc()))
        except AssertionError:
            raise
        finally:
            h.quit()

    # -------------------------------------------------------------------------
    def test_hashcreate_ok_glob(self):
        """
        Issue "hashcreate" in hsi with a wildcard argument, return results
        """
        glop = "%s/%s*" % (self.hdir, self.stem)
        
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        result = h.hashcreate(glop)
        h.quit()
        self.expected_in("hashcreate", result)
        for path in self.paths.split():
            exp = "\(?md5\)? %s" % path
            self.expected_in(exp, result)

    # -------------------------------------------------------------------------
    def test_hashcreate_ok_list(self):
        """
        Issue "hashcreate" in hsi with a list argument, return results
        """
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        result = h.hashcreate(self.plist)
        h.quit()
        self.expected_in("hashcreate", result)
        for path in self.plist:
            exp = "\(?md5\)? %s" % path
            self.expected_in(exp, result)

    # -------------------------------------------------------------------------
    def test_hashcreate_ok_str(self):
        """
        Issue "hashcreate" in hsi with a string argument, return results
        """
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        result = h.hashcreate(self.paths)
        h.quit()
        self.expected_in("hashcreate", result)
        for path in self.paths.split():
            exp = "\(?md5\)? %s" % path
            self.expected_in(exp, result)

    # -------------------------------------------------------------------------
    def test_hashdelete_argbad(self):
        """
        If hashdelete gets an invalid argument (not a str or list), it should
        throw an exception
        """
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        try:
            result = h.hashdelete(32)
            self.fail("Expected an exception, got nothing")
        except hpss.HSIerror, e:
            self.expected_in("hashdelete: Invalid argument", str(e))
        except AssertionError:
            raise
        finally:
            h.quit()

    # -------------------------------------------------------------------------
    def test_hashdelete_argnone(self):
        """
        If hashdelete gets no argument, it should throw an exception
        """
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        try:
            result = h.hashdelete()
            self.fail("Expected an exception, got nothing")
        except TypeError, e:
            self.expected_in("hashdelete\(\) takes exactly 2 arguments",
                             str(e))
        except AssertionError:
            raise
        finally:
            h.quit()

    # -------------------------------------------------------------------------
    def test_hashdelete_ok_glob(self):
        """
        If hashdelete gets a wildcard argument, it should work
        """
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        # make sure the hashables all have a checksum stored
        x = h.hashlist(self.plist)
        for path in self.plist:
            if util.rgxin("\(?none\)?  %s" % path, x):
                h.hashcreate(path)

        # run hashdelete on the glob path
        result = h.hashdelete("%s/hash*" % self.hdir)
        h.quit()

        # verify the results
        self.expected_in("hashdelete", result)
        for path in self.plist:
            self.expected_in("hash deleted: \(?md5\)? %s" % path, result)

        exp = "\(?none\)?  %s/hashnot" % self.hdir
        self.assertTrue(exp not in result,
                        "'%s' not expected in %s" % (exp,
                                                     util.line_quote(result)))

    # -------------------------------------------------------------------------
    def test_hashdelete_ok_list(self):
        """
        If hashdelete get a list argument, it should work
        """
        plist = self.plist + [self.hdir + "/hashnot"]
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        # make sure the hashables all have a checksum stored
        x = h.hashlist(self.plist)
        for path in self.plist:
            if util.rgxin("\(?none\)?  %s" % path, x):
                h.hashcreate(path)

        # run hashdelete on the list
        result = h.hashdelete(plist)
        h.quit()

        # verify the results
        self.expected_in("hashdelete", result)
        for path in self.plist:
            self.expected_in("hash deleted: md5 %s" % path, result)
        exp = "\(?none\)?  %s/hashnot" % self.hdir
        self.assertTrue(exp not in result,
                        "'%s' not expected in %s" % (exp,
                                                     util.line_quote(result)))

    # -------------------------------------------------------------------------
    def test_hashdelete_ok_str(self):
        """
        If hashdelete gets a string argument, it should work
        """
        paths = self.paths + " %s/hashnot" % self.hdir
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        # make sure the hashables all have a checksum stored
        x = h.hashlist(self.plist)
        for path in self.plist:
            if util.rgxin("\(?none\)?  %s" % path, x):
                h.hashcreate(path)

        # run hashdelete on the string
        result = h.hashdelete(paths)
        h.quit()

        # verify the results
        self.expected_in("hashdelete", result)
        for path in self.paths.split():
            exp = "hash deleted: \(?md5\)? %s" % path
            self.expected_in(exp, result)
        exp = "hash deleted: \(?md5\)? %s/hashnot" % self.hdir
        self.assertFalse(util.rgxin(exp, result),
                         "'%s' not expected in %s" %
                         (exp, util.line_quote(result)))

    # -------------------------------------------------------------------------
    def test_hashlist_argbad(self):
        """
        If hashlist gets an invalid argument (not a str or list), it should
        throw an exception
        """
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        try:
            result = h.hashlist(32)
            self.fail("Expected an exception, got nothing")
        except hpss.HSIerror, e:
            self.assertTrue("hashlist: Invalid argument" in str(e),
                            "Got the wrong HSIerror: %s" %
                            util.line_quote(tb.format_exc()))
        except AssertionError:
            raise
        finally:
            h.quit()

    # -------------------------------------------------------------------------
    def test_hashlist_argnone(self):
        """
        If hashlist gets no argument, it should throw an exception
        """
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        try:
            result = h.hashlist()
            self.fail("Expected an exception, got nothing")
        except TypeError, e:
            self.assertTrue("hashlist() takes exactly 2 arguments" in str(e),
                            "Got the wrong TypeError: %s" %
                            util.line_quote(tb.format_exc()))
        except AssertionError:
            raise
        finally:
            h.quit()

    # -------------------------------------------------------------------------
    def test_hashlist_ok_glob(self):
        """
        If hashlist gets a wildcard argument, it should work
        """
        vbval = ("verbose" in testhelp.testargs())
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        # make sure the hashables all have a checksum stored
        x = h.hashlist(self.plist)
        for path in self.plist:
            if util.rgxin("\(?none\)?  %s" % path, x):
                h.hashcreate(path)

        # run the test payload
        result = h.hashlist("%s/hash*" % self.hdir)
        h.quit()
        self.expected_in("hashlist", result)
        for path in self.plist:
            exp = "\(?md5\)? %s" % path
            self.expected_in(exp, result)
        exp = "\(?none\)?  %s/hashnot" % self.hdir
        self.expected_in(exp, result)

    # -------------------------------------------------------------------------
    def test_hashlist_ok_list(self):
        """
        If hashlist get a list argument, it should work
        """
        plist = self.plist + [self.hdir + "/hashnot"]
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        # make sure the hashables all have a checksum stored
        x = h.hashlist(self.plist)
        for path in self.plist:
            if util.rgxin("\(?none\)?  %s" % path, x):
                h.hashcreate(path)

        # run the test payload
        result = h.hashlist(plist)
        h.quit()
        self.expected_in("hashlist", result)
        for path in self.plist:
            exp = "\(?md5\)? %s" % path
            self.expected_in(exp, result)
        exp = "\(?none\)?  %s/hashnot" % self.hdir
        self.expected_in(exp, result)

    # -------------------------------------------------------------------------
    def test_hashlist_ok_str(self):
        """
        If hashlist gets a string argument, it should work
        """
        paths = self.paths + " %s/hashnot" % self.hdir
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        # make sure the hashables all have a checksum stored
        x = h.hashlist(self.plist)
        for path in self.plist:
            if util.rgxin("\(?none\)?  %s" % path, x):
                h.hashcreate(path)

        # run the test payload
        result = h.hashlist(paths)
        h.quit()
        self.expected_in("hashlist", result)
        for path in self.paths.split():
            exp = "\(?md5\)? %s" % path
            self.expected_in(exp, result)
        exp = "\(?none\)?  %s/hashnot" % self.hdir
        self.expected_in(exp, result)

    # -------------------------------------------------------------------------
    def test_hashverify_argbad(self):
        """
        If hashverify gets an invalid argument (not a str or list), it should
        throw an exception
        """
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        try:
            result = h.hashverify(32)
            self.fail("Expected an exception, got nothing")
        except hpss.HSIerror, e:
            self.assertTrue("hashverify: Invalid argument" in str(e),
                            "Got the wrong HSIerror: %s" %
                            util.line_quote(tb.format_exc()))
        except AssertionError:
            raise
        finally:
            h.quit()

    # -------------------------------------------------------------------------
    def test_hashverify_argnone(self):
        """
        Issue "hashverify" in hsi, return results
        """
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        try:
            result = h.hashverify()
            self.fail("Expected an exception, got nothing")
        except TypeError, e:
            self.assertTrue("hashverify() takes exactly 2 arguments" in str(e),
                            "Got the wrong TypeError: %s" %
                            util.line_quote(tb.format_exc()))
        except AssertionError:
            raise
        finally:
            h.quit()

    # -------------------------------------------------------------------------
    def test_hashverify_ok_glob(self):
        """
        Issue "hashverify" in hsi on a wildcard, return results
        """
        glop = "/home/tpb/hic_test/hash*"

        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        # make sure the hashables all have a checksum stored
        x = h.hashlist(self.plist)
        for path in self.plist:
            if "\(?none\)?  %s" % path in x:
                h.hashcreate(path)

        # run hashverify on the glob path
        result = h.hashverify(glop)
        h.quit()

        # verify the results: we should see "OK" for the hashables and an error
        # for hashnot
        self.expected_in("hashverify", result)
        for path in self.plist:
            exp = "%s: \(?md5\)? OK" % path
            self.expected_in(exp, result)
        exp = "hashnot failed: no valid checksum found"
        self.expected_in(exp, result)
        
    # -------------------------------------------------------------------------
    def test_hashverify_ok_list(self):
        """
        Issue "hashverify" in hsi, return results
        """
        plist = self.plist + ["%s/hashnot" % self.hdir]

        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))

        # make sure the hashables all have a checksum stored
        x = h.hashlist(self.plist)
        for path in self.plist:
            if "\(?none\)?  %s" % path in x:
                h.hashcreate(path)

        # run hashverify on the list
        result = h.hashverify(plist)
        h.quit()

        # verify the results: we should see "OK" for the hashables and an error
        # for hashnot
        self.expected_in("hashverify", result)
        for path in self.plist:
            self.expected_in("%s: \(?md5\)? OK" % path, result)
        self.expected_in("hashnot failed: no valid checksum found",
                         result)

    # -------------------------------------------------------------------------
    def test_hashverify_ok_str(self):
        """
        Issue "hashverify" in hsi, return results
        """
        paths = self.paths + " %s/hashnot" % self.hdir
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        result = h.hashverify(paths)
        h.quit()
        self.expected_in("hashverify", result)
        for path in self.plist:
            self.expected_in("%s: \(?md5\)? OK" % path, result)
        self.expected_in("hashnot failed: no valid checksum found",
                         result)

    # -------------------------------------------------------------------------
    def test_lscos(self):
        """
        Issue "lscos", check result
        """
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        result = h.lscos()
        h.quit()
        self.expected_in("3003 Disk Big Backups", result)
        self.expected_in("5081 Disk X-Small", result)
        self.expected_in("6001 Disk Small", result)
        self.expected_in("6054 Disk Large_T", result)
        self.expected_in("6057 Disk X-Large_T 2-Copy", result)

    # -------------------------------------------------------------------------
    def test_lsP_argbad(self):
        """
        Issue "ls -P" with non-string, non-list arg, expect exception
        """
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        try:
            result = h.lsP(19)
            self.fail("Expected an exception, got nothing")
        except hpss.HSIerror, e:
            self.assertTrue("lsP: Invalid argument" in str(e),
                            "Got the wrong HSIerror: %s" %
                            util.line_quote(tb.format_exc()))
        except AssertionError:
            raise
        finally:
            h.quit()

    # -------------------------------------------------------------------------
    def test_lsP_argnone(self):
        """
        Issue "ls -P" in /home/tpb/hic_test with no arg, validate result
        """
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        h.chdir("/home/tpb/hic_test")
        result = h.lsP()
        for path in self.plist:
            self.expected_in("FILE\s+%s" % path, result)

    # -------------------------------------------------------------------------
    def test_lsP_glob(self):
        """
        Issue "ls -P hash*" in hic_test, validate results
        """
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        h.chdir("/home/tpb/hic_test")
        result = h.lsP()
        for path in self.plist:
            self.expected_in("FILE\s+%s" % path, result)

    # -------------------------------------------------------------------------
    def test_lsP_list(self):
        """
        Issue "ls -P [foo, bar]" in hic_test, validate results
        """
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        result = h.lsP(self.plist)
        for path in self.plist:
            self.expected_in("FILE\s+%s" % path, result)

    # -------------------------------------------------------------------------
    def test_lsP_str(self):
        """
        Issue "ls -P foo bar" in hic_test, validate results
        """
        h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
        result = h.lsP(self.paths)
        for path in self.plist:
            self.expected_in("FILE\s+%s" % path, result)

    # -------------------------------------------------------------------------
    def test_unavailable(self):
        """
        If HPSS is down, the HSI constructor should throw an exception
        """
        h = hpss.HSI(connect=False, unavailable=True)
        try:
            h.connect()
            self.fail("Expected HSIerror not thrown")
        except hpss.HSIerror, e:
            self.assertTrue("HPSS Unavailable" in str(e),
                            "Got unexpected HSIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
    
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    toolframe.ez_launch(test='hpssTest',
                        logfile=testhelp.testlog(__name__))
        
