#!/usr/bin/env python
"""
Tests for hpss.py
"""
import hpss
import os
import sys
import testhelp
import toolframe
import util

mself = sys.modules[__name__]
logfile = "%s/crawl_test.log" % os.path.dirname(mself.__file__)

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
    testdir = '%s/test.d' % os.path.dirname(mself.__file__)
    
    # -------------------------------------------------------------------------
    def test_ctor_attrs(self):
        """
        Verify that a newly created HSI object has the right attributes
        """
        a = hpss.HSI()

        for attr in ['prompt', 'verbose', 'xobj', 'lscos', 'quit', 'connect',
                     'chdir', 'lsP', 'hashcreate', 'hashlist', 'hashverify']:
            self.assertTrue(hasattr(a, attr),
                            "Expected %s to have attribute '%s'" %
                            (a, attr))
        
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
    
    # -------------------------------------------------------------------------
    def test_cd(self):
        """
        Changing directory in HPSS
        """
        raise testhelp.UnderConstructionError()

    # -------------------------------------------------------------------------
    def test_lsP(self):
        """
        Issue "ls -P" in hsi, return results
        """
        pass
        raise testhelp.UnderConstructionError()

    # -------------------------------------------------------------------------
    def test_hashcreate(self):
        """
        Issue "hashcreate" in hsi, return results
        """
        pass
        raise testhelp.UnderConstructionError()

    # -------------------------------------------------------------------------
    def test_hashlist(self):
        """
        Issue "hashlist" in hsi, return results
        """
        pass
        raise testhelp.UnderConstructionError()

    # -------------------------------------------------------------------------
    def test_hashverify(self):
        """
        Issue "hashverify" in hsi, return results
        """
        pass
        raise testhelp.UnderConstructionError()

# -----------------------------------------------------------------------------
if __name__ == '__main__':
    toolframe.ez_launch(test='hpssTest',
                        logfile=logfile)
        
