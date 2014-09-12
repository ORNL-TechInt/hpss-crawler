import os
from hpssic import pexpect
from hpssic import mpra
from hpssic import testhelp as th
import unittest

# -----------------------------------------------------------------------------
class Test_MPRA(th.HelpedTestCase):
    # -------------------------------------------------------------------------
    def test_mpra_which_module(self):
        """
        Make sure we're loading the right mpra module
        """
        self.assertEqual(os.path.dirname(os.path.dirname(mpra.__file__)),
                         os.path.dirname(os.path.dirname(__file__)))

    # -------------------------------------------------------------------------
    def test_mpra_which_command(self):
        """
        Make sure the mpra command exists and is executable
        """
        cmd = pexpect.which("mpra")
        if cmd is None:
            cmd = "bin/mpra"
        self.assertTrue(os.access(cmd, os.X_OK))

    # -------------------------------------------------------------------------
    def test_mpra_help(self):
        """
        Make sure 'mpra help' generates something reasonable
        """
        mpra = pexpect.which("mpra")
        if mpra is None:
            mpra = "bin/mpra"
        result = pexpect.run("%s help" % mpra)
        self.assertFalse("Traceback" in result)
        self.assertTrue("age - " in result)
        self.assertTrue("date_age - " in result)
        self.assertTrue("epoch - " in result)
        self.assertTrue("history - " in result)
        self.assertTrue("migr_recs - " in result)
        self.assertTrue("purge_recs - " in result)
        self.assertTrue("reset - " in result)
        self.assertTrue("simplug - " in result)
        self.assertTrue("times - " in result)
        self.assertTrue("xplocks - " in result)
        self.assertTrue("ymd - " in result)
