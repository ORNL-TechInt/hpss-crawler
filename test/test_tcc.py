import os
from hpssic import pexpect
from hpssic import tcc
from hpssic import testhelp as th
import unittest

# -----------------------------------------------------------------------------
class Test_TCC(th.HelpedTestCase):
    # -------------------------------------------------------------------------
    def test_tcc_which_module(self):
        """
        Make sure we're loading the right tcc module
        """
        self.assertEqual(os.path.dirname(os.path.dirname(tcc.__file__)),
                         os.path.dirname(os.path.dirname(__file__)))

    # -------------------------------------------------------------------------
    def test_tcc_which_command(self):
        """
        Make sure the tcc command exists and is executable
        """
        cmd = pexpect.which("tcc")
        if cmd is None:
            cmd = "bin/tcc"
        self.assertTrue(os.access(cmd, os.X_OK))

    # -------------------------------------------------------------------------
    def test_tcc_help(self):
        """
        Make sure 'tcc help' generates something reasonable
        """
        tcc = pexpect.which("tcc")
        if tcc is None:
            tcc = "bin/tcc"
        result = pexpect.run("%s help" % tcc)
        self.assertFalse("Traceback" in result)
        self.assertTrue("bfid - " in result)
        self.assertTrue("bfpath - " in result)
        self.assertTrue("bfts - " in result)
        self.assertTrue("copies_by_cos - " in result)
        self.assertTrue("copies_by_file - " in result)
        self.assertTrue("report - " in result)
        self.assertTrue("selbf - " in result)
        self.assertTrue("simplug - " in result)
        self.assertTrue("tables - " in result)
        self.assertTrue("zreport - " in result)
