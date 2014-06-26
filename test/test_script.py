import os
import pexpect
import sys
from hpssic import testhelp as th

class ScriptBase(th.HelpedTestCase):
    # -------------------------------------------------------------------------
    def script_which_module(self, modname):
        """
        Make sure we're loading the right tcc module
        """
        try:
            mod = sys.modules[modname]
        except KeyError:
            mod = __import__(modname)
        self.assertEqual(os.path.dirname(os.path.dirname(mod.__file__)),
                         os.path.dirname(os.path.dirname(__file__)))

    # -------------------------------------------------------------------------
    def script_which_command(self, cmdname):
        """
        Make sure the tcc command exists and is executable
        """
        cmd = pexpect.which(cmdname)
        if cmd is None:
            cmd = "bin/" + cmdname
        self.assertTrue(os.access(cmd, os.X_OK))

    # -------------------------------------------------------------------------
    def script_help(self, cmdname, helplist):
        """
        Make sure 'tcc help' generates something reasonable
        """
        cmd = pexpect.which(cmdname)
        if cmd is None:
            cmd = "bin/" + cmdname
        result = pexpect.run("%s help" % cmd)
        self.assertFalse("Traceback" in result)
        for item in helplist:
            self.assertTrue(item in result)

# -----------------------------------------------------------------------------
class Test_CRAWL(ScriptBase):
    # -------------------------------------------------------------------------
    def test_crawl_which_lib(self):
        super(Test_CRAWL, self).script_which_module("hpssic.crawl_lib")

    # -------------------------------------------------------------------------
    def test_crawl_which_module(self):
        super(Test_CRAWL, self).script_which_module("hpssic.crawl")

    # -------------------------------------------------------------------------
    def test_crawl_which_command(self):
        super(Test_CRAWL, self).script_which_command("crawl")

    # -------------------------------------------------------------------------
    def test_crawl_help(self):
        super(Test_CRAWL, self).script_help("crawl",
                                            ["cfgdump - ",
                                             "cleanup - ",
                                             "cvreport - ",
                                             "dbdrop - ",
                                             "fire - ",
                                             "log - ",
                                             "pw_decode - ",
                                             "pw_encode - ",
                                             "start - ",
                                             "status - ",
                                             "stop - ",])
                                                
# -----------------------------------------------------------------------------
class Test_CV(ScriptBase):
    # -------------------------------------------------------------------------
    def test_cv_which_module(self):
        super(Test_CV, self).script_which_module("hpssic.cv")

    # -------------------------------------------------------------------------
    def test_cv_which_command(self):
        super(Test_CV, self).script_which_command("cv")

    # -------------------------------------------------------------------------
    def test_cv_help(self):
        super(Test_CV, self).script_help("cv",
                                            ["fail_reset - ",
                                             "nulltest - ",
                                             "report - ",
                                             "show_next - ",
                                             "simplug - ",
                                             "test_check - ",
                                             ])
                                                
# -----------------------------------------------------------------------------
class Test_MPRA(ScriptBase):
    # -------------------------------------------------------------------------
    def test_mpra_which_lib(self):
        super(Test_MPRA, self).script_which_module("hpssic.mpra_lib")

    # -------------------------------------------------------------------------
    def test_mpra_which_module(self):
        super(Test_MPRA, self).script_which_module("hpssic.mpra")

    # -------------------------------------------------------------------------
    def test_mpra_which_command(self):
        super(Test_MPRA, self).script_which_command("mpra")

    # -------------------------------------------------------------------------
    def test_mpra_help(self):
        super(Test_MPRA, self).script_help("mpra",
                                            ["age - ",
                                             "date_age - ",
                                             "epoch - ",
                                             "history - ",
                                             "migr_recs - ",
                                             "purge_recs - ",
                                             "reset - ",
                                             "simplug - ",
                                             "times - ",
                                             "xplocks - ",
                                             "ymd",
                                             ])
                                                
# -----------------------------------------------------------------------------
class Test_RPT(ScriptBase):
    # -------------------------------------------------------------------------
    def test_rpt_which_lib(self):
        super(Test_RPT, self).script_which_module("hpssic.rpt_lib")

    # -------------------------------------------------------------------------
    def test_rpt_which_module(self):
        super(Test_RPT, self).script_which_module("hpssic.rpt")

    # -------------------------------------------------------------------------
    def test_rpt_which_command(self):
        super(Test_RPT, self).script_which_command("rpt")

    # -------------------------------------------------------------------------
    def test_rpt_help(self):
        super(Test_RPT, self).script_help("rpt",
                                            ["insert - ",
                                             "report - ",
                                             "simplug - ",
                                             "testmail - ",
                                             ])
                                                
# -----------------------------------------------------------------------------
class Test_TCC(ScriptBase):
    # -------------------------------------------------------------------------
    def test_tcc_which_lib(self):
        super(Test_TCC, self).script_which_module("hpssic.tcc_lib")

    # -------------------------------------------------------------------------
    def test_tcc_which_module(self):
        super(Test_TCC, self).script_which_module("hpssic.tcc")

    # -------------------------------------------------------------------------
    def test_tcc_which_command(self):
        super(Test_TCC, self).script_which_command("tcc")

    # -------------------------------------------------------------------------
    def test_tcc_help(self):
        super(Test_TCC, self).script_help("tcc",
                                          ["bfid - ",
                                           "bfpath - ",
                                           "copies_by_cos - ",
                                           "copies_by_file - ",
                                           "report - ",
                                           "selbf - ",
                                           "simplug - ",
                                           "tables - ",
                                           "zreport - ",])
                                                
