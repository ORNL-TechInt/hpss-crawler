from nose.plugins.attrib import attr
import os
import pdb
import pexpect
import sys
from hpssic import testhelp as th
from hpssic import util as U


# -----------------------------------------------------------------------------
class ScriptBase(th.HelpedTestCase):
    # -------------------------------------------------------------------------
    def script_which_module(self, modname):
        """
        Make sure we're loading the right tcc module
        """
        try:
            mod = sys.modules[modname]
        except KeyError:
            sep = impname = ''
            for comp in modname.split('.'):
                impname += sep + comp
                sep = '.'
                __import__(impname)
            mod = sys.modules[modname]

        tdir = improot(__file__, __name__)
        mdir = improot(mod.__file__, modname)
        self.assertEqual(tdir, mdir, "Expected '%s', got '%s'" % (tdir, mdir))

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
        self.assertFalse("Traceback" in result,
                         "'Traceback' not expected in %s" %
                         U.line_quote(result))
        for item in helplist:
            self.assertTrue(item in result,
                            "Expected '%s' in '%s'" % (item, result))


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
                                             "dbdrop - ",
                                             "fire - ",
                                             "log - ",
                                             "pw_decode - ",
                                             "pw_encode - ",
                                             "start - ",
                                             "status - ",
                                             "stop - ",
                                             ])


# -----------------------------------------------------------------------------
class Test_CV(ScriptBase):
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

    # -------------------------------------------------------------------------
    def test_cv_which_command(self):
        super(Test_CV, self).script_which_command("cv")

    # -------------------------------------------------------------------------
    def test_cv_which_module(self):
        super(Test_CV, self).script_which_module("hpssic.cv")

    # -------------------------------------------------------------------------
    def test_cv_which_plugin(self):
        super(Test_CV, self).script_which_module("hpssic.plugins.cv_plugin")


# -----------------------------------------------------------------------------
class Test_MPRA(ScriptBase):
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

    # -------------------------------------------------------------------------
    def test_mpra_which_command(self):
        super(Test_MPRA, self).script_which_command("mpra")

    # -------------------------------------------------------------------------
    def test_mpra_which_lib(self):
        super(Test_MPRA, self).script_which_module("hpssic.mpra_lib")

    # -------------------------------------------------------------------------
    def test_mpra_which_module(self):
        super(Test_MPRA, self).script_which_module("hpssic.mpra")

    # -------------------------------------------------------------------------
    def test_mpra_which_plugin(self):
        super(Test_MPRA, self).script_which_module(
            "hpssic.plugins.mpra_plugin")


# -----------------------------------------------------------------------------
class Test_RPT(ScriptBase):
    # -------------------------------------------------------------------------
    def test_rpt_help(self):
        super(Test_RPT, self).script_help("rpt",
                                          ["insert - ",
                                           "report - ",
                                           "simplug - ",
                                           "testmail - ",
                                           ])

    # -------------------------------------------------------------------------
    def test_rpt_which_command(self):
        super(Test_RPT, self).script_which_command("rpt")

    # -------------------------------------------------------------------------
    def test_rpt_which_lib(self):
        super(Test_RPT, self).script_which_module("hpssic.rpt_lib")

    # -------------------------------------------------------------------------
    def test_rpt_which_module(self):
        super(Test_RPT, self).script_which_module("hpssic.rpt")

    # -------------------------------------------------------------------------
    def test_rpt_which_plugin(self):
        super(Test_RPT, self).script_which_module("hpssic.plugins.rpt_plugin")


# -----------------------------------------------------------------------------
class Test_TCC(ScriptBase):
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
                                           "zreport - ",
                                           ])

    # -------------------------------------------------------------------------
    def test_tcc_which_command(self):
        super(Test_TCC, self).script_which_command("tcc")

    # -------------------------------------------------------------------------
    def test_tcc_which_lib(self):
        super(Test_TCC, self).script_which_module("hpssic.tcc_lib")

    # -------------------------------------------------------------------------
    def test_tcc_which_module(self):
        super(Test_TCC, self).script_which_module("hpssic.tcc")

    # -------------------------------------------------------------------------
    def test_tcc_which_plugin(self):
        super(Test_TCC, self).script_which_module("hpssic.plugins.tcc_plugin")


# -----------------------------------------------------------------------------
@attr(slow=True)
class Test_PEP8(th.HelpedTestCase):
    # -------------------------------------------------------------------------
    def test_pep8(self):
        for r, d, f in os.walk('hpssic'):
            if any([r == "./test", ".git" in r, ".attic" in r]):
                continue
            pylist = [os.path.join(r, fn) for fn in f if fn.endswith('.py')]
            if pylist:
                inputs = " ".join(pylist)
                result = pexpect.run("pep8 %s" % inputs)
                self.assertEqual("", result, "\n" + result)


# -----------------------------------------------------------------------------
def improot(path, modpath):
    rval = path
    for x in modpath.split('.'):
        rval = os.path.dirname(rval)
    return rval
