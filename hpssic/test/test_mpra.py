from hpssic import CrawlConfig
from hpssic import CrawlDBI
from hpssic import dbschem
from hpssic import messages as MSG
import os
import pdb
import pexpect
import shutil
import sys
import tempfile
from hpssic import testhelp as th
from hpssic import util as U

M = sys.modules['__main__']
if 'py.test' in M.__file__:
    import pytest
    attr = pytest.mark.attr
else:
    from nose.plugins.attrib import attr


# -----------------------------------------------------------------------------
class MpraResetTest(th.HelpedTestCase):
    # -------------------------------------------------------------------------
    @classmethod
    def setUpClass(cls):
        """
        Set up for all tests: get a test dir, create the config
        """
        cls.testdir = tempfile.mkdtemp(dir="/tmp")
        cls.cfgname = os.path.join(cls.testdir, "mpra_test.cfg")
        cls.rptname = os.path.join(cls.testdir, "mpra_report.txt")

        if pexpect.which("mpra"):
            cls.cmd = "mpra"
        elif os.path.exists("bin/mpra"):
            cls.cmd = "bin/mpra"
        else:
            raise HpssicError("mpra command not found")

        cls.cfg = CrawlConfig.get_config(cfname="crawl.cfg", reset=True)
        cls.cfg.set('dbi-crawler', 'tbl_prefix', 'test')
        cls.cfg.set('mpra', 'report_file', cls.rptname)
        cls.cfg.crawl_write(open(cls.cfgname, 'w'))

    # -------------------------------------------------------------------------
    def setUp(self):
        """
        Set up for each test: create the mpra table with prefix 'test', touch
        the mpra test file in the testdir
        """
        dbschem.make_table("mpra", cfg=self.cfg)
        U.touch(self.rptname)

    # -------------------------------------------------------------------------
    @classmethod
    def tearDownClass(cls):
        """
        Tear down after all tests: drop the mpra table if it's still there,
        remove the temp test directory
        """
        if not th.keepfiles():
            shutil.rmtree(cls.testdir)
            th.drop_test_tables()

    # -------------------------------------------------------------------------
    def test_mpra_reset_force(self):
        """
        Test 'mpra reset --force'
        EXP: no prompt, table is dropped, report file unlinked
        """
        cmd = ("%s reset --cfg %s --force" % (self.cmd, self.cfgname))
        S = pexpect.spawn(cmd)
        S.expect(pexpect.EOF)
        S.close()
        db = CrawlDBI.DBI(dbtype="crawler", cfg=self.cfg)
        self.assertFalse(db.table_exists(table="mpra"),
                         "Expected the mpra table to be dropped")
        self.assertFalse(os.path.exists(self.rptname),
                         "Expected %s to be unlinked" % self.rptname)

    # -------------------------------------------------------------------------
    def test_mpra_reset_prompt_no(self):
        """
        Test 'mpra reset', respond to the prompt with 'no'
        EXP: the table is NOT dropped, the mpra report file is left behind
        """
        cmd = ("%s reset --cfg %s" % (self.cmd, self.cfgname))
        S = pexpect.spawn(cmd)
        S.expect("Are you sure\? >")
        S.sendline("no")
        S.expect(pexpect.EOF)
        S.close()
        db = CrawlDBI.DBI(dbtype="crawler", cfg=self.cfg)
        self.assertTrue(db.table_exists(table="mpra"))
        self.assertTrue(os.path.exists(self.rptname))

    # -------------------------------------------------------------------------
    def test_mpra_reset_prompt_yes(self):
        """
        Test 'mpra reset', respond to the prompt with 'yes'
        EXP: the mpra table is dropped, the mpra report file is unlinked
        """
        cmd = ("%s reset --cfg %s" % (self.cmd, self.cfgname))
        S = pexpect.spawn(cmd)
        S.expect("Are you sure\? >")
        S.sendline("yes")
        S.expect(pexpect.EOF)
        S.close()
        db = CrawlDBI.DBI(dbtype="crawler", cfg=self.cfg)
        self.assertFalse(db.table_exists(table="mpra"))
        self.assertFalse(os.path.exists(self.rptname))
