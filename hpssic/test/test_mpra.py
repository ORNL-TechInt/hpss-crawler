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


# -----------------------------------------------------------------------------
class MpraResetTest(th.HelpedTestCase):
    # -------------------------------------------------------------------------
    def cfg_dict(self):
        """
        Stand-alone config for these tests
        """
        rval = {'crawler': {'context': 'TEST',
                            'plugins': 'mpra',
                            'logpath': self.tmpdir('hpssic.log'),
                            },
                'mpra': {'report_file': self.rptname,
                         },
                'dbi-crawler': {'dbtype': 'sqlite',
                                'dbname': self.tmpdir('test.db'),
                                'tbl_prefix': 'test',
                                },
                }
        return rval

    # -------------------------------------------------------------------------
    def setUp(self):
        """
        Set up for each test: create the mpra table with prefix 'test', touch
        the mpra test file in the tmpdir
        """
        super(MpraResetTest, self).setUp()
        if pexpect.which("mpra"):
            self.cmd = "mpra"
        elif os.path.exists("bin/mpra"):
            self.cmd = "bin/mpra"
        else:
            raise HpssicError("mpra command not found")

        self.rptname = self.tmpdir("mpra_report.txt")
        self.cfgname = self.tmpdir("mpra_test.cfg")
        self.cfg = CrawlConfig.add_config(close=True, dct=self.cfg_dict())
        self.cfg.crawl_write(open(self.cfgname, 'w'))

        dbschem.make_table("mpra", cfg=self.cfg)
        U.touch(self.rptname)

    # -------------------------------------------------------------------------
    def test_mpra_reset_force(self):
        """
        Test 'mpra reset --force'
        EXP: no prompt, table is dropped, report file unlinked
        """
        self.dbgfunc()
        cmd = ("%s reset --cfg %s --force" % (self.cmd, self.cfgname))
        S = pexpect.spawn(cmd)
        S.expect(pexpect.EOF)
        S.close()
        db = CrawlDBI.DBI(dbtype="crawler", cfg=self.cfg)
        self.assertFalse(db.table_exists(table="mpra"),
                         "Expected the mpra table to be dropped")
        self.assertPathNotPresent(self.rptname)

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
        self.assertPathPresent(self.rptname)

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
        self.assertPathNotPresent(self.rptname)
