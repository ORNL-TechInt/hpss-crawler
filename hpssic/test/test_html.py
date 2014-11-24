"""
Tests for html.py
"""
from hpssic import CrawlConfig
from hpssic import CrawlDBI
from hpssic import cv_sublib
from hpssic import dbschem
from hpssic import hpss
from hpssic import html_lib
from hpssic import messages as MSG
from hpssic import mpra_sublib
import os
import pexpect
import pytest
from hpssic import tcc_sublib
from hpssic import testhelp
from hpssic import util as U


# -----------------------------------------------------------------------------
@pytest.mark.skipif('jenkins' in os.getcwd(),
                    reason="html report not yet supported on jenkins")
class HtmlTest(testhelp.HelpedTestCase):
    # testdir = testhelp.testdata(__name__)

    # -----------------------------------------------------------------------------
    def setUp(self):
        """
        Set up for tests
        """
        super(HtmlTest, self).setUp()
        data = {'checkables':
                ["/log/2007/05/27/logfile01_200705270654,f,6001,X1701700," +
                 "media,0,1410363023,0,0",
                 "/log/2007/05/27/logfile01_200705271304,f,6001,X1701700," +
                 "media,0,1410363023,0,0",
                 "/log/2007/05/27/logfile02_200705270424,f,6001,X1701700," +
                 "media,0,1410363024,0,0",
                 "/log/2007/05/27/logfile02_200705270847,f,6001,X1701700," +
                 "media,0,1410363024,0,0",
                 "/log/2007/05/27/logfile02_200705271716,f,6001,X1701700," +
                 "media,0,1410363025,0,0",
                 "/log/2007/02/27/logfile01_200702270958,f,6001,X1701700," +
                 "media,0,0,0,0",
                 "/log/2007/02/27/logfile01_200702271707,f,6001,X1701700," +
                 "media,0,0,0,0",
                 "/log/2007/02/27/logfile01_200702272150,f,6001,X1701700," +
                 "media,0,0,0,0",
                 "/log/2007/02/27/logfile02_200702270431,f,6001,X1701700," +
                 "media,0,0,0,0",
                 "/log/2007/02/27/logfile02_200702271427,f,6001,X1701700," +
                 "media,0,0,0,0",
                 "/log/2007/02/27/logfile02_200702271800,f,6001,X1701700," +
                 "media,0,0,0,0",
                 ],
                'mpra':
                ["migr,1403126855,0,1335302855,0",
                 "purge,1403126872,0,0,0",
                 "migr,1403127173,1335302855,1335303172,0",
                 "purge,1403127187,0,0,0",
                 "migr,1403127488,1335303172,1335303487,0",
                 "purge,1403127502,0,0,0",
                 "migr,1403127802,1335303487,1335303802,0",
                 "purge,1403127818,0,0,0",
                 "migr,1403128118,1335303802,1335304118,0",
                 "purge,1403128142,0,0,0",
                 "migr,1403128443,1335304118,1335304443,0",
                 "purge,1403128458,0,0,0",
                 ],
                'tcc_data':
                ["1398454713,1,5,1,0",
                 "1398454968,1,5,1,0",
                 "1398455089,1,5,1,0",
                 "1398455209,1,5,1,0",
                 "1398455329,1,5,1,0",
                 "1398455440,1,5,1,0",
                 "1398455449,1,5,1,0",
                 "1398455570,1,5,1,0",
                 "1398455690,1,5,1,0",
                 "1398455810,1,5,1,0",
                 "1398455930,1,5,1,0",
                 "1398456051,1,5,1,0",
                 "1398456171,1,5,1,0",
                 "1398456244,6,10,1,0",
                 "1398456291,1,5,1,0",
                 "1398456395,6,10,1,0",
                 "1398456403,11,15,1,0",
                 "1398456428,16,20,1,0",
                 "1398456437,1,5,1,0",
                 "1398456443,6,10,1,0",
                 "1398456474,11,15,1,0",
                 "1398456504,16,20,1,0",
                 "1398456534,1,5,1,0",
                 "1398456564,6,10,1,0",
                 "1398456594,11,15,1,0",
                 ]
                }

        cfg = CrawlConfig.add_config(close=True,
                                     filename='hpssic_sqlite_test.cfg')
        cfg.set('dbi-crawler', 'dbname', self.tmpdir("test.db"))

        db = CrawlDBI.DBI(dbtype='crawler')
        for table in ['checkables', 'mpra', 'tcc_data']:
            dbschem.make_table(table)
        db.close()

        db = CrawlDBI.DBI(dbtype='crawler')
        for table in ['checkables', 'mpra', 'tcc_data']:
            fld_list = [x[1] for x in db.describe(table=table)
                        if x[1] != 'rowid']
            db.insert(table=table,
                      fields=fld_list,
                      data=[x.split(',') for x in data[table]])
        db.close()

    # -------------------------------------------------------------------------
    def test_html_report(self):
        """
        Try running 'html report > filename' and verify that 1) no traceback
        occurs and 2) something is actually written to the output file.
        """
        self.dbgfunc()
        cfpath = self.tmpdir("crawl.cfg")
        cfg = CrawlConfig.add_config()
        cfg.crawl_write(open(cfpath, 'w'))
        result = pexpect.run("html report --config %s" % cfpath)
        self.validate_report(result)

    # -------------------------------------------------------------------------
    def test_get_html_report(self):
        """
        Call html_lib.get_html_report() directly
        """
        self.dbgfunc()

        c = CrawlConfig.add_config()

        db = CrawlDBI.DBI(dbtype="crawler")
        dbschem.drop_table(table="lscos")
        self.expected(False, db.table_exists(table="lscos"))

        try:
            result = html_lib.get_html_report('')
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

        self.expected(True, db.table_exists(table="lscos"))
        db.close()
        self.validate_report(result)

    # -------------------------------------------------------------------------
    def validate_report(self, result):
        """
        Call html_lib.get_html_report() directly
        """
        if type(result) == unicode:
            result = result.encode('ascii', 'ignore')
        self.assertTrue(str == type(result),
                        "Expected result of type str, got %s" % type(result))

        self.assertTrue("Traceback" not in result,
                        '"""%s"""should not contain "Traceback"' % result)

        self.expected_in(cv_sublib.report_title(), result)
        self.expected_in(mpra_sublib.report_title(), result)
        self.expected_in(tcc_sublib.report_title(), result)

        self.expected_in("cos  *Population  *Sample", result)
