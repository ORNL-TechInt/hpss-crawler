"""
Tests for html.py
"""
from hpssic import CrawlConfig
from hpssic import CrawlDBI
from hpssic import cv_sublib
from hpssic import dbschem
from hpssic import html_lib
from hpssic import mpra_sublib
import os
import pexpect
import pytest
from hpssic import tcc_sublib
from hpssic import testhelp
from hpssic import util as U


# -----------------------------------------------------------------------------
def setUpModule():
    """
    Set up for tests
    """
    CrawlConfig.get_config(reset=True, soft=True)


# -----------------------------------------------------------------------------
@pytest.mark.skipif('jenkins' in os.getcwd(),
                    reason="html report not yet supported on jenkins")
class HtmlTest(testhelp.HelpedTestCase):
    testdir = testhelp.testdata(__name__)

    # -------------------------------------------------------------------------
    def test_html_report(self):
        """
        Try running 'html report > filename' and verify that 1) no traceback
        occurs and 2) something is actually written to the output file.
        """
        self.dbgfunc()
        result = pexpect.run("html report")
        self.validate_report(result)

    # -------------------------------------------------------------------------
    def test_get_html_report(self):
        """
        Call html_lib.get_html_report() directly
        """
        self.dbgfunc()

        db = CrawlDBI.DBI(dbtype="crawler")
        dbschem.drop_table(table="lscos")
        self.expected(False, db.table_exists(table="lscos"))

        result = html_lib.get_html_report('')

        self.expected(True, db.table_exists(table="lscos"))
        db.close()
        self.validate_report(result)

    # -------------------------------------------------------------------------
    def validate_report(self, result):
        """
        Call html_lib.get_html_report() directly
        """
        self.assertTrue(str == type(result),
                        "Expected result of type str, got %s" % type(result))

        self.assertTrue("Traceback" not in result,
                        '"""%s"""should not contain "Traceback"' % result)

        self.expected_in(cv_sublib.report_title(), result)
        self.expected_in(mpra_sublib.report_title(), result)
        self.expected_in(tcc_sublib.report_title(), result)

        self.expected_in("cos  *Population  *Sample", result)
