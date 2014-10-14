"""
Tests for html.py
"""
from hpssic import cv_sublib
from hpssic import html
from hpssic import mpra_sublib
import os
import pexpect
import pytest
from hpssic import tcc_sublib
from hpssic import testhelp
from hpssic import util as U


# -----------------------------------------------------------------------------
@pytest.mark.skipif('jenkins' in os.getcwd())
class HtmlTest(testhelp.HelpedTestCase):
    testdir = testhelp.testdata(__name__)

    # -------------------------------------------------------------------------
    def test_html_report(self):
        """
        Try running 'html report > filename' and verify that 1) no traceback
        occurs and 2) something is actually written to the output file.
        """
        result = pexpect.run("html report")
        self.assertTrue(str == type(result),
                        "Expected result of type str, got %s" % type(result))

        self.assertTrue("Traceback" not in result,
                        '"""%s"""should not contain "Traceback"' % result)

        self.expected_in(cv_sublib.report_title(), result)
        self.expected_in(mpra_sublib.report_title(), result)
        self.expected_in(tcc_sublib.report_title(), result)

        self.expected_in("cos  *Population  *Sample", result)
