"""
Tests for html.py
"""
from hpssic import html
from hpssic import testhelp
from hpssic import util as U

# -----------------------------------------------------------------------------
class HtmlTest(testhelp.HelpedTestCase):
    testdir = testhelp.testdata(__name__)

    # -------------------------------------------------------------------------
    def test_html_report(self):
        """
        Try running 'html report > filename' and verify that 1) no traceback
        occurs and 2) something is actually written to the output file.
        """
        self.fail('construction')
