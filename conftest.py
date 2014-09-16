import pdb
import pytest
import sys
from hpssic import version


# -----------------------------------------------------------------------------
def pytest_report_header(config):
    """
    Add version to header of test report
    """
    return("Testing HPSSIC version %s" % version.__version__)


# -----------------------------------------------------------------------------
def pytest_addoption(parser):
    """
    Add option --all to py.test command line
    """
    global attr
    parser.addoption("--all", action="store_true",
                     help="run all tests")
    attr = pytest.mark.attr


# -----------------------------------------------------------------------------
def pytest_runtest_setup(item):
    """
    Decide whether to skip a test under consideration
    """
    if 'attr' in item.keywords and not item.config.getoption("--all"):
        pytest.skip('slow')
