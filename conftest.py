import os
import pdb
import pytest
import sys
from hpssic import version
import time


# -----------------------------------------------------------------------------
def hpssic_test_log(config, loggable):
    """
    Write to the default log path unless the user said --nolog
    """
    if config.getoption("nolog"):
        return
    msg = "%s %s\n" % (time.strftime("%Y.%m%d %H:%M:%S"),
                       loggable)
    with open(hpssic_test_log._logpath, 'a') as f:
        f.write(msg)

# -----------------------------------------------------------------------------
def pytest_addoption(parser):
    """
    Add options --nolog, --all to the command line
    """
    global attr
    parser.addoption("--all", action="store_true",
                     help="run all tests")
    parser.addoption("--fast", action="store_true",
                     help="run only the fast tests")
    parser.addoption("--nolog", action="store_true", default=False,
                     help="suppress test logging")
    parser.addoption("--dbg", action="append", default=[],
                     help="start debugger on named test or all")
    attr = pytest.mark.attr


# -----------------------------------------------------------------------------
def pytest_report_header(config):
    """
    Add version to header of test report
    """
    hpssic_test_log(config, "-" * 60)
    return("Testing HPSSIC version %s" % version.__version__)


# -----------------------------------------------------------------------------
@pytest.mark.tryfirst
def pytest_runtest_makereport(item, call, __multicall__):
    """
    Write a line to the log file for this test
    """
    rep = __multicall__.execute()
    if rep.when != 'call':
        return rep

    if rep.outcome == 'failed':
        status = ">>>>FAIL"
        hpssic_test_log._failcount += 1
    else:
        status = "--pass"
        hpssic_test_log._passcount += 1

    parent = item.parent
    msg = "%-8s %s:%s.%s" % (status,
                             os.path.basename(parent.fspath.strpath),
                             parent.name,
                             item.name)
    hpssic_test_log(item.config, msg)
    return rep


# -----------------------------------------------------------------------------
def pytest_runtest_setup(item):
    """
    Decide whether to skip a test under consideration
    """
    if 'attr' in item.keywords and item.config.getoption("--fast"):
        pytest.skip('slow')


# -----------------------------------------------------------------------------
def pytest_unconfigure(config):
    hpssic_test_log(config,
                    "passed: %d; FAILED: %d" % (hpssic_test_log._passcount,
                                                hpssic_test_log._failcount))


hpssic_test_log._logpath = "hpssic/test/hpssic_test.log"
hpssic_test_log._passcount = 0
hpssic_test_log._failcount = 0
