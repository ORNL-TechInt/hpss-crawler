#!/usr/bin/env python
"""
Tests for daemon.py
"""
from hpssic import CrawlConfig
from hpssic import daemon
import os
import sys
from hpssic import testhelp
from hpssic import toolframe
from hpssic import util

mself = sys.modules[__name__]
logfile = "%s/crawl_test.log" % os.path.dirname(mself.__file__)


M = sys.modules['__main__']
if 'py.test' in M.__file__:
    import pytest
    attr = pytest.mark.attr
else:
    from nose.plugins.attrib import attr


# -----------------------------------------------------------------------------
def setUpModule():
    """
    Set up for testing
    """
    testhelp.module_test_setup(daemonTest.testdir)


# -----------------------------------------------------------------------------
def tearDownModule():
    """
    Clean up after testing
    """
    testhelp.module_test_teardown(daemonTest.testdir)


# -----------------------------------------------------------------------------
class daemonTest(testhelp.HelpedTestCase):
    """
    Tests for the daemon class
    """
    testdir = '%s/test.d' % os.path.dirname(mself.__file__)

    # -------------------------------------------------------------------------
    def test_ctor_attrs(self):
        """
        Verify that a newly created daemon object has the right attributes
        """
        a = daemon.Daemon('%s/daemon_pid' % self.testdir)

        for attr in ['origdir', 'stdin', 'stdout', 'stderr', 'pidfile',
                     'logger', '__repr__', 'workdir', 'daemonize',
                     'close_if_open', 'dlog', 'delpid', 'get_max_fd',
                     'start', 'stop', 'restart', 'run']:
            self.assertTrue(hasattr(a, attr),
                            "Expected %s to have attribute '%s'" %
                            (a, attr))

    # -------------------------------------------------------------------------
    def test_get_max_fd(self):
        """
        Call get_max_fd on a daemon object.
        """
        a = daemon.Daemon("%s/daemon_pid" % self.testdir)
        exp = 4096
        val = a.get_max_fd()
        self.expected(exp, val)

    # -------------------------------------------------------------------------
    def test_dlog(self):
        lfname = '%s/daemon.dlog.log' % self.testdir
        lf = CrawlConfig.log(logpath=lfname)
        a = daemon.Daemon("%s/daemon_pid" % self.testdir,
                          logger=lf)
        logmsg = "testing the dlog method of %s" % a
        a.dlog(logmsg)
        self.assertTrue(logmsg in util.contents(lfname),
                        "Expected '%s' in '%s'" %
                        (logmsg,
                         util.line_quote(util.contents(lfname))))
