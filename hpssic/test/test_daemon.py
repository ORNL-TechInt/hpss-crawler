"""
Tests for daemon.py
"""
from hpssic import CrawlConfig
from hpssic import daemon
import os
import sys
from hpssic import testhelp
from hpssic import util


# -----------------------------------------------------------------------------
class daemonTest(testhelp.HelpedTestCase):
    """
    Tests for the daemon class
    """
    # -------------------------------------------------------------------------
    def test_ctor_attrs(self):
        """
        Verify that a newly created daemon object has the right attributes
        """
        a = daemon.Daemon(self.tmpdir('daemon_pid'))

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
        a = daemon.Daemon(self.tmpdir("daemon_pid"))
        exp = 4096
        val = a.get_max_fd()
        self.expected(exp, val)

    # -------------------------------------------------------------------------
    def test_dlog(self):
        lfname = self.tmpdir('daemon.dlog.log')
        lf = CrawlConfig.log(logpath=lfname)
        a = daemon.Daemon(self.tmpdir("daemon_pid"), logger=lf)
        logmsg = "testing the dlog method of %s" % a
        a.dlog(logmsg)
        self.assertTrue(logmsg in util.contents(lfname),
                        "Expected '%s' in '%s'" %
                        (logmsg,
                         util.line_quote(util.contents(lfname))))
