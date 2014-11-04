#!/usr/bin/env python
"""
Tests for the Alert class

In this file we import fakesmtp, which monkey patches smtplib so that any
e-mail sent gets captured in a memory inbox for inspection rather than actually
sent off the machine.
"""
from hpssic import Alert
from hpssic import CrawlConfig
import email.mime.text as emt
from hpssic import fakesmtp
import os
import pytest
import socket
import stat
import sys
from hpssic import testhelp
from hpssic import toolframe
from hpssic import util
from hpssic import util as U


# -----------------------------------------------------------------------------
def setUpModule():
    """
    Set up test directory.
    """
    testhelp.module_test_setup(AlertTest.testdir)


# -----------------------------------------------------------------------------
def tearDownModule():
    """
    Clean up test directory, removing any test data left in it.
    """
    testhelp.module_test_teardown(AlertTest.testdir)


# -----------------------------------------------------------------------------
class AlertTest(testhelp.HelpedTestCase):
    testdir = testhelp.testdata(__name__)

    # -------------------------------------------------------------------------
    def test_init(self):
        """
        Get an Alert object and make sure it has the correct attributes
        """
        x = Alert.Alert('this is the message',
                        caller=util.my_name(),
                        dispatch=False)
        self.expected('this is the message', x.msg)
        self.expected(util.my_name(), x.caller)
        self.expected_in('dispatch', dir(x))

    # -------------------------------------------------------------------------
    def test_alert_log(self):
        """
        Generate a log alert and verify that the message was written to the
        correct log file.
        """
        logfile = '%s/alert_log.log' % self.testdir
        cfg = CrawlConfig.CrawlConfig()
        cfg.add_section('crawler')
        cfg.add_section('AlertTest')
        cfg.add_section('alert_section')
        cfg.set('crawler', 'logpath', logfile)
        cfg.set('AlertTest', 'alerts', 'alert_section')
        cfg.set('alert_section', 'log', "%s")
        CrawlConfig.log(logpath=logfile, close=True)
        x = Alert.Alert(caller='AlertTest', msg='this is a test message',
                        cfg=cfg)
        self.expected_in('this is a test message', util.contents(logfile))

    # -------------------------------------------------------------------------
    def test_alert_shell(self):
        """
        Generate a shell alert and verify that it ran.
        """
        logfile = '%s/alert_shell.log' % self.testdir
        outfile = '%s/alert_shell.out' % self.testdir
        runfile = '%s/runme' % self.testdir
        f = open(runfile, 'w')
        f.write("#!/bin/bash\n")
        f.write("echo \"ALERT: $*\" > %s\n" % outfile)
        f.close()
        os.chmod(runfile,
                 stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
                 stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP |
                 stat.S_IROTH | stat.S_IXOTH)
        cfg = CrawlConfig.CrawlConfig()
        cfg.add_section('crawler')
        cfg.add_section('AlertTest')
        cfg.add_section('alert_section')
        cfg.set('crawler', 'logpath', logfile)
        cfg.set('AlertTest', 'alerts', 'alert_section')
        cfg.set('alert_section', 'shell', '%s/runme' % self.testdir + " %s")
        CrawlConfig.log(logpath=logfile, close=True)
        x = Alert.Alert(caller='AlertTest', msg='this is a test message',
                        cfg=cfg)
        expected = "ran: '%s this is a test message'" % runfile
        self.expected_in(expected, util.contents(logfile))
        self.assertTrue(os.path.exists(outfile),
                        "expected %s to exist but it's not found" %
                        outfile)

    # -------------------------------------------------------------------------
    def test_alert_shell_nospec(self):
        """
        Generate a shell alert and verify that it ran. With no '%s' in the
        shell alert string, no message should be offered for formatting.
        """
        logfile = '%s/alert_shell.log' % self.testdir
        outfile = '%s/alert_shell.out' % self.testdir
        runfile = '%s/runme' % self.testdir
        f = open(runfile, 'w')
        f.write("#!/bin/bash\n")
        f.write("echo \"ALERT: $*\" > %s\n" % outfile)
        f.close()
        os.chmod(runfile,
                 stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
                 stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP |
                 stat.S_IROTH | stat.S_IXOTH)
        cfg = CrawlConfig.CrawlConfig()
        cfg.add_section('crawler')
        cfg.add_section('AlertTest')
        cfg.add_section('alert_section')
        cfg.set('crawler', 'logpath', logfile)
        cfg.set('AlertTest', 'alerts', 'alert_section')
        cfg.set('alert_section', 'shell', '%s/runme' % self.testdir)
        CrawlConfig.log(logpath=logfile, close=True)
        x = Alert.Alert(caller='AlertTest', msg='this is a test message',
                        cfg=cfg)
        expected = "ran: '%s'" % runfile
        self.expected_in(expected, util.contents(logfile))
        self.assertTrue(os.path.exists(outfile),
                        "expected %s to exist but it's not found" %
                        outfile)

    # -------------------------------------------------------------------------
    def test_alert_email(self):
        """
        Generate an e-mail alert and verify that it was sent (this is where we
        use 'monkey patching').
        """
        self.dbgfunc()
        fakesmtp.inbox = []
        logfile = '%s/alert_email.log' % self.testdir
        targets = "addr1@somewhere.com, addr2@other.org, addr3@else.net"
        payload = 'this is an e-mail alert'
        sender = 'hpssic@' + util.hostname(long=True)

        cfg = CrawlConfig.CrawlConfig()
        cfg.add_section('crawler')
        cfg.add_section('AlertTest')
        cfg.add_section('alert_section')
        cfg.set('crawler', 'logpath', logfile)
        cfg.set('AlertTest', 'alerts', 'alert_section')
        cfg.set('alert_section', 'email', targets)
        CrawlConfig.log(logpath=logfile, close=True)

        x = Alert.Alert(caller='AlertTest', msg=payload,
                        cfg=cfg)
        # print fakesmtp.inbox[0].fullmessage
        m = fakesmtp.inbox[0]
        self.assertEqual(', '.join(m.to_address),
                         targets,
                         "'%s' does not match '%s'" %
                         (', '.join(m.to_address), targets))
        self.assertEqual(m.from_address, sender,
                         "from address '%s' does not match sender '%s'" %
                         (m.from_address, sender))
        self.expected_in('sent mail to', util.contents(logfile))
        self.expected_in(payload, m.fullmessage)

    # -------------------------------------------------------------------------
    def test_alert_email_mtcaller(self):
        """
        Generate an e-mail alert and verify that it was sent (this is where we
        use 'monkey patching'). For this case, caller is ''.
        """
        fakesmtp.inbox = []
        logfile = '%s/alert_email.log' % self.testdir
        targets = "addr1@somewhere.com, addr2@other.org, addr3@else.net"
        payload = 'this is an e-mail alert'
        sender = 'hpssic@' + util.hostname(long=True)

        cfg = CrawlConfig.CrawlConfig()
        cfg.add_section('crawler')
        # cfg.add_section('AlertTest')
        cfg.add_section('alerts')
        cfg.set('crawler', 'logpath', logfile)
        # cfg.set('AlertTest', 'alerts', 'alert_section')
        cfg.set('alerts', 'email', targets)
        CrawlConfig.log(logpath=logfile, close=True)

        x = Alert.Alert(caller='', msg=payload,
                        cfg=cfg)
        # print fakesmtp.inbox[0].fullmessage
        m = fakesmtp.inbox[0]
        self.assertEqual(', '.join(m.to_address),
                         targets,
                         "'%s' does not match '%s'" %
                         (', '.join(m.to_address), targets))
        self.expected(m.from_address, sender)
        self.expected_in('sent mail to', util.contents(logfile))
        self.expected_in(payload, m.fullmessage)

    # -------------------------------------------------------------------------
    def test_alert_email_defcfg(self):
        """
        Generate an e-mail alert using the default config and verify that it
        was sent (this is where we use 'monkey patching').
        """
        self.dbgfunc()
        fakesmtp.inbox = []
        with U.tmpenv('CRAWL_CONF', 'crawl_cfg'):
            logfile = '%s/alert_email.log' % self.testdir
            targets = "addr1@domain.gov, addr2@domain.gov"
            payload = 'this is an e-mail alert'
            sender = 'hpssic@' + util.hostname(long=True)
            CrawlConfig.log(logpath=logfile, close=True)

            x = Alert.Alert(caller='cv', msg=payload)
            m = fakesmtp.inbox[0]
            self.expected(', '.join(m.to_address), targets)
            self.expected(m.from_address, sender)
            self.expected_in('sent mail to', util.contents(logfile))
            self.expected_in(payload, m.fullmessage)

    # -------------------------------------------------------------------------
    def test_alert_use_other(self):
        """
        A use directive sends us to another config section where we generate a
        log alert and verify that the message was written to the correct log
        file.
        """
        logfile = '%s/alert_use.log' % self.testdir
        cfg = CrawlConfig.CrawlConfig()
        cfg.add_section('crawler')
        cfg.add_section('AlertTest')
        cfg.add_section('alert_section')
        cfg.add_section('other_section')
        cfg.set('crawler', 'logpath', logfile)
        cfg.set('AlertTest', 'alerts', 'alert_section')
        cfg.set('alert_section', 'use', "other_section")
        cfg.set('other_section', 'log', "%s")
        CrawlConfig.log(logpath=logfile, close=True)
        payload = 'this is a test message from %s' % util.my_name()
        x = Alert.Alert(caller='AlertTest', msg=payload,
                        cfg=cfg)
        self.expected_in(payload, util.contents(logfile))

    # -------------------------------------------------------------------------
    def test_alert_use_same(self):
        """
        Generate a log alert and verify that the message was written to the
        correct log file.
        """
        logfile = '%s/alert_use.log' % self.testdir
        cfg = CrawlConfig.CrawlConfig()
        cfg.add_section('crawler')
        cfg.add_section('AlertTest')
        cfg.add_section('alert_section')
        cfg.set('crawler', 'logpath', logfile)
        cfg.set('AlertTest', 'alerts', 'alert_section')
        cfg.set('alert_section', 'log', "%s")
        cfg.set('alert_section', 'use', 'alert_section')
        CrawlConfig.log(logpath=logfile, close=True)
        payload = 'this is a test message from %s' % util.my_name()
        x = Alert.Alert(caller='AlertTest', msg=payload,
                        cfg=cfg)
        self.expected_in(payload, util.contents(logfile))


# -----------------------------------------------------------------------------
if __name__ == '__main__':
    toolframe.ez_launch(test='AlertTest',
                        logfile=testhelp.testlog(__name__))
