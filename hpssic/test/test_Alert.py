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
from hpssic import util
from hpssic import util as U


# -----------------------------------------------------------------------------
class AlertTest(testhelp.HelpedTestCase):

    # -------------------------------------------------------------------------
    def test_init(self):
        """
        Get an Alert object and make sure it has the correct attributes
        """
        self.dbgfunc()
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
        self.dbgfunc()
        logfile = self.tmpdir('alert_log.log')
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
        self.dbgfunc()
        logfile = self.tmpdir('alert_shell.log')
        outfile = self.tmpdir('alert_shell.out')
        runfile = self.tmpdir('runme')
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
        cfg.set('alert_section', 'shell', runfile + " %s")
        CrawlConfig.log(logpath=logfile, close=True)
        x = Alert.Alert(caller='AlertTest', msg='this is a test message',
                        cfg=cfg)
        expected = "ran: '%s this is a test message'" % runfile
        self.expected_in(expected, util.contents(logfile))
        self.assertPathPresent(outfile)

    # -------------------------------------------------------------------------
    def test_alert_shell_nospec(self):
        """
        Generate a shell alert and verify that it ran. With no '%s' in the
        shell alert string, no message should be offered for formatting.
        """
        self.dbgfunc()
        logfile = self.tmpdir('alert_shell.log')
        outfile = self.tmpdir('alert_shell.out')
        runfile = self.tmpdir('runme')
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
        cfg.set('alert_section', 'shell', runfile)
        CrawlConfig.log(logpath=logfile, close=True)
        x = Alert.Alert(caller='AlertTest', msg='this is a test message',
                        cfg=cfg)
        expected = "ran: '%s'" % runfile
        self.expected_in(expected, util.contents(logfile))
        self.assertPathPresent(outfile)

    # -------------------------------------------------------------------------
    def test_alert_email(self):
        """
        Generate an e-mail alert and verify that it was sent (this is where we
        use 'monkey patching').
        """
        self.dbgfunc()
        fakesmtp.inbox = []
        logfile = self.tmpdir('alert_email.log')
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
        m = fakesmtp.inbox[0]
        self.expected(targets, ', '.join(m.to_address))
        self.expected(sender, m.from_address)
        self.expected_in('sent mail to', util.contents(logfile))
        self.expected_in(payload, m.fullmessage)

    # -------------------------------------------------------------------------
    def test_alert_email_mtcaller(self):
        """
        Generate an e-mail alert and verify that it was sent (this is where we
        use 'monkey patching'). For this case, caller is ''.
        """
        self.dbgfunc()
        fakesmtp.inbox = []
        logfile = self.tmpdir('alert_email.log')
        targets = "addr1@somewhere.com, addr2@other.org, addr3@else.net"
        payload = 'this is an e-mail alert'
        sender = 'hpssic@' + util.hostname(long=True)

        cfg = CrawlConfig.CrawlConfig()
        cfg.add_section('crawler')
        cfg.add_section('alerts')
        cfg.set('crawler', 'logpath', logfile)
        cfg.set('alerts', 'email', targets)
        CrawlConfig.log(logpath=logfile, close=True)

        x = Alert.Alert(caller='', msg=payload,
                        cfg=cfg)
        m = fakesmtp.inbox[0]
        self.expected(targets, ', '.join(m.to_address))
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
        CrawlConfig.add_config(close=True)
        with U.tmpenv('CRAWL_CONF', 'hpssic_test.cfg'):
            logfile = self.tmpdir('alert_email.log')
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
        self.dbgfunc()
        logfile = self.tmpdir('alert_use.log')
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
        self.dbgfunc()
        logfile = self.tmpdir('alert_use.log')
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
