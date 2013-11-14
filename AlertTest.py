#!/usr/bin/env python
"""
Tests for the Alert class

In this file we import fakesmtp, which monkey patches smtplib so that any
e-mail sent gets captured in a memory inbox for inspection rather than actually
sent off the machine.
"""
import Alert
import CrawlConfig
import email.mime.text as emt
import fakesmtp
import os
import shutil
import smtplib
import socket
import stat
import sys
import testhelp
import time
import toolframe
import util

# -----------------------------------------------------------------------------
def setUpModule():
    if not os.path.exists(AlertTest.testdir):
        os.mkdir(AlertTest.testdir)

# -----------------------------------------------------------------------------
def tearDownModule():
    if not testhelp.keepfiles():
        logger = util.get_logger(reset=True, soft=True)
        shutil.rmtree(AlertTest.testdir)

# -----------------------------------------------------------------------------
class AlertTest(testhelp.HelpedTestCase):
    testdir = 'test.d'
    
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
        self.assertEqual('dispatch' in dir(x), True,
                         "'dispatch' should be an attribute of the Alert" +
                         " object, but is not")

    # -------------------------------------------------------------------------
    def test_alert_log(self):
        logfile = '%s/alert_log.log' % self.testdir
        cfg = CrawlConfig.CrawlConfig()
        cfg.add_section('crawler')
        cfg.add_section('AlertTest')
        cfg.add_section('alert_section')
        cfg.set('crawler', 'logpath', logfile)
        cfg.set('AlertTest', 'alerts', 'alert_section')
        cfg.set('alert_section', 'log', "%s")
        util.get_logger(cmdline=logfile, reset=True)
        x = Alert.Alert(caller='AlertTest', msg='this is a test message',
                        cfg=cfg)
        self.assertEqual('this is a test message' in util.contents(logfile),
                         True,
                         "'this is a test message' expected in log file" +
                         " but not found")
    
    # -------------------------------------------------------------------------
    def test_alert_shell(self):
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
        cfg.set('alert_section', 'shell', 'test.d/runme')
        util.get_logger(cmdline=logfile, reset=True)
        x = Alert.Alert(caller='AlertTest', msg='this is a test message',
                        cfg=cfg)
        expected = "ran: '%s this is a test message'" % runfile
        self.assertTrue(expected in util.contents(logfile),
                        "'%s' expected in log file" % expected +
                        " but not found")
        self.assertTrue(os.path.exists(outfile),
                        "expected %s to exist but it's not found" %
                        outfile)
    
    # -------------------------------------------------------------------------
    def test_alert_email(self):
        fakesmtp.inbox = []
        logfile = '%s/alert_email.log' % self.testdir
        targets = "addr1@somewhere.com, addr2@other.org, addr3@else.net"
        payload = 'this is an e-mail alert'
        sender = 'HIC@' + socket.gethostname()

        cfg = CrawlConfig.CrawlConfig()
        cfg.add_section('crawler')
        cfg.add_section('AlertTest')
        cfg.add_section('alert_section')
        cfg.set('crawler', 'logpath', logfile)
        cfg.set('AlertTest', 'alerts', 'alert_section')
        cfg.set('alert_section', 'email', targets)
        util.get_logger(cmdline=logfile, reset=True)

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
        self.assertTrue('sent mail to' in util.contents(logfile),
                        "expected '%s' in %s, not found")
        self.assertTrue(payload in m.fullmessage,
                        "'%s' not found in e-mail message '%s'" %
                        (payload, m.fullmessage))

if __name__ == '__main__':
    toolframe.ez_launch(test='AlertTest',
                        logfile='crawl_test.log')
