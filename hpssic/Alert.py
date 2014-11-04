#!/usr/bin/env python
"""
Dispatch messages via log, email, or external program

Usage:
    xxx.cfg:
        [plugin]
        ...
        alerts = plug_alert_section

        [plug_alert_section]
        email = addr1, addr2, addr3
        log   = !!!ALERT!!! %s
        shell = echo %s
        use   = other_section

    ...
    x = Alert("The balrog frobnicated!")
    ...

Any options after 'use' in the configuration section will be ignored. If 'use'
points at the current section, the 'use' will terminate parsing of the section,
but the section will not be re-parsed, thus avoiding an infinite loop. An
infinite loop can be created by having two or more sections use each other.

The tests for the Alert class are in the file AlertTest.py. This facilitates
the testing of the email alert through monkey patching as described at

http://www.psychicorigami.com/2007/09/20/monkey-patching-pythons-smtp-lib-for-
unit-testing/

"""
import CrawlConfig
import CrawlMail
import email.mime.text
import os
import pdb
import smtplib
import socket
import util


# -----------------------------------------------------------------------------
class Alert(object):
    # -------------------------------------------------------------------------
    def __init__(self, msg='unspecified alert', caller='', dispatch=True,
                 cfg=None):
        """
        Constructor for alert object. Calls dispatch if the *dispatch* argument
        is True.
        """
        self.msg = msg
        self.caller = caller
        self.cfg = cfg
        if dispatch:
            self.dispatch()

    # -------------------------------------------------------------------------
    def dispatch(self):
        """
        Figure out where we're supposed to send this alert and send it.
        Possible destinations are the log file, one or more e-mail addresses,
        and/or a shell program.

        It's also possible for a 'use' option to show up in the alerts section.
        In this case, we're being redirected to another section, also 'use' can
        also point to the current alerts section. There's no reason to ever do
        this, but it could happen so we want to handle it in a reasonable way.

        That's why we sort the config options in the while statement below --
        to make 'use' get handled last, so any other options in the section
        will get handled. Once we process 'use', anything not yet processed in
        the current section is ignored.
        """
        if self.cfg is not None:
            cfg = self.cfg
        else:
            cfg = CrawlConfig.get_config()
        if self.caller != '':
            section = cfg.get(self.caller, 'alerts')
        else:
            section = 'alerts'

        done = False
        while not done:
            for opt in sorted(cfg.options(section)):
                if opt == 'log':
                    # write to log
                    fmt = cfg.get(section, 'log')
                    CrawlConfig.log(fmt, self.msg)
                    done = True

                elif opt == 'shell':
                    # run the program
                    cmd = cfg.get(section, 'shell')
                    if '%s' in cmd:
                        cmdline = cmd % (self.msg)
                    else:
                        cmdline = cmd
                    os.system(cmdline)
                    CrawlConfig.log("ran: '%s'" % (cmdline))
                    done = True

                elif opt == 'email':
                    CrawlMail.send(cfg=cfg,
                                   to="%s.email" % section,
                                   subj="HPSS Integrity Crawler ALERT",
                                   msg=self.msg)
                    done = True

                elif opt == 'use':
                    # delegate to another section
                    done = True
                    new_section = cfg.get(section, 'use')

                    # if it's the same section, ignore the 'use', but we don't
                    # want to break the rule that all options after a 'use' are
                    # ignored. So we set done to True to terminate the while
                    # loop and break unconditionally at the end of this clause
                    # to get out of the for loop
                    if new_section != section:
                        section = new_section
                        done = False
                    break
