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
import email.mime.text
import os
import smtplib
import socket
import util

# -----------------------------------------------------------------------------
class Alert(object):
    # -------------------------------------------------------------------------
    def __init__(self, msg='unspecified alert', caller='', dispatch=True,
                 cfg=None):
        self.msg = msg
        self.caller = caller
        self.cfg = cfg
        if dispatch:
            self.dispatch()
        
    # -------------------------------------------------------------------------
    def dispatch(self):
        # mainmod = sys.modules['__main__']
        # cfg = mainmod.get_config()
        # log = mainmod.get_logger()
        if self.cfg is not None:
            cfg = self.cfg
        else:
            cfg = CrawlConfig.get_config()
        log = util.get_logger()
        if self.caller != '':
            section = cfg.get(self.caller, 'alerts')
        else:
            section = 'alerts'

        done = False
        while not done:
            for opt in cfg.options(section):
                if opt == 'log':
                    # write to log
                    fmt = cfg.get(section, 'log')
                    log.info(fmt, self.msg)
                    done = True

                elif opt == 'email':
                    # send mail
                    hostname = socket.gethostname()
                    addrs = cfg.get(section, 'email')
                    addrlist = [x.strip() for x in addrs.split(',')]
                    sender = 'HIC@%s' % hostname
                    payload = email.mime.text.MIMEText(self.msg)
                    payload['Subject'] = 'HPSS Integrity Crawler ALERT'
                    payload['From'] = sender
                    payload['To'] = addrs
                    s = smtplib.SMTP(hostname)
                    s.sendmail(sender, addrlist, payload.as_string())
                    log.info("sent mail to %s", addrlist)
                    done = True
                    
                elif opt == 'shell':
                    # run the program
                    cmd = cfg.get(section, 'shell')
                    cmdline = '%s %s' % (cmd, self.msg)
                    os.system(cmdline)
                    log.info("ran: '%s'" % (cmdline))
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
                    



