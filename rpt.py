#!/usr/bin/env python

import optparse
import pdb
import rpt_lib
import toolframe
import util

# -----------------------------------------------------------------------------
def rpt_testmail(args):
    """testmail - send a test e-mail to a specified address

    usage: rpt testmail [-d] -t <addr1>[,<addr2>...] -f <filename> -s <subject>
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-f', '--filename',
                 action='store', default='', dest='filename',
                 help='content to send')
    p.add_option('-s', '--subject',
                 action='store', default='default subject', dest='subject',
                 help="subject for the test message")
    p.add_option('-t', '--to',
                 action='store', default='', dest='recipient',
                 help='where to send the test mail')
    (o, a) = p.parse_args(args)

    if o.debug: pdb.set_trace()

    if o.recipient == '':
        raise StandardErrro("-t <address> option is required")
    
    if o.filename == "":
        msg = """
        This is the default body for the test e-mail that you asked for.
        """
    else:
        msg = util.contents(o.filename)

    rpt_lib.sendmail(sender='HIC-testing@%s' % util.hostname(long=True),
                     to=o.recipient,
                     subject=o.subject,
                     body=msg)
                     
# -----------------------------------------------------------------------------
toolframe.tf_launch('rpt', __name__)
