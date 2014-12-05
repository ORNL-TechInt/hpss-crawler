from hpssic import CrawlMail
from hpssic import rpt_lib
import time


# -----------------------------------------------------------------------------
def main(cfg):
    """
    This plugin will generate a report and send it to the designated e-mail
    address(es).
    """
    rval = 0
    try:
        if cfg is None:
            cfg = CrawlConfig.get_config()

        subject = "%s %s" % (cfg.get('rpt', 'subject'),
                             time.strftime("%Y.%m%d %H:%M:%S",
                                           time.localtime()))

        CrawlMail.send(sender=cfg.get('rpt', 'sender'),
                       to='rpt.recipients',
                       subj=subject,
                       msg=rpt_lib.get_report())
    except Exception as e:
        rval = 1
        CrawlConfig.log("Failure in rpt_lib: '%s'" % str(e))

    return rval
