from hpssic import rpt_lib
import time

# -----------------------------------------------------------------------------
def main(cfg):
    """
    This plugin will generate a report and send it to the designated e-mail
    address(es).
    """

    if cfg is None:
        cfg = CrawlConfig.get_config()

    subject = "%s %s" % (cfg.get('rpt', 'subject'),
                         time.strftime("%Y.%m%d %H:%M:%S",
                                       time.localtime()))
    
    rpt_lib.sendmail(sender=cfg.get('rpt', 'sender'),
                     to=cfg.get('rpt', 'recipients'),
                     subject=subject,
                     body=rpt_lib.get_report())
