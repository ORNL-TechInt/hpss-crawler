import CrawlConfig
import sys


# -----------------------------------------------------------------------------
def main(cfg):
    try:
        msg = cfg.get('example', 'message')
    except ConfigParser.NoOptionError:
        msg = 'No message in configuration'

    CrawlConfig.log('EXAMPLE: This is plugin EXAMPLE saying "%s"' % msg)
