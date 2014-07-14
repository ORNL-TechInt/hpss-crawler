import ConfigParser
import sys


# -----------------------------------------------------------------------------
def main(cfg):
    try:
        msg = cfg.get('example', 'message')
    except ConfigParser.NoOptionError:
        msg = 'No message in configuration'

    log = sys.modules['__main__'].get_logger()
    log.info('EXAMPLE: This is plugin EXAMPLE saying "%s"' % msg)
