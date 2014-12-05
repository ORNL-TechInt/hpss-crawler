from hpssic import CrawlConfig
from hpssic import mpra_lib
import time
from hpssic import util


# -----------------------------------------------------------------------------
def main(cfg):
    """
    Migration Purge Record Ager (mpra) reads the database tables BFMIGRREC and
    BFPURGEREC and reports migration and purge records that are older than the
    age specified in the configuration.
    """
    if cfg is None:
        cfg = CrawlConfig.get_config()
    age = cfg.get_time('mpra', 'age')

    end = time.time() - age

    start = mpra_lib.mpra_fetch_recent("migr")
    #
    # If the configured age has been moved back in time, so that end is before
    # start, we need to reset and start scanning from the beginning of time.
    #
    if end < start:
        start = 0
    CrawlConfig.log("migr recs after %d (%s) before %d (%s)" %
                    (start, util.ymdhms(start), end, util.ymdhms(end)))
    result = mpra_lib.age("migr", start=start, end=end, mark=True)
    CrawlConfig.log("found %d migration records in the range" % result)
    rval = result

    start = mpra_lib.mpra_fetch_recent("purge")
    CrawlConfig.log("Looking for expired purge locks")
    result = mpra_lib.xplocks(mark=True)
    CrawlConfig.log("found %d expired purge locks" % result)
    rval += result

    return rval
