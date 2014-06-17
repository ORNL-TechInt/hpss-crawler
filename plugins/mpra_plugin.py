#!/usr/bin/env python
import CrawlConfig
import mpra_lib
import time
import util

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
    CrawlConfig.log("migr recs after %d (%s) before %d (%s)" %
                    (start, util.ymdhms(start), end, util.ymdhms(end)))
    result = mpra_lib.age("migr", start=start, end=end, mark=True)
    CrawlConfig.log("found %d migration records in the range" % result)
                
            
        
