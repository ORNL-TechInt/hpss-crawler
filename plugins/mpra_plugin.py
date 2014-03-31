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
    util.log("searching for migration records betweeen %d and %d" %
             (start, end))
    result = mpra_lib.age("migr", start=start, end=end, mark=True)
    util.log("found %d migration records in the range" % result)
    
    start = mpra_lib.mpra_fetch_recent("purge")
    util.log("searching for purge records betweeen %d and %d" %
             (start, end))
    result = mpra_lib.age("purge", start=start, end=end, mark=True)
    util.log("found %d purge records in the range" % result)
                
            
        
