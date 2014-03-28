#!/usr/bin/env python
import CrawlConfig
import mpra_lib

# -----------------------------------------------------------------------------
def main(cfg):
    """
    Migration Purge Record Ager (mpra) reads the database tables BFMIGRREC and
    BFPURGEREC and reports migration and purge records that are older than the
    age specified in the configuration.
    """
    cfg = CrawlConfig.get_config()
    age = cfg.get('mpra', 'age')

    result = mpra_lib.age("migr", age)
                
    result = mpra_lib.age("purge", age)
                
            
        
