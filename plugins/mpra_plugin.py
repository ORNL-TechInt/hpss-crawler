#!/usr/bin/env python
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
    # if 0 < len(result):
    #     with open(rptfile, 'a') as f:
    #         f.write("Migration Records Older Than %s\n" % age)
    #         f.write("%-67s %-18s %s\n" % ("BFID", "Created", "MigrFails"))
    #         for row in result:
    #             f.write("%s %s %d\n" % (CrawlDBI.DBIdb2.hexstr(row['BFID']),
    #                                     util.ymdhms(row['RECORD_CREATE_TIME']),
    #                                     row['MIGRATION_FAILURE_COUNT']))
                
    result = mpra_lib.age("purge", age)
    # if 0 < len(result):
    #     with open(rptfile, 'a') as f:
    #         f.write("Purge Records Older Than %s" % age)
    #         f.write("%-67s %-18s\n" % ("BFID", "Created"))
    #         for row in result:
    #             f.write("%s %s %d\n" % (CrawlDBI.DBIdb2.hexstr(row['BFID']),
    #                                     util.ymdhms(row['RECORD_CREATE_TIME'])))
                
            
        
