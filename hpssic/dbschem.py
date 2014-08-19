import CrawlDBI

tdefs = {
    'tape_types': {'fields': ["type int",
                              "subtype int",
                              "name text"]
                   },
    }

def make_table(tabname):
    """
    Make the indicated table if it does not exist
    !@! test this
    """
    db = CrawlDBI.DBI(type='crawler')
    if not db.table_exists(table=tabname):
        db.create(table=tabname, fields=tdefs[tabname]['fields'])
    db.close()

