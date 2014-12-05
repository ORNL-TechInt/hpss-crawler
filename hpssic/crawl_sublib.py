import CrawlDBI
import dbschem


# -----------------------------------------------------------------------------
def record_history(name, when, errors):
    """
    Record a plugin name and runtime in the history table
    """
    db = CrawlDBI.DBI(dbtype='crawler')
    if not db.table_exists(table='history'):
        dbschem.make_table('history')
    db.insert(table='history',
              fields=['plugin', 'runtime', 'errors'],
              data=[(name, when, errors)])
    db.close()
