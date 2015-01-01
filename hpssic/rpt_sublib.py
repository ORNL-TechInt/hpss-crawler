import CrawlConfig
import CrawlDBI
import cv_sublib


# -----------------------------------------------------------------------------
def cos_description(name):
    """
    Look up and return the description of cos *name*
    """
    db = CrawlDBI.DBI(dbtype='crawler')
    if not db.table_exists(table='lscos'):
        cv_sublib.lscos_populate()

    r = db.select(table='lscos',
                  fields=['name'],
                  where="name = ?",
                  data=(name,))
    if 0 < len(r):
        rval = r[0][0]
    else:
        rval = "???"
    db.close()

    return rval


# -----------------------------------------------------------------------------
def load_history():
    """
    Read the contents of table pfx_report and load the report times into table
    pfx_history as run times for the report plugin.
    """
    db = CrawlDBI.DBI(dbtype='crawler')
    rows = db.select(table='report',
                     fields=['report_time'])
    insert_data = [('report', x[0], 0) for x in rows]
    db.insert(table='history',
              ignore=True,
              fields=['plugin', 'runtime', 'errors'],
              data=insert_data)
    db.close()
