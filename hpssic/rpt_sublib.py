import CrawlConfig
import CrawlDBI
import cv_sublib


# -----------------------------------------------------------------------------
def cos_description(name):
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
