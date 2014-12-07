import CrawlDBI


# -----------------------------------------------------------------------------
def load_history():
    """
    Read the contents of table pfx_mpra and load the unique scan_times
    into table pfx_history as mpra runtimes.
    """
    db = CrawlDBI.DBI(dbtype='crawler')
    rows = db.select(table='mpra',
                     fields=['type', 'scan_time', 'hits'])
    db.insert(table='history',
              ignore=True,
              fields=['plugin', 'runtime', 'errors'],
              data=list(rows))
    db.close()


# -----------------------------------------------------------------------------
def report_title():
    """
    MPRA report title
    """
    return "Migration/Purge Record Checks"


# -----------------------------------------------------------------------------
def recent_records(last_rpt_time, db=None):
    """
    Return mpra records scanned since the last report time
    """
    need_close = False
    if db is None:
        db = CrawlDBI.DBI(dbtype="crawler")
        need_close = True

    rows = db.select(table="mpra",
                     fields=['type',
                             'scan_time',
                             'start_time',
                             'end_time',
                             'hits'],
                     where="? < scan_time",
                     data=(last_rpt_time,),
                     orderby="type")

    if need_close:
        db.close()

    return rows
