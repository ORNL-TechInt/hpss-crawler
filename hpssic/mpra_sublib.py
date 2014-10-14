import CrawlDBI


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
