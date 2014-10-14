# -----------------------------------------------------------------------------
def report_title():
    """
    TCC report title
    """
    return "Tape Copy Checker"


# -----------------------------------------------------------------------------
def recent_records(last_rpt_time, db=None):
    """
    Return recent TCC records
    """
    need_close = False
    if db is None:
        need_close = True
        db = CrawlDBI.DBI(dbtype="crawler")

    rows = db.select(table="tcc_data",
                     fields=['check_time',
                             'low_nsobj_id',
                             'high_nsobj_id',
                             'correct',
                             'error',
                             ],
                     where="? < check_time",
                     data=(last_rpt_time,))

    if need_close:
        db.close()

    return rows


# -----------------------------------------------------------------------------
def distinct_objects(where="", db=None):
    """
    Return distinct objects from tcc_data where correct == 1 or correct <> 1
    """
    need_close = False
    if db is None:
        db = CrawlDBI.DBI(dbtype="crawler")
        need_close = True

    kw = {'table': "tcc_data",
          'fields': ["distinct(low_nsobj_id)"]}
    if where != "":
        kw['where'] = where
        kw['fields'].append("correct")

    rows = db.select(**kw)

    if need_close:
        db.close()

    return rows
