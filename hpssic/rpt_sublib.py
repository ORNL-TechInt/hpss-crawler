import CrawlConfig
import dbschem


# -----------------------------------------------------------------------------
def get_last_rpt_time(db):
    """
    Retrieve the last report time from the report table. If the table does not
    exist before make_table ('Created' in result), the table is empty so we
    just return 0 to indicate no last report time.
    """
    result = dbschem.make_table("report")
    if "Created" in result:
        rval = 0
    else:
        rows = db.select(table='report',
                         fields=['max(report_time)'])
        (rval) = rows[0][0]
        if rval is None:
            rval = 0

    CrawlConfig.log("time of last report: %d" % rval)
    return rval
