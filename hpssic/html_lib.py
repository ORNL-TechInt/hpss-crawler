
# -----------------------------------------------------------------------------
def get_report():
    """
    Format a report in HTML
    """
    db = CrawlDBI.DBI(dbtype="crawler")
    db.close()

