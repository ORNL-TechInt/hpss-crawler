import CrawlDBI
import dbschem
import hpss
import messages as MSG
import pdb
import util as U


# -----------------------------------------------------------------------------
def load_history(filename):
    """
    Read log file *filename* and create records in table pfx_history
    corresponding to each time the cv plugin was run.

    Line containing 'cv_plugin' and 'firing up' indicates run time.

    Subsequent line containing 'cv_plugin' and 'failures: %d' indicate errors
    for the run.
    """
    # -------------------------------------------------------------------------
    def cv_fires(line):
        """
        Parse *line* to decide whether it indicates a firing of the cv plugin.
        """
        return all([runtime is None,
                    error is None,
                    'cv_plugin' in line or 'checksum-verifier' in line,
                    'firing up' in line])

    # -------------------------------------------------------------------------
    def cv_completes(line):
        """
        Parse *line* to decide whether it indicates completion of a firing of
        the cv plugin.
        """
        return all([runtime is not None,
                    error is None,
                    'cv_plugin' in line or 'checksum-verifier' in line,
                    'failures:' in line,
                    'totals' not in line])

    # -------------------------------------------------------------------------
    db = CrawlDBI.DBI(dbtype='crawler')
    runtime = error = None
    with open(filename, 'r') as f:
        for line in f:
            if cv_fires(line):
                runtime = U.epoch(line[0:18])
            if cv_completes(line):
                error = int(U.rgxin('failures: (\d+)', line))
            if runtime is not None and error is not None:
                db.insert(table='history',
                          ignore=True,
                          fields=['plugin', 'runtime', 'errors'],
                          data=[('cv', runtime, error)])
                runtime = error = None
    db.close()


# -----------------------------------------------------------------------------
def lscos_populate():
    """
    If table lscos already exists, we're done. Otherwise, retrieve the lscos
    info from hsi, create the table, and fill the table in.

    We store the min_size and max_size for each COS as text strings containing
    digits because the largest sizes are already within three orders of
    magnitude of a mysql bigint and growing.
    """
    db = CrawlDBI.DBI(dbtype="crawler")
    tabname = 'lscos'
    st = dbschem.make_table(tabname)
    szrgx = "(\d+([KMGT]B)?)"
    rgx = ("\s*(\d+)\s*(([-_a-zA-Z0-9]+\s)+)\s+[UGAN]*\s+(\d+)" +
           "\s+(ALL)?\s+%s\s+-\s+%s" % (szrgx, szrgx))
    if "Created" == st:
        H = hpss.HSI()
        raw = H.lscos()
        H.quit()

        z = [x.strip() for x in raw.split('\r')]
        rules = [q for q in z if '----------' in q]
        first = z.index(rules[0]) + 1
        second = z[first:].index(rules[0]) + first
        lines = z[first:second]
        data = []
        for line in lines:
            m = U.rgxin(rgx, line)
            (cos, desc, copies, lo_i, hi_i) = (m[0],
                                               m[1].strip(),
                                               m[3],
                                               U.scale(m[5], kb=1024),
                                               U.scale(m[7], kb=1024))
            data.append((cos, desc, copies, lo_i, hi_i))

        db.insert(table=tabname,
                  fields=['cos', 'name', 'copies', 'min_size', 'max_size'],
                  data=data)
        rval = MSG.table_created_S % tabname
    else:
        rval = MSG.table_already_S % tabname

    db.close()
    return rval


# -----------------------------------------------------------------------------
def report_title():
    """
    CV report title
    """
    return "Checksum Verifier Population vs. Sample"
