import CrawlDBI
import dbschem
import hpss
import messages as MSG


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
    st = dbschem.make_table("lscos")
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
            (cos, desc, copies, lo, hi) = (line[0:4],
                                           line[5:34].strip(),
                                           int(line[36:44].strip()),
                                           line[60:].split('-')[0],
                                           line[60:].split('-')[1])
            lo_i = lo.replace(',', '')
            hi_i = hi.replace(',', '')
            data.append((cos, desc, copies, lo_i, hi_i))

        tabname = 'lscos'
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
