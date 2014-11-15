import CrawlConfig
import CrawlDBI
import dbschem
import Dimension
import email.mime.text
import pdb
import rpt_sublib
import smtplib
import time
import util


# -----------------------------------------------------------------------------
def get_cv_report(db, last_rpt_time):
    """
    Generate the checksum verifier portion of the report
    """
    # get the body of the report from a Dimension object
    rval = "\n" + ("-" * 79) + "\n"
    rval += "Checksum Verifier Population vs. Sample\n"
    if db.table_exists(table="checkables"):
        d = Dimension.Dimension(name="cos")
        rval += d.report()
        rval += "\n"

        # get the population and sample entries added since the last report
        rows = db.select(table="checkables",
                         fields=["count(path)"],
                         where='type = "f" and ? < last_check',
                         data=(last_rpt_time,))
        (c_pop_size) = rows[0]

        rows = db.select(table="checkables",
                         fields=["count(path)"],
                         where='type = "f" and checksum = 1 and ' +
                               '? < last_check',
                         data=(last_rpt_time,))
        (c_sample_size) = rows[0]

        # report the deltas
        rval += ("Since last report, " +
                 "%d items added to population, " % (c_pop_size) +
                 "%d items added to sample" % (c_sample_size))
        rval += "\n"

        c = Dimension.Dimension(name='ttypes')
        rval += c.report()
        rval += "\n"

        # get the population and sample entries added since the last report
        rows = db.select(table="checkables",
                         fields=["count(path)"],
                         where='type = "f" and ' +
                               'ttypes is not null and ' +
                               '? < last_check',
                         data=(last_rpt_time,))
        (c_pop_size) = rows[0]

        rows = db.select(table="checkables",
                         fields=["count(path)"],
                         where='type = "f" and ttypes is not null and ' +
                               'checksum = 1 and ? < last_check',
                         data=(last_rpt_time,))
        (c_sample_size) = rows[0]

        # report the deltas
        rval += ("Since last report, " +
                 "%d items added to population, " % (c_pop_size) +
                 "%d items added to sample" % (c_sample_size))
    else:
        rval += "   No checksum verifier data to report."

    rval += "\n"

    return rval


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


# -----------------------------------------------------------------------------
def get_mpra_report(db=None, last_rpt_time=0):
    """
    Generate the MPRA portion of the report
    """
    close = False
    if db is None:
        db = CrawlDBI.DBI(dbtype="crawler")
        close = True
    rval = "\n" + ("-" * 79) + "\n"
    rval += "Migration/Purge Record Checks\n\n"
    hfmt = "   %-5s  %-20s  %-20s  %-20s  %8s\n"
    bfmt = "   %-5s  %-20s  %-20s  %-20s  %8d\n"
    body = ''
    mdelta = 0
    pdelta = 0
    if db.table_exists(table='mpra'):
        rows = db.select(table="mpra",
                         fields=['type',
                                 'scan_time',
                                 'start_time',
                                 'end_time',
                                 'hits',
                                 ],
                         where="? < scan_time",
                         data=(last_rpt_time,),
                         orderby="type")
        for r in rows:
            if r[0] == 'migr':
                start = "beginning of time" if r[2] == 0 else util.ymdhms(r[2])
                end = util.ymdhms(r[3])
                mdelta += int(r[4])
            else:
                start = '...'
                end = '...'
            body += bfmt % (r[0],
                            util.ymdhms(r[1]),
                            start,
                            end,
                            r[4])

        if 0 < len(body):
            body = (hfmt % ('Type', 'Scan Time', 'Start', 'End', 'Records') +
                    body)
        else:
            body = "   No records found to report"

        delta = sum([x[4] for x in rows])
        rows = db.select(table="mpra",
                         fields=["type", "sum(hits)"],
                         groupby="type")
        total = {}
        for r in rows:
            total[r[0]] = int(r[1])
        body += "\n\n         %s Migration            Purge\n" % (" " * 20)
        body += ("    Since %-18s %10d       %10d\n" %
                 (util.ymdhms(last_rpt_time), mdelta, pdelta))
        body += ("    Total                    %10d       %10d\n" %
                 (total['migr'], total['purge']))

    else:
        body = "   No MPRA result to report at this time."

    rval += body + "\n"

    if close:
        db.close()
    return rval


# -----------------------------------------------------------------------------
def get_report():
    """
    Generate and return a text report
    """
    db = CrawlDBI.DBI(dbtype="crawler")

    last_report_time = rpt_sublib.get_last_rpt_time(db)
    report = get_cv_report(db, last_report_time)
    report += get_mpra_report(db, last_report_time)
    report += get_tcc_report(db, last_report_time)
    set_last_rpt_time(db)

    db.close()
    return report


# -----------------------------------------------------------------------------
def get_tcc_report(db, last_rpt_time):
    """
    Generate the TCC portion of the report
    """
    rval = "\n" + ("-" * 79) + "\n"
    rval += "Tape Copy Checker:\n\n"

    if db.table_exists(table='tcc_data'):
        checks = correct = error = 0
        rows = db.select(table="tcc_data",
                         fields=['check_time',
                                 'low_nsobj_id',
                                 'high_nsobj_id',
                                 'correct',
                                 'error',
                                 ],
                         where="? < check_time",
                         data=(last_rpt_time,))

        for (t, l, h, c, e) in rows:
            checks += (h - l + 1)
            correct += c
            error += e

        rows = db.select(table="tcc_data",
                         fields=["distinct(low_nsobj_id)", ])
        t_check = len(rows)

        rows = db.select(table="tcc_data",
                         fields=["distinct(low_nsobj_id)", "correct"],
                         where="correct = 1")
        t_correct = len(rows)
        c_obj_id_l = [x[0] for x in rows]

        t_error = 0
        erows = db.select(table="tcc_data",
                          fields=["distinct(low_nsobj_id)", "correct"],
                          where="correct <> 1")
        for r in erows:
            if r[0] not in c_obj_id_l:
                t_error += 1

        rval += " %s Checked   Correct  Errors\n" % (" " * 29)
        rval += ("   Since %-18s:    %6d   %6d   %6d\n" %
                 (util.ymdhms(last_rpt_time), checks, correct, error))
        rval += ("   Total: %s %6d   %6d   %6d\n" % (" " * 21,
                                                     t_check,
                                                     t_correct,
                                                     t_error))
    else:
        rval += ("   No Tape Copy Checker results to report")

    return rval


# -----------------------------------------------------------------------------
def set_last_rpt_time(db):
    """
    Record the current time as the time of the last report generated
    """
    db.insert(table='report',
              fields=['report_time'],
              data=[(int(time.time()),)])
