import CrawlConfig
import CrawlDBI
import Dimension
import email.mime.text
import pdb
import smtplib
import time
import util


# -----------------------------------------------------------------------------
def get_cv_report(db, last_rpt_time):
    # get the body of the report from a Dimension object
    rval = "----------------------------------------------------------\n"
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

        c = Dimension.Dimension(name='cart')
        rval += c.report()
        rval += "\n"

        # get the population and sample entries added since the last report
        rows = db.select(table="checkables",
                         fields=["count(path)"],
                         where='type = "f" and ' +
                               'cart is not null and ' +
                               '? < last_check',
                         data=(last_rpt_time,))
        (c_pop_size) = rows[0]

        rows = db.select(table="checkables",
                         fields=["count(path)"],
                         where='type = "f" and cart is not null and ' +
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
def get_mpra_report(db=None, last_rpt_time=0):
    if db is None:
        db = CrawlDBI.DBI(dbtype="crawler")
        close = True
    rval = ("\n----------------------------------------------------------\n" +
            "Migration/Purge Record Checks\n" +
            "\n")
    hfmt = "   %-5s  %-20s  %-20s  %-20s  %8s\n"
    bfmt = "   %-5s  %-20s  %-20s  %-20s  %8d\n"
    body = ''
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

    else:
        body = "   No MPRA result to report at this time."

    rval += body + "\n"

    if close:
        db.close()
    return rval


# -----------------------------------------------------------------------------
def get_tcc_report(db, last_rpt_time):
    rval = ("\n----------------------------------------------------------\n" +
            "Tape Copy Checker:\n")

    # db = CrawlDBI.DBI(dbtype="crawler")
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

        rval += ("   Since %-18s, files checked: %7d\n" %
                 (util.ymdhms(last_rpt_time), checks) +
                 "%-34s correct: %7d\n" % (' ', correct) +
                 "%-34s  errors: %7d\n" % (' ', error))
    else:
        rval += ("   No Tape Copy Checker results to report")

    return rval


# -----------------------------------------------------------------------------
def get_report():
    db = CrawlDBI.DBI(dbtype="crawler")

    last_report_time = get_last_rpt_time(db)
    report = get_cv_report(db, last_report_time)
    report += get_mpra_report(db, last_report_time)
    report += get_tcc_report(db, last_report_time)
    set_last_rpt_time(db)
    return report


# -----------------------------------------------------------------------------
def set_last_rpt_time(db):
    db.insert(table='report',
              fields=['report_time'],
              data=[(int(time.time()),)])


# -----------------------------------------------------------------------------
def get_last_rpt_time(db):
    if not db.table_exists(table="report"):
        db.create(table="report",
                  fields=['report_time     integer'])
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
def sendmail(sender='',
             to='',
             subject='',
             body=''):
    """
    Send an e-mail to the addresses listed in to.
    """
    if to.strip() == '':
        raise StandardError("Can't send mail without at least one recipient")
    if '@' not in to:
        raise StandardError("'%s' does not look like an e-mail address" % to)

    if sender.strip() == '':
        raise StandardError("Can't send mail without a sender")

    if subject.strip() == '':
        subject = "<empty subject>"

    if body.strip() == '':
        body = "<empty body>"

    hostname = util.hostname()
    payload = email.mime.text.MIMEText(body)
    payload['Subject'] = subject
    payload['From'] = sender
    payload['To'] = to
    CrawlConfig.log("Sending mail from %s to %s." % (sender, to))
    s = smtplib.SMTP('localhost')
    s.sendmail(sender, to, payload.as_string())
    s.quit()
