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
    rval += "Checksum Verifier Population vs. Sample"
    d = Dimension.Dimension(name="cos")
    rval += d.report()
    rval += "\n"

    # retrieve the population and sample size from last time (or set them to 0)
    # db = CrawlDBI.DBI()
    # rows = db.select(table="report",
    #                  fields=["max(report_time)"])
    # max_rpt_time = rows[0] 
    # rows = db.select(table="report",
    #                  fields=["pop_size", "sample_size"],
    #                  where="report_time = ?",
    #                  data=(max_rpt_time,))
    # if len(rows) <= 0:
    #     l_pop_size = l_sample_size = 0
    # else:
    #     (l_pop_size, l_sample_size) = rows[0]
    # 
    # # get the current population and sample size
    rows = db.select(table="checkables",
                     fields=["count(path)"],
                     where='type = "f" and ? < last_check',
                     data=(last_rpt_time,))
    (c_pop_size) = rows[0]
    
    rows = db.select(table="checkables",
                     fields=["count(path)"],
                     where='type = "f" and checksum = 1 and ? < last_check',
                     data=(last_rpt_time,))
    (c_sample_size) = rows[0]

    # report the deltas
    rval += ("Since last report, " +
             "%d items added to population, " % (c_pop_size) +
             "%d items added to sample" % (c_sample_size))
    rval += "\n"

    # # save the current population and sample size
    # 
    # db.insert(table="report",
    #           fields=["report_time", "pop_size", "sample_size"],
    #           data=[(int(time.time()), c_pop_size, c_sample_size)])
                     
    return rval

# -----------------------------------------------------------------------------
def get_mpra_report(db, last_rpt_time):
    # db = CrawlDBI.DBI()
    rows = db.select(table="mpra")
    rval = ("----------------------------------------------------------\n" +
            "Migration/Purge Record Checks\n" +
            "\n")
    body = ""
    for r in rows:
        body += "  %-10s %s\n" % (r[1],
                                time.strftime("%Y.%m%d %H:%M:%S",
                                              time.localtime(r[0])))
        
    if 0 < len(body):
        body = "  %-10s %s\n" % ("Type", "Most Recent Record Found") + body
    else:
        body = "  No records found to report"
        
    rval += body + "\n"
    return rval

# -----------------------------------------------------------------------------
def get_tcc_report(db, last_rpt_time):
    rval = ("----------------------------------------------------------\n" +
            "Tape Copy Checker:\n")

    # db = CrawlDBI.DBI()
    rows = db.select(table="tcc_data")
    (c_tcc_rec) = rows[0]
    
    # db = CrawlDBI.DBI()
    # rows = db.select(table="report",
    #                  fields=["max(report_time)"])
    # max_rpt_time = rows[0] 
    # rows = db.select(table="report",
    #                  fields=["tcc_record"],
    #                  where="report_time=?",
    #                  data=(last_rpt_time,))
    # (l_tcc_rec) = rows[0]

    # rval += ("   Since last report, processed objects %d through %d" %
    #          (l_tcc_rec, c_tcc_rec))
    
    return rval

# -----------------------------------------------------------------------------
def get_report():
    db = CrawlDBI.DBI()
    if not db.table_exists(table="report"):
        last_report_time = make_report_table(db)
    else:
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
              data=[int(time.time()),])

# -----------------------------------------------------------------------------
def get_last_rpt_time(db):
    rows = db.select(table='report',
                     fields=['max(report_time)'])
    (rval) = rows[0][0]
    if rval is None:
        rval = 0
    return rval

# -----------------------------------------------------------------------------
def make_report_table(db):
    db.create(table="report",
              fields=['report_time     integer'])
              # fields=['report_time     integer'
              #         'pop_size        integer',
              #         'sample_size     integer',
              #         'tcc_record      integer'])
    return 0
    
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
    
    
