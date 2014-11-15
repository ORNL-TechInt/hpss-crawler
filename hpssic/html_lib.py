import CrawlConfig
import CrawlDBI
import cv_sublib
import Dimension
import mpra_sublib
import pdb
import rpt_lib
import tcc_sublib
import time
import util
import version


# -----------------------------------------------------------------------------
def get_html_report(cfg_file):
    """
    Format a report in HTML
    """
    rval = ""
    cfg = CrawlConfig.get_config(cfg_file)
    db = CrawlDBI.DBI(dbtype="crawler")

    last_rpt_time = rpt_lib.get_last_rpt_time(db)
    rval += ('<head><meta http-equiv="refresh" content="60">\n')
    rval += ("<title>HPSSIC Dashboard</title></head>")
    rval += ("<body><center><h1>HPSS Integrity Crawler Dashboard</h1>" +
             "<br><h4>Version %s</h4>" % version.__version__ +
             "</center>\n")
    rval += ("Report generated at %s\n" % time.strftime("%Y.%m%d %H:%M:%S"))
    rval += ("<br>Based on data from %s\n" %
             time.strftime("%Y.%m%d %H:%M:%S", time.localtime(last_rpt_time)))
    rval += get_html_cv_report(db, last_rpt_time)
    rval += get_html_mpra_report(db, last_rpt_time)
    rval += get_html_tcc_report(db, last_rpt_time)
    rval += "</body>"
    db.close()

    return rval


# -----------------------------------------------------------------------------
def get_html_cv_report(db, last_rpt_time):
    """
    Format the CV report in HTML
    """
    rval = ""
    if not db.table_exists(table="checkables"):
        return rval

    rval += ("<h2>%s</h2>\n" % cv_sublib.report_title())
    diml = [{'name': 'cos',
             'pop': "type = 'f' and ? < last_check",
             'samp': "type = 'f' and checksum = 1 and ? < last_check"},
            {'name': 'ttypes',
             'pop': "type = 'f' and ttypes is not null and ? < last_check",
             'samp': "type = 'f' and ttypes is not null " +
             "and checksum = 1 and ? < last_check"}]

    for dim in diml:
        d = Dimension.Dimension(name=dim['name'])
        rval += "<pre>\n"
        rval += d.report()
        rval += "</pre>\n<br>"

        # get the population and sample entries added since the last report
        rows = db.select(table="checkables",
                         fields=["count(path)"],
                         where=dim['pop'],
                         data=(last_rpt_time,))
        (c_pop_size) = rows[0]

        rows = db.select(table="checkables",
                         fields=["count(path)"],
                         where=dim['samp'],
                         data=(last_rpt_time,))
        (c_sample_size) = rows[0]

        # report the deltas
        rval += ("Since the last report, " +
                 "%d items were added to population, " % (c_pop_size) +
                 "%d items were added to sample" % (c_sample_size))
        rval += "\n"

    return rval


# -----------------------------------------------------------------------------
def get_html_mpra_report(db, last_rpt_time):
    """
    Format the MPRA report in HTML
    """
    if not db.table_exists(table="mpra"):
        return ""

    rval = ("<h2>%s</h2>\n" % mpra_sublib.report_title())
    body = ''
    hfmt = "   %-5s  %-20s  %-20s  %-20s  %8s\n"
    bfmt = "   %-5s  %-20s  %-20s  %-20s  %8d\n"
    mdelta = 0
    pdelta = 0
    rows = mpra_sublib.recent_records(last_rpt_time, db=db)
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

    rval += "<pre>\n" + body + "\n</pre>\n<br>\n"

    return rval


# -----------------------------------------------------------------------------
def get_html_tcc_report(db, last_rpt_time):
    """
    Format the TCC report in HTML
    """
    rval = ""
    if not db.table_exists(table='tcc_data'):
        return rval

    rval = ("<h2>%s</h2>\n" % tcc_sublib.report_title())

    checks = correct = error = 0

    rows = tcc_sublib.recent_records(last_rpt_time, db=db)
    for (t, l, h, c, e) in rows:
        checks += (h - l + 1)
        correct += c
        error += e

    rows = tcc_sublib.distinct_objects(db=db)
    t_check = len(rows)

    rows = tcc_sublib.distinct_objects(db=db,
                                       where="correct = 1")
    t_correct = len(rows)
    c_obj_id_l = [x[0] for x in rows]

    t_error = 0
    erows = tcc_sublib.distinct_objects(db=db,
                                        where="correct <> 1")
    for r in erows:
        if r[0] not in c_obj_id_l:
            t_error += 1

    rval += "<pre>\n"
    rval += " %s Checked   Correct  Errors\n" % (" " * 29)
    rval += ("   Since %-18s:    %6d   %6d   %6d\n" %
             (util.ymdhms(last_rpt_time), checks, correct, error))
    rval += ("   Total: %s %6d   %6d   %6d\n" % (" " * 21,
                                                 t_check,
                                                 t_correct,
                                                 t_error))
    rval += "</pre>\n"

    return rval
