#!/usr/bin/env python
from hpssic import CrawlDBI
import hpss
import optparse
import pdb
from hpssic import pexpect
import toolframe

prefix = "c"
H = None

# -----------------------------------------------------------------------------
def c_cscount(argv):
    """cscount - count checksummed files in log and in database

    Figure out which files logged as checksummed (in hpss_crawl.log) did not
    get checksum set to 1 in the database.
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-f', '--filename',
                 action='store', default='hpss_crawl.log', dest='filename',
                 help='name of log file')
    p.add_option('-v', '--verbose',
                 action='store_true', default=False, dest='verbose',
                 help='pass verbose flag to HSI object')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug: pdb.set_trace()

    result = []
    with open(o.filename, "r") as f:
        for line in f.readlines():
            if line.strip().endswith("checksummed"):
                z = line.strip().split(":")
                if "/" in z[-1]:
                    (path, x) = z[-1].split()
                    result.append(path)
    
    db = CrawlDBI.DBI()
    for path in result:
        rows = db.select(table="checkables",
                         fields=["checksum"],
                         where="path = '%s'" % path)
        print("%d %s" % (rows[0][0], path))

# -----------------------------------------------------------------------------
def c_failtest(argv):
    """failtest - how do NULL values show up when queried?

    After doing 'alter table dev_checkables add fails int', the field fails is
    NULL, not 0. What does that look like when I select a record?
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-f', '--filename',
                 action='store', default='hpss_crawl.log', dest='filename',
                 help='name of log file')
    p.add_option('-v', '--verbose',
                 action='store_true', default=False, dest='verbose',
                 help='pass verbose flag to HSI object')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug: pdb.set_trace()

    db = CrawlDBI.DBI()

    rows = db.select(table='checkables',
                     where="rowid < 10")
    print rows
    db.close()
    
# -----------------------------------------------------------------------------
toolframe.tf_launch(prefix, __name__)
