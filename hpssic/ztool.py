#!/usr/bin/env python
import glob
from hpssic import CrawlDBI
import hpss
import optparse
import os
import pdb
import pexpect
import toolframe
from hpssic import util as U

prefix = "c"
H = None


# -----------------------------------------------------------------------------
def c_mkhooks(argv):
    """mkhooks - create a symlink in .git/hooks for each of githooks/*

    Install repo-specific git hooks
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    try:
        (o, a) = p.parse_args(argv)
    except SystemExit:
        return

    if o.debug:
        pdb.set_trace()

    if not os.path.isdir(".git"):
        print("We don't appear to be in the root of a git repo")
        sys.exit(1)

    srcdir = U.abspath(".git/hooks")
    for target in glob.glob(U.abspath("githooks/*")):
        base = U.basename(target)
        src = U.pathjoin(srcdir, base)
        if os.path.exists(src):
            print("%s already exists" % src)
        else:
            os.symlink(target, U.pathjoin(srcdir, base))



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

    if o.debug:
        pdb.set_trace()

    if o.filename:
        filename = o.filename
    else:
        cfg = CrawlConfig.get_config()
        filename = cfg.get('crawler', 'logpath')

    result = []
    with open(filename, "r") as f:
        for line in f.readlines():
            if line.strip().endswith("checksummed"):
                z = line.strip().split(":")
                if "/" in z[-1]:
                    (path, x) = z[-1].split()
                    result.append(path)

    for path in result:
        cs = cv_lib.lookup_checksum_by_path(path)
        print("%d %s" % (cs, path))


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

    if o.debug:
        pdb.set_trace()

    rows = cv_lib.lookup_nulls()
    print rows
