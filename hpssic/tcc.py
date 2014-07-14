#!/usr/bin/env python
import base64
import crawl_lib
import CrawlConfig
import hpss
import CrawlDBI
import optparse
import os
import pdb
import pexpect
import pprint
import re
import sys
import tcc_lib
import time
import toolframe
import util


# -----------------------------------------------------------------------------
def tccp_bfid(args):
    """bfid - report  a list of bfids

    usage: tcc bfid
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(args)

    if o.debug:
        pdb.set_trace()

    db = CrawlDBI.DBI(dbtype='db2', dbname=CrawlDBI.db2name('subsys'))
    rows = db.select(table='bitfile',
                     fields=['bfid', 'bfattr_cos_id', 'bfattr_create_time'],
                     limit=10)
    for row in rows:
        ct = time.strftime("%Y.%m%d %H:%M:%S",
                           time.localtime(row['BFATTR_CREATE_TIME']))
        print "%s %d %s" % (tcc_lib.hexstr(row['BFID']),
                            row['BFATTR_COS_ID'],
                            ct)


# -----------------------------------------------------------------------------
def tccp_bfpath(args):
    """bfpath - construct a bitfile path from a bitfile id

    usage: tcc bfpath BFID

    E.G.:
     tcc bfpath "x'8484A9A36E02E2119B4910005AFA75BFA2F9616A36FDD01193CB00000' +
                  '0000004'"
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(args)

    if o.debug:
        pdb.set_trace()

    try:
        bitfile = a[0]
    except:
        print("usage: tcc bfpath BITFILE_ID")

    bfpath = tcc_lib.get_bitfile_path(bitfile)
    print(bfpath)


# -----------------------------------------------------------------------------
def tccp_bfts(args):
    """bfts - whats in bftapeseg?

    usage: tcc bfts
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(args)

    if o.debug:
        pdb.set_trace()

    db = CrawlDBI.DBI(dbtype='db2', dbname=CrawlDBI.db2name('subsys'))
    rows = db.select(table='bftapeseg',
                     fields=['bfid', 'storage_class'],
                     limit=20)
    for row in rows:
        print("%s %s" % (tcc_lib.hexstr(row['BFID']),
                         row['STORAGE_CLASS']))


# -----------------------------------------------------------------------------
def tccp_copies_by_cos(args):
    """copies_by_cos - get a list of cos and the copy count for each

    usage: tcc copies_by_cos

    Issues an 'lscos' command to hsi to get copy count information for each
    COS.
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(args)

    if o.debug:
        pdb.set_trace()

    cbc = copies_by_cos()
    for cos in sorted(cbc.keys()):
        print("%s %d" % (cos, cbc[cos]))


# -----------------------------------------------------------------------------
def tccp_copies_by_file(args):
    """copies_by_file - get a list of bitfiles and storage class counts

    usage: tcc copies_by_file

    This will do a left outer join between tables bitfile and bftapeseg.
    Depending on the quantity of data in those tables, this request may take a
    while to run and may use significant temp space in DB2.
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(args)

    if o.debug:
        pdb.set_trace()

    result = copies_by_file()
    for row in result:
        print row


# -----------------------------------------------------------------------------
def tccp_report(args):
    """report - report files with the wrong number of copies

    usage: tcc report

    This will do a left outer join between tables bitfile and bftapeseg.
    Depending on the quantity of data in those tables, this request may take a
    while to run and may use significant temp space in DB2.
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(args)

    if o.debug:
        pdb.set_trace()

    cbc = copies_by_cos()
    cbf = copies_by_file()
    print("%63s %10d %6s %5d" % ("Bitfile ID",
                                 "File Count",
                                 "COS ID",
                                 "COS Count"))
    for file in cbf:
        if int(file['SC_COUNT']) != int(cbc[file['BFATTR_COS_ID']]):
            print("%68s %5d %6s %5d" %
                  (file['BFID'], int(file['SC_COUNT']),
                   file['BFATTR_COS_ID'],
                   int(cbc[file['BFATTR_COS_ID']])))


# -----------------------------------------------------------------------------
def tccp_selbf(args):
    """selbf - select records from bitfile table

    usage: tcc selbf
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(args)

    if o.debug:
        pdb.set_trace()

    record = 0
    db = CrawlDBI.DBI(dbtype='db2', dbname=CrawlDBI.db2name('subsys'))
    rows = db.select(table='bitfile',
                     fields=[])
    for row in rows:
        print("--- record %d ---" % record)
        record += 1
        for k in sorted(row):
            if k == 'BFID':
                print("%s: %s" % (k, tcc_lib.hexstr(row[k])))
            elif k == 'ALLOC_METHOD':
                print("%s: %s" % (k, ord(row[k])))
            elif '_TIME' in k and int(row[k]) != 0:
                print("%s: %s" %
                      (k,
                       time.strftime("%Y.%m%d %H:%M:%S",
                                     time.localtime(int(row[k])))))
            else:
                print("%s: %s" % (k, row[k]))


# -----------------------------------------------------------------------------
def tccp_simplug(args):
    """simplug - run one iteration of the plugin

    usage: tcc simplug
    """
    crawl_lib.simplug('tcc', args)


# -----------------------------------------------------------------------------
def tccp_tables(args):
    """tables - print a list of tables

    usage: tcc tables
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-D', '--db',
                 action='store', default='', dest='dbsect',
                 help='use an alternate database')
    (o, a) = p.parse_args(args)

    if o.debug:
        pdb.set_trace()

    db = CrawlDBI.DBI(dbtype='db2',
                      dbname=CrawlDBI.db2name('subsys'))
    db._dbobj.tbl_prefix = 'syscat.'
    rows = db.select(table='tables',
                     fields=["substr(tabname, 1, 30) as \"Table\"",
                             "substr(tabschema, 1, 30) as \"Schema\"",
                             "type"],
                     where="tabschema = 'HPSS'")
    print("Table                          Schema                         Type")
    for r in rows:
        print("%s %s %s" % (r['Table'], r['Schema'], r['TYPE']))


# -----------------------------------------------------------------------------
def tccp_zreport(args):
    """zreport - show what tcc_report will do with a bitfile id

    usage: tcc zreport NSOBJECT-ID

    Note: This will only report bitfiles where the COS count and file count
    differ. Giving it any old object id won't necessarily generate any output.
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(args)

    if o.debug:
        pdb.set_trace()

    try:
        nsobj_id = a[0]
    except:
        print("usage: tcc zreport OBJECT_ID")
        return

    cfg = CrawlConfig.get_config()
    outfile = cfg.get(tcc_lib.sectname(), 'report_file')

    cosinfo = tcc_lib.get_cos_info()
    bfl = tcc_lib.get_bitfile_set(cfg, int(nsobj_id), 1)
    print("Writing output to %s" % outfile)
    for bf in bfl:
        tcc_lib.tcc_report(bf, cosinfo)


# -----------------------------------------------------------------------------
def copies_by_cos():
    """
    Use hsi to retrieve copy count information for each COS.
    """
    h = hpss.HSI()
    rsp = h.lscos()
    h.quit()
    cbc = {}
    for line in rsp.split("\n"):
        tup = cos_parse(line)
        if tup:
            cbc[tup[0]] = int(tup[1])
    return cbc


# -----------------------------------------------------------------------------
def copies_by_file(limit=10):
    """
    This does a left outer join between tables bitfile and bftapeseg. Depending
    on the quantity of data in those tables, this request may take a while to
    run and may use significant temp space in DB2.
    """
    raise DBIerror("May need a raw sql call to support this")
    # db = CrawlDBI.DBI(dbtype='db2', CrawlDBI.db2name('subsys'))
    # result = db.select(table
    # result = query("""select A.bfid, A.bfattr_cos_id, A.bfattr_create_time,
    #                   count(B.storage_class) as sc_count
    #                   from hpss.bitfile A left outer join hpss.bftapeseg B
    #                   on A.bfid = B.bfid
    #                   where A.bfattr_data_len > 0 and B.bf_offset = 0
    #                   group by A.bfid, A.bfattr_cos_id, A.bfattr_create_time
    #                   fetch first %d rows only""" % limit,
    #                dbsect="subsys")
    # return result


# -----------------------------------------------------------------------------
def cos_parse(line):
    """
    Parse a line returned by "hsi lscos" to extract the COS ID and copy count.
    """
    rgx = "^(\d+)\s+.{30}\s+(\d+)\s+"
    q = re.search(rgx, line)
    if q:
        rval = q.groups()
    else:
        rval = None
    return rval


# -----------------------------------------------------------------------------
def hpss_password():
    """
    Retrieve the password from /var/hpss/etc/mm.keytab.
    """
    filename = "/var/hpss/etc/mm.keytab"
    # os.setgid(9900)
    S = pexpect.spawn("/opt/hpss/config/hpss_mm_keytab -c -f %s" % filename)
    S.expect(pexpect.EOF)
    [q] = re.findall('USER \S+ USING "([^"]+)"', S.before)
    return q


# -----------------------------------------------------------------------------
def hpss_userpass():
    """
    Retrieve the username and password from /var/hpss/etc/mm.keytab.
    """
    filename = "/var/hpss/etc/mm.keytab"
    # os.setgid(9900)
    S = pexpect.spawn("/opt/hpss/config/hpss_mm_keytab -c -f %s" % filename)
    S.expect(pexpect.EOF)
    [(username, password)] = re.findall('USER (\S+) USING "([^"]+)"', S.before)
    return (username, password)

# -----------------------------------------------------------------------------
# toolframe.tf_launch('tccp', __name__)
