#!/usr/bin/env python
import base64
import CrawlConfig
import hpss
import ibm_db as db2
import ibm_db_dbi as db2dbi
import optparse
import os
import pdb
import pexpect
import pprint
import re
import sys
import time
import toolframe

# -----------------------------------------------------------------------------
def tcc_bfid(args):
    """bfid - report  a list of bfids

    usage: tcc bfid
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(args)

    if o.debug: pdb.set_trace()
    
    for row in query("select bfid, bfattr_cos_id, bfattr_create_time from bitfile"):
        ct = time.strftime("%Y.%m%d %H:%M:%S",
                           time.localtime(row['BFATTR_CREATE_TIME']))
        print "%s %d %s" % (hexstr(row['BFID']),
                            row['BFATTR_COS_ID'],
                            ct)

# -----------------------------------------------------------------------------
def tcc_bfts(args):
    """bfts - whats in bftapeseg?

    usage: tcc bfts
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(args)

    if o.debug: pdb.set_trace()
    
    for row in query("select bfid, storage_class from bftapeseg"):
        print("%s %s" % (hexstr(row['BFID']), row['STORAGEE_CLASS']))
        
# -----------------------------------------------------------------------------
def tcc_copies_by_cos(args):
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

    if o.debug: pdb.set_trace()
    
    cbc = copies_by_cos()
    for cos in sorted(cbc.keys()):
        print("%s %d" % (cos, cbc[cos]))

# -----------------------------------------------------------------------------
def tcc_copies_by_file(args):
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

    if o.debug: pdb.set_trace()
    
    result = copies_by_file()
    for row in result:
        print row

# -----------------------------------------------------------------------------
def tcc_report(args):
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

    if o.debug: pdb.set_trace()
    
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
def tcc_selbf(args):
    """selbf - select records from bitfile table

    usage: tcc selbf
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(args)

    if o.debug: pdb.set_trace()
    
    record = 0
    for row in query("select * from bitfile"):
        print("--- record %d ---" % record)
        record += 1
        for k in sorted(row):
            if k == 'BFID':
                print("%s: %s" % (k, hexstr(row[k])))
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
def tcc_sql(args):
    """sql - run arbitrary sql

    usage: tcc sql <sql statement>
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-D', '--dbname',
                 action='store', default='', dest='dbname',
                 help='use an alternate database')
    (o, a) = p.parse_args(args)

    if o.debug: pdb.set_trace()
    
    sql = " ".join(a)
    print sql
    for row in query(sql, dbname=o.dbname):
        pprint.pprint(row)

# -----------------------------------------------------------------------------
def tcc_tables(args):
    """tables - print a list of tables

    usage: tcc tables
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(args)

    if o.debug: pdb.set_trace()
    
    db = db2.connect('subsys', 'hpss', hpss_password())
    cxn = db2dbi.Connection(db)
    x = cxn.tables('HPSS', '%')
    pprint.pprint(x)
    cxn.close()
    
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
def copies_by_file():
    """
    This does a left outer join between tables bitfile and bftapeseg. Depending
    on the quantity of data in those tables, this request may take a while to
    run and may use significant temp space in DB2.
    """
    result = query("select A.bfid, A.bfattr_cos_id, A.bfattr_create_time, " +
                   "count(B.storage_class) as sc_count " +
                   "from hpss.bitfile A left outer join hpss.bftapeseg B " +
                   "on A.bfid = B.bfid " +
                   "where A.bfattr_data_len > 0 and B.bf_offset = 0 " +
                   "group by A.bfid, A.bfattr_cos_id, A.bfattr_create_time")
    return result

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
def hexstr(bfid):
    """
    Convert a raw bitfile id into a hexadecimal string as presented by DB2.
    """
    rval = "x'"
    for c in list(bfid):
        rval += "%02x" % ord(c)
    rval += "'"
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
def query(sql, dbname=''):
    """
    Connect to a DB2 database, run an sql command (assumed to be a select), and
    return the result.
    """
    if dbname == '':
        dbname = 'SUBSYS'
    (username, password) = hpss_userpass()
    db = db2.connect(dbname, username, password)
    r = db2.exec_immediate(db, sql)
    rval = []
    x = db2.fetch_assoc(r)
    while (x):
        rval.append(x)
        x = db2.fetch_assoc(r)
    return rval

# -----------------------------------------------------------------------------
toolframe.tf_launch('tcc', __name__)
