#!/usr/bin/env python
import base64
import CrawlConfig
import hpss
import ibm_db as db2
import ibm_db_dbi as db2dbi
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
    """
    for row in query("select bfid, bfattr_cos_id, bfattr_create_time from bitfile"):
        ct = time.strftime("%Y.%m%d %H:%M:%S",
                           time.localtime(row['BFATTR_CREATE_TIME']))
        print "%s %d %s" % (hexstr(row['BFID']),
                            row['BFATTR_COS_ID'],
                            ct)

# -----------------------------------------------------------------------------
def tcc_bfts(args):
    """bfts - whats in bftapeseg?
    """
    for row in query("select bfid, storage_class from bftapeseg"):
        print("%s" % hexstr(row['BFID']))
        
# -----------------------------------------------------------------------------
def tcc_copies_by_cos(args):
    """copies_by_cos - get a list of cos and the copy count for each
    """
    cbc = copies_by_cos()
    for cos in sorted(cbc.keys()):
        print("%s %d" % (cos, cbc[cos]))

# -----------------------------------------------------------------------------
def tcc_copies_by_file(args):
    """copies_by_file - get a list of bitfiles and storage class counts
    """
    result = copies_by_file()
    for row in result:
        print row

# -----------------------------------------------------------------------------
def tcc_selbf(args):
    """selbf - select records from bitfile table
    """
    for row in query("select * from bitfile"):
        print row

# -----------------------------------------------------------------------------
def tcc_sql(args):
    """sql - run arbitrary sql
    """
    sql = " ".join(args)
    print sql
    for row in query(sql):
        print row

# -----------------------------------------------------------------------------
def tcc_tables(args):
    """tables - print a list of tables
    """
    # pdb.set_trace()
    db = db2.connect('subsys', 'hpss', hpss_password())
    cxn = db2dbi.Connection(db)
    x = cxn.tables('HPSS', '%')
    pprint.pprint(x)
    cxn.close()
    # db.close()
    
# -----------------------------------------------------------------------------
def copies_by_cos():
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
    result = query("select A.bfid, A.bfattr_cos_id, A.bfattr_create_time, " +
                   "count(B.storage_class) " +
                   "from hpss.bitfile A left outer join hpss.bftapeseg B " +
                   "on A.bfid = B.bfid " +
                   "where A.bfattr_data_len > 0 and B.bf_offset = 0 " +
                   "group by A.bfid, A.bfattr_cos_id, A.bfattr_create_time")
    return result

# -----------------------------------------------------------------------------
def cos_parse(line):
    rgx = "^(\d+)\s+.{30}\s+(\d+)\s+"
    q = re.search(rgx, line)
    if q:
        rval = q.groups()
    else:
        rval = None
    return rval

# -----------------------------------------------------------------------------
def hexstr(bfid):
    rval = "x'"
    for c in list(bfid):
        rval += "%02x" % ord(c)
    rval += "'"
    return rval

# -----------------------------------------------------------------------------
def hpss_password():
    filename = "/var/hpss/etc/mm.keytab"
    # os.setgid(9900)
    S = pexpect.spawn("/opt/hpss/config/hpss_mm_keytab -c -f %s" % filename)
    S.expect(pexpect.EOF)
    [q] = re.findall('USER \S+ USING "([^"]+)"', S.before)
    return q

# -----------------------------------------------------------------------------
def query(sql):
    db = db2.connect('subsys', 'hpss', hpss_password())
    r = db2.exec_immediate(db, sql)
    rval = []
    x = db2.fetch_assoc(r)
    while (x):
        rval.append(x)
        x = db2.fetch_assoc(r)
    return rval

# -----------------------------------------------------------------------------
toolframe.tf_launch('tcc', __name__)
