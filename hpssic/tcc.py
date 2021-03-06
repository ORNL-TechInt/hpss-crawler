import base64
import crawl_lib
import CrawlConfig
import hpss
import CrawlDBI
import optparse
import os
import pdb
import pprint
import re
import sys
import tcc_lib
import time
import util
import util as U


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

    dbargs = {
        'table': "bitfile",
        'fields': ["bfid", "bfattr_cos_id", "bfattr_create_time"],
        'limit': 10
        }
    rows = tcc_lib.db_select(**dbargs)
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

    dbargs = {
        'table': "bftapeseg",
        'fields': ["bfid", "storage_class"],
        'limit': 20
        }
    rows = tcc_lib.db_select(**dbargs)
    for row in rows:
        print("%s %s" % (tcc_lib.hexstr(row['BFID']),
                         row['STORAGE_CLASS']))


# -----------------------------------------------------------------------------
def tccp_check(args):
    """check - examine the copy count for one or more files

    usage: tcc check [-f FILENAME] [-p PATH] [-o OBJECT_ID] [-b BITFILE_ID]

     -f/--filename    Get the items to check from FILENAME, one per line
     -p/--path        Check the file identified by PATH
     -o/--object      Check the file identified by OBJECT_ID
     -b/--bitfile     CHeck the file identified by BITFILE_ID
    """
    p = optparse.OptionParser()
    p.add_option('-b', '--bitfile',
                 action='append', default=[], dest='bitfile',
                 help='BITFILE to check')
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-f', '--filename',
                 action='append', default=[], dest='file',
                 help='file(s) containing items to check')
    p.add_option('-o', '--object',
                 action='append', default=[], dest='object',
                 help='NSOBJECT IDs to check')
    p.add_option('-p', '--path',
                 action='append', default=[], dest='path',
                 help='PATH to check')
    p.add_option('-v', '--verbose',
                 action='store_true', default=False, dest='verbose',
                 help='report items with correct number of copies')

    (o, a) = p.parse_args(args)

    if o.debug:
        pdb.set_trace()

    for objid in o.object:
        tcc_lib.check_object(objid, verbose=o.verbose, plugin=False)

    for bfid in o.bitfile:
        tcc_lib.check_bitfile(bfid, verbose=o.verbose, plugin=False)

    for path in o.path:
        tcc_lib.check_path(path, verbose=o.verbose, plugin=False)

    for filename in o.file:
        tcc_lib.check_file(filename, verbose=o.verbose, plugin=False)


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
def tccp_nsobj_by_path(args):
    """nsobj_by_path - report the NSOBJECT ids by path

    usage: tcc nsobj_by_path pathname
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(args)

    path = a[0]

    if o.debug:
        pdb.set_trace()

    nl = ['/'] + [z for z in path.split(os.path.sep)][1:]
    parent_id = None
    for name in nl:
        (obj_id, parent_id) = tcc_lib.nsobj_id(name=name, parent=parent_id)
        if parent_id is None:
            parent_id = -1
        print("%10d %10d %-20s" % (parent_id, obj_id, name))
        parent_id = obj_id


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
    dbargs = {'table': 'bitfile',
              'fields': ["BFID",
                         "BFATTR_DATA_LEN",
                         "BFATTR_READ_COUNT",
                         "BFATTR_WRITE_COUNT",
                         "BFATTR_LINK_COUNT",
                         "BFATTR_CREATE_TIME",
                         "BFATTR_MODIFY_TIME",
                         "BFATTR_WRITE_TIME",
                         "BFATTR_READ_TIME",
                         "BFATTR_COS_ID",
                         "BFATTR_NEW_COS_ID",
                         "BFATTR_ACCT",
                         "BFATTR_FLAGS",
                         "BFATTR_STORAGE_SEG_MULT",
                         "BFATTR_CELL_ID",
                         "LEVEL_STATS0_FLAGS",
                         "LEVEL_STATS0_READ_TIME",
                         "LEVEL_STATS0_WRITE_TIME",
                         "LEVEL_STATS0_MIGRATE_TIME",
                         "LEVEL_STATS0_CACHE_TIME",
                         "LEVEL_STATS0_READ_COUNT",
                         "LEVEL_STATS0_WRITE_COUNT",
                         "LEVEL_STATS1_FLAGS",
                         "LEVEL_STATS1_READ_TIME",
                         "LEVEL_STATS1_WRITE_TIME",
                         "LEVEL_STATS1_MIGRATE_TIME",
                         "LEVEL_STATS1_CACHE_TIME",
                         "LEVEL_STATS1_READ_COUNT",
                         "LEVEL_STATS1_WRITE_COUNT",
                         "LEVEL_STATS2_FLAGS",
                         "LEVEL_STATS2_READ_TIME",
                         "LEVEL_STATS2_WRITE_TIME",
                         "LEVEL_STATS2_MIGRATE_TIME",
                         "LEVEL_STATS2_CACHE_TIME",
                         "LEVEL_STATS2_READ_COUNT",
                         "LEVEL_STATS2_WRITE_COUNT",
                         "LEVEL_STATS3_FLAGS",
                         "LEVEL_STATS3_READ_TIME",
                         "LEVEL_STATS3_WRITE_TIME",
                         "LEVEL_STATS3_MIGRATE_TIME",
                         "LEVEL_STATS3_CACHE_TIME",
                         "LEVEL_STATS3_READ_COUNT",
                         "LEVEL_STATS3_WRITE_COUNT",
                         "LEVEL_STATS4_FLAGS",
                         "LEVEL_STATS4_READ_TIME",
                         "LEVEL_STATS4_WRITE_TIME",
                         "LEVEL_STATS4_MIGRATE_TIME",
                         "LEVEL_STATS4_CACHE_TIME",
                         "LEVEL_STATS4_READ_COUNT",
                         "LEVEL_STATS4_WRITE_COUNT",
                         "FAMILY_ID",
                         "ALLOC_METHOD",
                         "MIN_SEG_SIZE",
                         ]
              }
    rows = tcc_lib.db_select(**dbargs)
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
    (o, a) = p.parse_args(args)

    if o.debug:
        pdb.set_trace()

    rows = tcc_lib.table_list()
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
    try:
        bfl = tcc_lib.get_bitfile_set(int(nsobj_id), 1)
    except U.HpssicError as e:
        bfl = []
        pass
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
    dbargs = {
        'table':
            "bitfile A left outer join hpss.bftapeseg B on A.bfid = B.bfid",
        'fields': ["A.bfid", "A.bfattr_cos_id", "A.bfattr_create_time",
                   "count(B.storage_class) as sc_count"],
        'where': "A.bfattr_data_len > 0 and B.bf_offset = 0",
        'groupby': "A.bfid, A.bfattr_cos_id, A.bfattr_create_time",
        'limit': limit
        }
    result = tcc_lib.db_select(**dbargs)
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
