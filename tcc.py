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
import tcc_common
import time
import toolframe

# -----------------------------------------------------------------------------
def tccp_remote(args):
    """remote - test connecting to a remote database

    usage: tcc remote -C <config-file>
                      -c <config-section>
                      -h <hostname>
                      -D <database>
                      -p <port>
                      -u <username>
                      -P <password>
                      -s <optional sql statement>
    """
    p = optparse.OptionParser()

    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-s', '--sql',
                 action='store', default='', dest='sql',
                 help='optional sql statement')
    p.add_option('-D', '--database',
                 action='store', dest='database',
                 help='name of database to connect')

    cg = optparse.OptionGroup(p, "config",
                              "-C and -c take the connection info from "
                              "a configuration file. They are not compatible "
                              "with the cmdline options")
    cg.add_option('-C', '--configfile',
                 action='store', dest='cfgfile',
                 help='which config file to use')
    cg.add_option('-c', '--config',
                 action='store', dest='cfgsect',
                 help='which config file section to use')
    p.add_option_group(cg)

    cl = optparse.OptionGroup(p, "cmdline",
                              "These options allow you to specify the "
                              "database connection information on the "
                              "command line. They are not compatible with "
                              "the config options")
    cl.add_option('-H', '--hostname',
                 action='store', dest='hostname',
                 help='name of host to connect')
    cl.add_option('-p', '--port',
                 action='store', dest='port',
                 help='TCP port num to use')
    cl.add_option('-u', '--username',
                 action='store', dest='username',
                 help='DB2 username')
    cl.add_option('-P', '--password',
                 action='store', dest='password',
                 help='DB2 password')
    p.add_option_group(cl)
    
    (o, a) = p.parse_args(args)

    if o.debug: pdb.set_trace()

    # at this point, all the args in one group or the other should be set. To
    # check this, I need a count of the options set in each group. One should
    # be 0, the other should be non-zero, and I need to be able to tell which
    # group has the non-zero count.

    # get a list of the attributes of o that represent specified arguments
    s = [x for x in dir(o) if getattr(o, x) is not None and
         not callable(getattr(o, x)) and
         not x.startswith('_')]

    # the following gives us a list of lists of n occurrences of the group
    # title where n is the number of options from that group that were actually
    # specified on the command line. The number of elements in this list of
    # lists should be 1 since only options from one group or the other should
    # be specified.
    gl = [l for l in [[g.title for x in s if x in
                         [n.dest for n in g.option_list]]
                        for g in p.option_groups] if l != []]
    if 1 < len(gl):
        p.print_help()
        p.error("Options from the config and cmdline groups "
                "are not compatible")

    # Now throw away the empty list from gl and let's look at what we have left
    [sl] = [x for x in gl if x != []]

    # the length of sl should match the list of option_list from the option group
    [og] = [g for g in p.option_groups if g.title == sl[0]]
    if len(og.option_list) != len(sl):
        p.print_help()
        p.error("Whichever option group is used, all options in it "
                "must be specified")

    if o.hostname is not None:
        hostname = o.hostname
        port = o.port
        username = o.username
        password = o.password
    elif o.cfgfile is not None:
        cfg = CrawlConfig.get_config(o.cfgfile)
        hostname = cfg.get(o.cfgsect, 'hostname')
        port = cfg.get(o.cfgsect, 'port')
        username = cfg.get(o.cfgsect, 'username')
        password = base64.b64decode(cfg.get(o.cfgsect, 'password'))
        
    db = db2.connect("database=%s;" % o.database +
                     "hostname=%s;" % hostname +
                     "port=%s;" % port +
                     "uid=%s;" % username +
                     "pwd=%s" % password,
                     "",
                     "")
    if o.sql == 'cbf':
        result = copies_by_file(db, o.database)
        print "got %d rows" % len(result)
        for row in result:
            print row
    elif o.sql != '':
        r = db2.exec_immediate(db, o.sql)
        x = db2.fetch_assoc(r)
        while x:
            pprint.pprint(x)
            x = db2.fetch_assoc(r)
    else:
        r = db2.tables(db, 'SYSCAT', '%')
        x = db2.fetch_assoc(r)
        while x:
            pprint.pprint(x)
            x = db2.fetch_assoc(r)
    db2.close(db)

# -----------------------------------------------------------------------------
def tccp_bfid(args):
    """bfid - report  a list of bfids

    usage: tcc bfid
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-D', '--db',
                 action='store', default='', dest='dbsect',
                 help='which database to access')
    (o, a) = p.parse_args(args)

    if o.debug: pdb.set_trace()
    
    for row in query("select bfid, bfattr_cos_id, bfattr_create_time from bitfile",
                     dbsect=o.dbsect):
        ct = time.strftime("%Y.%m%d %H:%M:%S",
                           time.localtime(row['BFATTR_CREATE_TIME']))
        print "%s %d %s" % (tcc_common.hexstr(row['BFID']),
                            row['BFATTR_COS_ID'],
                            ct)

# -----------------------------------------------------------------------------
def tccp_bfts(args):
    """bfts - whats in bftapeseg?

    usage: tcc bfts
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-D', '--db',
                 action='store', default='', dest='dbsect',
                 help='which database to access')
    (o, a) = p.parse_args(args)

    if o.debug: pdb.set_trace()
    
    for row in query("select bfid, storage_class from bftapeseg",
                     dbsect=o.dbsect):
        print("%s %s" % (tcc_common.hexstr(row['BFID']),
                         row['STORAGEE_CLASS']))
        
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

    if o.debug: pdb.set_trace()
    
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
    p.add_option('-D', '--db',
                 action='store', default='', dest='dbsect',
                 help='which database to access')
    (o, a) = p.parse_args(args)

    if o.debug: pdb.set_trace()
    
    result = copies_by_file(dbsect=o.dbsect)
    for row in result:
        print row

# -----------------------------------------------------------------------------
def tccp_dblist(args):
    """dblist - display a list of accessible databases

    usage: tcc dblist
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-D', '--db',
                 action='store', default='', dest='dbsect',
                 help='which database to access')
    (o, a) = p.parse_args(args)

    if o.debug: pdb.set_trace()
    
    cfg = CrawlConfig.get_config("tcc.cfg")
    for s in cfg.sections():
        print("   %s" % s)

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
    p.add_option('-D', '--db',
                 action='store', default='', dest='dbsect',
                 help='which database to access')
    (o, a) = p.parse_args(args)

    if o.debug: pdb.set_trace()
    
    cbc = copies_by_cos()
    cbf = copies_by_file(dbsect=o.dbsect)
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
    p.add_option('-D', '--db',
                 action='store', default='', dest='dbsect',
                 help='which database to access')
    (o, a) = p.parse_args(args)

    if o.debug: pdb.set_trace()
    
    record = 0
    for row in query("select * from bitfile", dbsect=o.dbsect):
        print("--- record %d ---" % record)
        record += 1
        for k in sorted(row):
            if k == 'BFID':
                print("%s: %s" % (k, tcc_common.hexstr(row[k])))
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
def tccp_sql(args):
    """sql - run arbitrary sql

    usage: tcc sql [-d/--debug] -D/--db cfg-section <sql statement>
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-D', '--db',
                 action='store', default='', dest='dbsect',
                 help='use an alternate database')
    (o, a) = p.parse_args(args)

    if o.debug: pdb.set_trace()
    
    sql = " ".join(a)
    print sql
    for row in query(sql, dbsect=o.dbsect):
        pprint.pprint(row)

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
def copies_by_file(dbc=None, dbsect=''):
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
                   "group by A.bfid, A.bfattr_cos_id, A.bfattr_create_time",
                   dbsect=dbsect)
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
def query(sql, dbsect='cfg'):
    """
    Connect to a DB2 database, run an sql command (assumed to be a select), and
    return the result.
    """

    cfg = CrawlConfig.get_config()
    if dbsect == 'cfg':
        dbname = cfg.get('db2', 'db_cfg_name')
    elif dbsect == 'sub':
        dbname = cfg.get('db2', 'db_sub_name')
    else:
        raise StandardError("Unknonwn database: '%s'" % dbsect)
    
    username = cfg.get('db2', 'username')
    password = base64.b64decode(cfg.get('db2', 'password'))
    if username == 'retrieve':
        (username, password) = hpss_userpass()
    # dbname = cfg.get(dbsect, 'dbname')
    dbargs = [dbname, username, password]
    if cfg.has_option('db2', 'hostname'):
        hostname = cfg.get('db2', 'hostname')
        port = cfg.get('db2', 'port')
        dbargs = ["database=%s;" % dbname +
                  "hostname=%s;" % hostname +
                  "port=%s;" % port +
                  "uid=%s;" % username +
                  "pwd=%s;" % password,
                  "",
                  ""]
    db = db2.connect(*dbargs)
    
    r = db2.exec_immediate(db, sql)
    rval = []
    x = db2.fetch_assoc(r)
    while (x):
        rval.append(x)
        x = db2.fetch_assoc(r)
    return rval

# -----------------------------------------------------------------------------
toolframe.tf_launch('tccp', __name__)
