from __future__ import print_function
import base64
import CrawlDBI
import CrawlConfig
import dbschem
import messages as MSG
import os
import pdb
import sys
import time
import util
import util as U


# -----------------------------------------------------------------------------
def db_select(dbtype='hpss', dbname='sub', **dbargs):
    """
    Issue a select based on arguments passed in. Return the result.
    """
    db = CrawlDBI.DBI(dbtype=dbtype, dbname=dbname)
    result = db.select(**dbargs)
    db.close()
    return result


# -----------------------------------------------------------------------------
def get_bitfile_path(bitfile):
    """
    Given a bitfile id, walk back up the tree in HPSS to generate the bitfile's
    path
    """
    db = CrawlDBI.DBI(dbtype='hpss', dbname='sub')

    rows = db.select(table='nsobject',
                     fields=['parent_id', 'name'],
                     where='bitfile_id = ?',
                     data=(bitfile,))

    if 1 < len(rows):
        raise U.HpssicError(MSG.multiple_objects_S % hexstr(bitfile))
    elif len(rows) < 1:
        return("<unnamed bitfile>")

    rval = ''
    while rows:
        x = rows[0]
        if rval == '':
            rval = x['NAME']
        else:
            rval = os.path.join(x['NAME'], rval)

        rows = db.select(table='nsobject',
                         fields=['parent_id', 'name'],
                         where='object_id = ?',
                         data=(x['PARENT_ID'],))

    return rval


# -----------------------------------------------------------------------------
def nsobject_lookup(start_id, id_count, dbh=None):
    """
    Lookup the records in NSOBJECT beginning with start_id and continuing for
    id_count entries. Return the list of corresponding bitfile ids.
    """
    local_connect = False
    if dbh is None:
        dbh = CrawlDBI.DBI(dbtype='hpss', dbname='sub')
        local_connect = True

    bflist = dbh.select(table='nsobject',
                        fields=['bitfile_id'],
                        where='? <= object_id and object_id < ?',
                        data=(start_id, start_id + id_count),
                        limit=id_count)

    if local_connect:
        dbh.close()

    rval = [CrawlDBI.DBIdb2.hexstr(z['BITFILE_ID']) for z in bflist]
    return rval


# -----------------------------------------------------------------------------
def bitfile_lookup(bflist, dbh=None):
    """
    Lookup each of the ids in *bflist* in table BITFILE. Return the count of
    records found.
    """
    local_connect = False
    if dbh is None:
        dbh = CrawlDBI.DBI(dbtype='hpss', dbname='sub')
        local_connect = True

    count = dbh.select(table='bitfile',
                       fields=['count(bfid) as bfid_count'],
                       where='bfid in (%s)' %
                       ','.join(bflist))

    if local_connect:
        dbh.close()

    return(count[0]['BFID_COUNT'])


# -----------------------------------------------------------------------------
def get_bitfile_set(first_nsobj_id, limit):
    """
    Get a collection of bitfiles from DB2 returning a dict. The bitfiles in the
    set begin with object_id first_nsobj_id and end with the one before
    last_nsobj_id.
    """
    db = CrawlDBI.DBI(dbtype='hpss', dbname='sub')

    bfid_list = nsobject_lookup(first_nsobj_id, limit, dbh=db)
    if 0 == len(bfid_list):
        db.close()
        raise U.HpssicError(MSG.not_in_nsobject_D % first_nsobj_id)

    n_found = bitfile_lookup(bfid_list, dbh=db)
    if 0 == n_found:
        db.close()
        raise U.HpssicError(MSG.not_in_bitfile_S % bfid_list[0])

    rval = db.select(table=['nsobject A',
                            'bitfile B',
                            'bftapeseg C'],
                     fields=['A.object_id',
                             'B.bfid',
                             'B.bfattr_cos_id',
                             'B.bfattr_create_time',
                             'count(C.storage_class) as sc_count'],
                     where="A.bitfile_id = B.bfid and B.bfid = C.bfid and " +
                           "B.bfattr_data_len > 0 and C.bf_offset = 0 and " +
                           "? <= A.object_id and A.object_id < ? ",
                     groupby=", ".join(["A.object_id",
                                        "B.bfid",
                                        "B.bfattr_cos_id",
                                        "B.bfattr_create_time"]),
                     data=(first_nsobj_id, first_nsobj_id + limit),
                     limit=limit)
    db.close()

    if 0 == len(rval):
        raise U.HpssicError(MSG.not_in_bftapeseg_S % bfid_list[0])

    return rval


# -----------------------------------------------------------------------------
def by_bitfile_id(bfid):
    """
    Get info about a bitfile from DB2 returning a dict.
    """
    bfid_val = CrawlDBI.DBIdb2.hexval(bfid)
    db = CrawlDBI.DBI(dbtype='hpss', dbname='sub')
    rval = db.select(table=['nsobject A',
                            'bitfile B',
                            'bftapeseg C'],
                     fields=['A.object_id',
                             'B.bfid',
                             'B.bfattr_cos_id',
                             'B.bfattr_create_time',
                             'count(C.storage_class) as sc_count'],
                     where="A.bitfile_id = B.bfid and B.bfid = C.bfid and " +
                           "B.bfattr_data_len > 0 and C.bf_offset = 0 and " +
                           "A.bitfile_id = ?",
                     groupby=", ".join(["A.object_id",
                                        "B.bfid",
                                        "B.bfattr_cos_id",
                                        "B.bfattr_create_time"]),
                     data=(bfid_val, ))
    if 1 < len(rval):
        raise U.HpssicError(MSG.multiple_objects_S % bfid)
    elif len(rval) < 1:
        raise U.HpssicError(MSG.no_bitfile_found_S % bfid)
    return rval[0]


# -----------------------------------------------------------------------------
@U.memoize
def get_cos_info(obarg=None):
    """
    Read COS info from tables COS and HIER in the DB2 database
    """
    db = CrawlDBI.DBI(dbtype='hpss', dbname='cfg')
    rows = db.select(table=['cos A', 'hier B'],
                     fields=['A.cos_id',
                             'A.hier_id',
                             'B.slevel0_migrate_list_count'],
                     where="A.hier_id = B.hier_id")
    rval = {}
    for r in rows:
        rval[r['COS_ID']] = r['SLEVEL0_MIGRATE_LIST_COUNT']

    return rval


# -----------------------------------------------------------------------------
def get_next_nsobj_id(cfg):
    """
    Read the TCC table in the HPSSIC database to get the next nsobject id. If
    the table does not exist, we create it and return 1 for the next object id
    to check. If the table exists but is empty, we return 1 for the next object
    id to check.
    """
    tabname = cfg.get(sectname(), 'table_name')
    db = CrawlDBI.DBI(dbtype="crawler")
    if not db.table_exists(table=tabname):
        rval = 1
    else:
        rows = db.select(table=tabname,
                         fields=['max(check_time)'])
        max_time = rows[0][0]
        if max_time is None:
            rval = 1
        else:
            rows = db.select(table=tabname,
                             fields=['high_nsobj_id'],
                             where='check_time = ?',
                             data=(max_time,))
            rval = int(rows[0][0]) + 1
            if highest_nsobject_id() < rval:
                rval = 1
    db.close()
    return rval


# -----------------------------------------------------------------------------
def hexstr(bfid):
    """
    Convert a raw bitfile id into a hexadecimal string as presented by DB2.
    """
    rval = "x'" + hexstr_uq(bfid) + "'"
    return rval


# -----------------------------------------------------------------------------
def hexstr_uq(bfid):
    """
    Convert a raw bitfile id into an unquoted hexadecimal string as presented
    by DB2.
    """
    rval = "".join(["%02x" % ord(c) for c in list(bfid)])
    return rval.upper()


# -----------------------------------------------------------------------------
def highest_nsobject_id():
    """
    Cache and return the largest NSOBJECT id in the DB2 database. The variables
    highest_nsobject_id._max_obj_id and highest_nsobject_id._when are local to
    this function but do not lose their values between invocations.
    """
    if (not hasattr(highest_nsobject_id, '_max_obj_id') or
            not hasattr(highest_nsobject_id, '_when') or
            60 < time.time() - highest_nsobject_id._when):
        highest_nsobject_id._max_obj_id = max_nsobj_id()
        highest_nsobject_id._when = time.time()
        CrawlConfig.log("max object id = %d at %s" %
                        (highest_nsobject_id._max_obj_id,
                         util.ymdhms(highest_nsobject_id._when)))

    rval = highest_nsobject_id._max_obj_id
    return rval


# -----------------------------------------------------------------------------
def max_nsobj_id():
    """
    Return the value of the largest NS object id in the nsobject table
    """
    db = CrawlDBI.DBI(dbtype='hpss', dbname='sub')
    result = db.select(table='nsobject',
                       fields=['max(object_id) as max_obj_id'])
    db.close()
    rval = int(result[0]['MAX_OBJ_ID'])
    return rval


# -----------------------------------------------------------------------------
def record_checked_ids(cfg, low, high, correct, error):
    """
    Save checked NSOBJECT ids in the HPSSIC database.

    If we check a range and get no hits (i.e., no NSOBJECT ids exist in the
    range), we'll store

       (<time>, <low-id>, <high-id>, 0, 0)

    If we get a hit with the right copy count, we store it by itself as

       (<time>, <hit-id>, <hit-id>, 1, 0)

    If we get a hit with the wrong copy count, we store it by itself as

       (<time>, <hit-id>, <hit-id>, 0, 1)
    """
    tabname = cfg.get(sectname(), 'table_name')

    result = dbschem.make_table(tabname)
    ts = int(time.time())
    CrawlConfig.log("recording checked ids %d to %d at %d" % (low, high, ts))
    db = CrawlDBI.DBI(dbtype="crawler")
    db.insert(table=tabname,
              fields=['check_time',
                      'low_nsobj_id',
                      'high_nsobj_id',
                      'correct',
                      'error'],
              data=[(ts, low, high, correct, error)])
    db.close()


# -----------------------------------------------------------------------------
def sectname():
    """
    The configuration section name for this plugin
    """
    return 'tcc'


# -----------------------------------------------------------------------------
def check_file(filename, verbose, plugin=True):
    """
    This may be called interactively from the command line, in which case
    *plugin* will be False. Or it may be called by the tcc plugin, in which
    case plugin will be True.
    """
    for item in [x.strip() for x in U.contents(filename, string=False)]:
        try:
            if 0 == len(item.strip()):
                continue
            elif item[1] == ':' and item[0] in ['o', 'b', 'p']:
                (which, value) = item.split(':', 1)
                if which == 'o':
                    check_object(value.strip(),
                                 verbose,
                                 plugin=plugin,
                                 xof=False)
                elif which == 'b':
                    check_bitfile(value.strip(),
                                  verbose,
                                  plugin=plugin,
                                  xof=False)
                elif which == 'p':
                    check_path(value.strip(),
                               verbose,
                               plugin=plugin,
                               xof=False)
                else:
                    check_path(value.strip(),
                               verbose,
                               plugin=plugin,
                               xof=False)
            else:
                check_path(item.strip(),
                           verbose,
                           plugin=plugin,
                           xof=False)
        except U.HpssicError as e:
            print("%s:\n %s" % (item, e.value), file=sys.stderr)


# -----------------------------------------------------------------------------
def check_object(obj_id, verbose=False, plugin=True, xof=True):
    """
    If plugin is True, we want to log and store, which tcc_report does by
    default so we leave those flags alone.

    If plugin is False, we're interactive and we want to write any report to
    stdout. However, we only make a report if 1) verbose is True, or 2) the
    counts don't match.
    """
    cosinfo = get_cos_info()
    try:
        bfl = get_bitfile_set(int(obj_id), 1)
    except U.HpssicError as e:
        if plugin:
            CrawlConfig.log(e.value)
        elif xof:
            raise SystemExit(e.value)
        else:
            raise U.HpssicError(e.value)

    bf = U.pop0(bfl)
    sc_count = int(bf['SC_COUNT'])
    cos_count = int(cosinfo[bf['BFATTR_COS_ID']])

    if plugin and sc_count != cos_count:
        tcc_report(bf)
    elif not plugin and (verbose or sc_count != cos_count):
        print(tcc_report(bf, log=False, store=False))


# -----------------------------------------------------------------------------
def check_bitfile(bfid, verbose=False, plugin=True, xof=True):
    """
    If plugin is True, we want to log and store, which tcc_report does by
    default so we leave those flags alone.

    If plugin is False, we're interactive and we want to write any report to
    stdout. However, we only make a report if 1) verbose is True, or 2) the
    counts don't match.
    """
    cosinfo = get_cos_info()
    bf = by_bitfile_id(bfid)
    sc_count = int(bf['SC_COUNT'])
    cos_count = int(cosinfo[bf['BFATTR_COS_ID']])

    if plugin and sc_count != cos_count:
        rpt = tcc_report(bf)
    elif not plugin and (verbose or sc_count != cos_count):
        print(tcc_report(bf, log=False, store=False))


# -----------------------------------------------------------------------------
def check_path(path, verbose=False, plugin=True, xof=True):
    """
    If plugin is True, we want to log and store, which tcc_report does by
    default so we leave those flags alone.

    If plugin is False, we're interactive and we want to write any report to
    stdout. However, we only make a report if 1) verbose is True, or 2) the
    counts don't match.
    """
    cosinfo = get_cos_info()
    nsobj = path_nsobject(path)
    try:
        bfl = get_bitfile_set(int(nsobj), 1)
    except U.HpssicError as e:
        if plugin:
            CrawlConfig.log(e.value)
            return
        elif xof:
            raise SystemExit(e.value)
        else:
            raise U.HpssicError(e.value)

    bf = U.pop0(bfl)
    sc_count = int(bf['SC_COUNT'])
    cos_count = int(cosinfo[bf['BFATTR_COS_ID']])

    if plugin and sc_count != cos_count:
        tcc_report(bf, path=path)
    elif not plugin and (verbose or sc_count != cos_count):
        print(tcc_report(bf, path=path, log=False, store=False))


# -----------------------------------------------------------------------------
def path_nsobject(path=''):
    """
    Look up an nsobject id based on a path
    """
    if not path.startswith('/'):
        raise U.HpssicError("An absolute path is required")

    # break the path into its components with '/' at the beginning
    nl = ['/'] + [z for z in path.lstrip('/').split(os.path.sep)]
    parent_id = None

    # walk down the tree structure to the leaf
    for name in nl:
        (obj_id, parent_id) = nsobj_id(name=name, parent=parent_id)
        parent_id = obj_id

    # return the bottom object id
    return obj_id


# -----------------------------------------------------------------------------
def nsobj_id(name='', parent=None):
    """
    Look up an nsobject id based on name and, optionally, parent id
    """
    db = CrawlDBI.DBI(dbtype='hpss', dbname='sub')
    if name == '':
        return -1
    elif name != '' and parent is None:
        where = "name = '%s'" % name
    elif name != '' and parent is not None:
        where = "name = '%s' and parent_id=%d" % (name, parent)

    rows = db.select(table='hpss.nsobject',
                     fields=['object_id', 'parent_id'],
                     where=where)
    db.close()
    try:
        rval = (rows[0]['OBJECT_ID'], rows[0]['PARENT_ID'])
    except IndexError:
        raise U.HpssicError(MSG.no_such_path_component_SD % (name, parent))

    return rval


# -----------------------------------------------------------------------------
def table_list():
    """
    Return the list of HPSS tables from the DB2 database
    """
    db = CrawlDBI.DBI(dbtype='hpss', dbname='sub')
    db._dbobj.tbl_prefix = 'syscat.'
    rows = db.select(table='tables',
                     fields=["substr(tabname, 1, 30) as \"Table\"",
                             "substr(tabschema, 1, 30) as \"Schema\"",
                             "type"],
                     where="tabschema = 'HPSS'")
    return rows


# -----------------------------------------------------------------------------
def tcc_report(bitfile, cosinfo=None, path=None, log=True, store=True):
    """
    The bitfile appears to not have the right number of copies. We're going to
    write its information out to a report for manual followup.
    """
    cosinfo = get_cos_info()
    fmt = "%7s %8s %8s %s"
    hdr = fmt % ("COS", "Ccopies", "Fcopies", "Filepath")

    # Compute the bitfile's path
    if path is None:
        bfp = get_bitfile_path(bitfile['BFID'])
    else:
        bfp = path
    rpt = fmt % (bitfile['BFATTR_COS_ID'],
                 str(cosinfo[bitfile['BFATTR_COS_ID']]),
                 str(bitfile['SC_COUNT']),
                 bfp)
    if log:
        CrawlConfig.log(rpt)
    if store:
        try:
            tcc_report._f.write(rpt + "\n")
            tcc_report._f.flush()
        except AttributeError:
            cfg = CrawlConfig.get_config()
            rptfname = cfg.get(sectname(), 'report_file')
            tcc_report._f = open(rptfname, 'a')
            tcc_report._f.write(hdr)
            tcc_report._f.write(rpt + "\n")
            tcc_report._f.flush()
    return rpt
