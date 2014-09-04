import base64
import CrawlDBI
import CrawlConfig
import dbschem
import os
import time
import util


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

    if bitfile.startswith("x'") or bitfile.startswith('x"'):
        bfarg = bitfile
    else:
        bfarg = hexstr(bitfile)
    bfl = db.select(table='nsobject',
                    fields=['parent_id', 'name'],
                    where='bitfile_id = ?',
                    data=(bfarg,))

    if 1 < len(bfl):
        raise StandardError("Multiple objects found for bf %s" %
                            hexstr(bitfile))
    elif len(bfl) < 1:
        return("<unnamed bitfile>")

    x = bfl[0]
    rval = ''
    while x:
        if rval == '':
            rval = x['NAME']
        else:
            rval = os.path.join(x['NAME'], rval)

        x = db.select(table='nsobject',
                      fields=['parent_id', 'name'],
                      where='object_id = ?',
                      data=(x['PARENT_ID']))
    return rval


# -----------------------------------------------------------------------------
def get_bitfile_set(cfg, first_nsobj_id, limit):
    """
    Get a collection of bitfiles from DB2 returning a dict. The bitfiles in the
    set begin with object_id first_nsobj_id and end with the one before
    last_nsobj_id.

    get items between object_id LOW and HIGH:
          select A.object_id,
                 B.bfid, B.bfattr_cos_id, B.bfattr_create_time,
                 count(C.storage_class) as sc_count
          from hpss.nsobject A, hpss.bitfile B, hpss.bftapeseg C
          where A.bitfile_id = B.bfid and B.bfid = C.bfid and
                 B.bfattr_data_len > 0 and C.bf_offset = 0 and
                 ? <= A.object_id and A.object_id < ?
          group by A.object_id, B.bfid, B.bfattr_cos_id, B.bfattr_create_time

    get N items beginning at object_id LOW:
          "select A.object_id,
                 B.bfid, B.bfattr_cos_id, B.bfattr_create_time,
                 count(C.storage_class) as sc_count
          from hpss.nsobject A, hpss.bitfile B, hpss.bftapeseg C
          where A.bitfile_id = B.bfid and B.bfid = C.bfid and
                 B.bfattr_data_len > 0 and C.bf_offset = 0 and
                 ? <= A.object_id
          group by A.object_id, B.bfid, B.bfattr_cos_id, B.bfattr_create_time
          fetch first %d rows only" % limit
    """
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
                           "? <= A.object_id and A.object_id < ? ",
                     groupby=", ".join(["A.object_id",
                                        "B.bfid",
                                        "B.bfattr_cos_id",
                                        "B.bfattr_create_time"]),
                     data=(first_nsobj_id, first_nsobj_id + limit),
                     limit=limit)
    return rval


# -----------------------------------------------------------------------------
def get_cos_info():
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
    return 'tcc'


# -----------------------------------------------------------------------------
def table_list():
    db = CrawlDBI.DBI(dbtype='hpss', dbname='sub')
    db._dbobj.tbl_prefix = 'syscat.'
    rows = db.select(table='tables',
                     fields=["substr(tabname, 1, 30) as \"Table\"",
                             "substr(tabschema, 1, 30) as \"Schema\"",
                             "type"],
                     where="tabschema = 'HPSS'")
    return rows


# -----------------------------------------------------------------------------
def tcc_report(bitfile, cosinfo):
    """
    The bitfile appears to not have the right number of copies. We're going to
    write its information out to a report for manual followup.
    """
    fmt = "%7s %8s %8s %s\n"
    # Compute the bitfile's path
    bfp = get_bitfile_path(bitfile['BFID'])
    rpt = fmt % (bitfile['BFATTR_COS_ID'],
                 str(cosinfo[bitfile['BFATTR_COS_ID']]),
                 str(bitfile['SC_COUNT']),
                 bfp)
    CrawlConfig.log(rpt)
    try:
        tcc_report._f.write(rpt)
        tcc_report._f.flush()
    except AttributeError:
        cfg = CrawlConfig.get_config()
        rptfname = cfg.get(sectname(), 'report_file')
        tcc_report._f = open(rptfname, 'a')
        tcc_report._f.write(fmt % ("COS", "Ccopies", "Fcopies", "Filepath"))
        tcc_report._f.write(rpt)
        tcc_report._f.flush()
