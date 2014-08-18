import base64
import CrawlDBI
import CrawlConfig
import os
import util


# -----------------------------------------------------------------------------
def get_bitfile_path(bitfile):
    """
    Given a bitfile id, walk back up the tree in HPSS to generate the bitfile's
    path
    """
    db = CrawlDBI.DBI(dbtype='db2', dbname=CrawlDBI.db2name('subsys'))

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
    db = CrawlDBI.DBI(dbtype='db2', dbname=CrawlDBI.db2name('subsys'))
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
    db = CrawlDBI.DBI(dbtype='db2', dbname=CrawlDBI.db2name('cfg'))
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
    db = CrawlDBI.DBI()
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
    db = CrawlDBI.DBI()

    if not db.table_exists(table=tabname):
        db.create(table=tabname,
                  fields=['check_time    integer',
                          'low_nsobj_id  integer',
                          'high_nsobj_id integer',
                          'correct       integer',
                          'error         integer'])

    ts = int(time.time())
    CrawlConfig.log("recording checked ids %d to %d at %d" % (low, high, ts))
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
