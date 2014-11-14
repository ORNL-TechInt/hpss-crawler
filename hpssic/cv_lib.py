import CrawlDBI
import dbschem
import hpss
import pdb
import time
import util as U

stats_table = 'cvstats'


# -----------------------------------------------------------------------------
def get_checksum_count():
    """
    Return the count of checksums in the crawler database
    """
    db = CrawlDBI.DBI(dbtype="crawler")
    if db.table_exists(table="checkables"):
        rows = db.select(table='checkables',
                         fields=["count(path)"],
                         where="checksum = 1")
        checksums = rows[0][0]
    else:
        checksums = 0
    db.close()
    return checksums


# -----------------------------------------------------------------------------
def get_match_fail_count():
    """
    Return the match and fail counts in the crawler database
    """
    db = CrawlDBI.DBI(dbtype="crawler")
    if db.table_exists(table=stats_table):
        rows = db.select(table=stats_table,
                         fields=["matches", "failures"],
                         where="rowid = 1")
        (matches, failures) = rval = rows[0]
    else:
        (matches, failures) = (0, 0)

    db.close()
    return (matches, failures)


# -----------------------------------------------------------------------------
def lookup_checksum_by_path(path):
    """
    Retrieve a row by path and return the checksum value
    """

    db = CrawlDBI.DBI(dbtype="crawler")
    rows = db.select(table="checkables",
                     fields=["checksum"],
                     where="path = '%s'" % path)
    if 0 == len(rows):
        rval = -1
    elif 1 == len(rows):
        rval = rows[0][0]
    else:
        raise SystemExit("Too many records found for path %s" % path)
    db.close()


# -----------------------------------------------------------------------------
def lookup_nulls():
    """
    Return records that contain NULL values
    """
    db = CrawlDBI.DBI(dbtype="crawler")
    rval = db.select(table="checkables",
                     fields=["rowid", "path", "type", "cos", "cart",
                             "ttypes", "checksum", "last_check", "fails",
                             "reported"],
                     where="cos is NULL or cart is NULL or ttypes is NULL")
    db.close()
    return rval


# -----------------------------------------------------------------------------
def lscos_populate():
    """
    If table lscos already exists, we're done. Otherwise, retrieve the lscos
    info from hsi, create the table, and fill the table in.

    We store the min_size and max_size for each COS as text strings containing
    digits because the largest sizes are already within three orders of
    magnitude of a mysql bigint and growing.
    """
    db = CrawlDBI.DBI(dbtype="crawler")
    st = dbschem.make_table("lscos")
    if "Created" == st:
        H = hpss.HSI()
        raw = H.lscos()
        H.quit()

        z = [x.strip() for x in raw.split('\r')]
        rules = [q for q in z if '----------' in q]
        first = z.index(rules[0]) + 1
        second = z[first:].index(rules[0]) + first
        lines = z[first:second]
        data = []
        for line in lines:
            (cos, desc, copies, lo, hi) = (line[0:4],
                                           line[5:34].strip(),
                                           int(line[36:44].strip()),
                                           line[60:].split('-')[0],
                                           line[60:].split('-')[1])
            lo_i = lo.replace(',', '')
            hi_i = hi.replace(',', '')
            data.append((cos, desc, copies, lo_i, hi_i))

        db.insert(table='lscos',
                  fields=['cos', 'name', 'copies', 'min_size', 'max_size'],
                  data=data)

    db.close()


# -----------------------------------------------------------------------------
def nulls_from_checkables():
    """
    Return rows from table checkables that contain null values
    """
    db = CrawlDBI.DBI(dbtype="crawler")
    rval = db.select(table="checkables",
                     fields=["rowid", "path", "type", "cos", "cart",
                             "ttypes", "checksum", "last_check", "fails",
                             "reported"],
                     where="fails is null or reported is null or cart is null")
    db.close()
    return rval


# -----------------------------------------------------------------------------
def popcart(pc_l):
    """
    *pc_l* contains tuples of (path, db val, hsi cart val)
    """
    hp_l = [(x[2], x[0]) for x in pc_l]
    db = CrawlDBI.DBI(dbtype="crawler")
    db.update(table="checkables",
              fields=["cart"],
              where="path = ?",
              data=hp_l)
    db.close()


# -----------------------------------------------------------------------------
def prep_popcart(where, limit):
    """
    Get a list of paths and carts from database based on where. If 0 < limit,
    no more than limit records will be retrieved.
    """
    db = CrawlDBI.DBI(dbtype="crawler")
    kw = {}
    if 0 < limit:
        kw['limit'] = limit
    rows = db.select(table="checkables",
                     fields=["path", "cart"],
                     where=where,
                     **kw)
    db.close()
    return rows


# -----------------------------------------------------------------------------
def reset_path(pathname):
    """
    Reset the fails and reported fields on a rows so it can be rechecked
    """
    db = CrawlDBI.DBI(dbtype="crawler")
    db.update(table='checkables',
              fields=['fails', 'reported'],
              where="path = ?",
              data=[(0, 0, o.pathname)])
    db.close()


# -----------------------------------------------------------------------------
def tpop_report_updates(data):
    """
    Report records updated by tpop_update_by_path
    """
    # -------------------------------------------------------------------------
    def report_row(row):
        (ttype, cart, path, lcheck) = row
        if lcheck == 0:
            lcheck_s = "unchecked"
        else:
            lcheck_s = time.strftime("%Y.%m%d %H:%M:%S",
                                     time.localtime(lcheck))
        print("%-60s \n   %s %30s %s" % (path, cart, ttype, lcheck_s))

    for row in data:
        report_row(row)


# -------------------------------------------------------------------------
def tpop_select_all(db=None):
    """
    Return a list of all the 'f' (file) type checkables records with a null
    ttypes or cart.
    """
    close = False
    if db is None:
        db = CrawlDBI.DBI(dbtype='crawler')
        close = True
    rval = db.select(table="checkables",
                     fields=["path", "type", "ttypes", "cart", "last_check"],
                     where="type = 'f' and " +
                     "(ttypes is NULL or cart is NULL)")
    if close:
        db.close()
    return rval


# -------------------------------------------------------------------------
def tpop_select_by_paths(path_l, db=None):
    """
    Return a list checkable rows that match the path list where ttypes
    and/or cart is null.
    """
    close = False
    if db is None:
        db = CrawlDBI.DBI(dbtype='crawler')
        close = True
    rval = []
    for path in path_l:
        rows = db.select(table="checkables",
                         fields=["path",
                                 "type",
                                 "ttypes",
                                 "cart",
                                 "last_check"],
                         where="path like ? and type = 'f' and " +
                         "(ttypes is NULL or cart is NULL)",
                         data=(path,))
        rval.extend(rows)
    if close:
        db.close()
    return rval


# -----------------------------------------------------------------------------
def tpop_update_by_path(data):
    """
    Update media type (ttypes) and cartridge names (cart) based on path.

    Incoming *data* is a list of tuples containing ttypes, cart, path, and last
    check.
    """
    zdata = [(d[0], d[1], d[2]) for d in data]
    db = CrawlDBI.DBI(dbtype="crawler")
    db.update(table="checkables",
              fields=["ttypes", "cart"],
              where="path = ?",
              data=zdata)
    db.close()


# -----------------------------------------------------------------------------
def ttype_lookup(pathname, cart=None):
    """
    Use hsi to get the name of the cart where this file lives.

    Look up the cart in table pvlpv and get the type and subtype.

    Look up the type/subtype combination in the *_tape_types table and return
    the corresponding string.
    """
    rval = []

    # Get the cart name from hsi if we don't already have it
    if cart is None or cart == '':
        H = hpss.HSI()
        r = H.lsP(pathname)
        H.quit()

        (type, name, cart, cos) = U.lsp_parse(r)
        if not cart:
            return None

    cartlist = cart.split(',')

    # Get the type/subtype from PVLPV
    for cart in cartlist:
        desc = ttype_cart_to_desc(cart)
        rval.append((cart, desc))

    # Return the media description
    return rval


# -----------------------------------------------------------------------------
@U.memoize
def ttype_cart_to_desc(cart):
    """
    Call ttype_cart_lookup and ttype_map_desc to go from cart to media
    description
    """
    (type, subtype) = ttype_cart_lookup(cart)
    desc = ttype_map_desc(type, subtype)
    return desc


# -----------------------------------------------------------------------------
def ttype_cart_lookup(cartname):
    """
    Look up *cartname* in HPSS table PVLPV and return the cartridge's media
    type and subtype.
    """
    db = CrawlDBI.DBI(dbtype='hpss', dbname='cfg')
    rows = db.select(table="pvlpv",
                     fields=["phys_vol_type_type",
                             "phys_vol_type_subtype",
                             ],
                     where="phys_vol_id = ?",
                     data=(cartname,))
    db.close()
    return (rows[0]['PHYS_VOL_TYPE_TYPE'], rows[0]['PHYS_VOL_TYPE_SUBTYPE'])


# -----------------------------------------------------------------------------
def ttype_map_desc(type, subtype):
    """
    Look up *type* and *subtype* in the crawler table tape_types and return
    the corresponding media description.
    """
    db = CrawlDBI.DBI(dbtype='crawler')
    rows = db.select(table="tape_types",
                     fields=["name"],
                     where="type = ? and subtype = ?",
                     data=(type, subtype,))
    db.close()
    return rows[0][0]


# -----------------------------------------------------------------------------
def ttype_map_insert(TT):
    """
    Populate the table PFX_tape_types with the contents of *data*.
    """
    tt_tups = []
    for k in TT:
        if type(k) == int:
            try:
                mtype = TT[k]['label']
            except KeyError:
                mtype = TT[k]['name']
            for l in TT[k]:
                if type(l) == int:
                    try:
                        mstype = TT[k][l]['label']
                    except KeyError:
                        mstype = TT[k][l]['list'][0]

                    tt_tups.append((k, l, '%s/%s' % (mtype, mstype)))

    db = CrawlDBI.DBI(dbtype="crawler")
    db.insert(table='tape_types',
              fields=['type', 'subtype', 'name'],
              data=tt_tups)
    db.close()


# -----------------------------------------------------------------------------
def ttype_missing():
    """
    Return a list of records where type = 'f' and ttype is null
    """
    db = CrawlDBI.DBI(dbtype='crawler')
    rows = db.select(table='checkables',
                     fields=['rowid',
                             'path',
                             'type',
                             'cos',
                             'cart',
                             'ttypes',
                             'checksum',
                             'last_check',
                             'fails',
                             'reported'],
                     where="type = 'f' and ttypes is null")
    db.close()
    return rows


# -----------------------------------------------------------------------------
def update_stats(cmf):
    """
    Record the values in tuple cmf in table cvstats in the database. If the
    table does not exist, create it.
    """
    result = dbschem.make_table(stats_table)
    db = CrawlDBI.DBI(dbtype="crawler")
    if result == "Created":
        db.insert(table=stats_table,
                  fields=["rowid", "matches", "failures"],
                  data=[(1, 0, 0)])

    db.update(table=stats_table,
              fields=["matches", "failures"],
              data=[cmf],
              where="rowid = 1")
    db.close()
