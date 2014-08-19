import CrawlDBI
import hpss
import util as U


# -----------------------------------------------------------------------------
def dbalter(table=None, addcol=None, dropcol=None, pos=None):
    """
    Pass arguments through to the db alter routine
    !@! test this
    """
    db = CrawlDBI.DBI(dbtype="crawler")
    db.alter(table=table, addcol=addcol, dropcol=dropcol, pos=pos)
    db.close()


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
                     where="cos is NULL or cart is NULL or ttypes is NULL")
    db.close()
    return rval


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
    *pc_l* contains tuples of (hsi cart val, path)
    """
    db = CrawlDBI.DBI(dbtype="crawler")
    db.update(table="checkables",
              fields=["cart"],
              where="path = ?",
              data=pc_l)
    db.close()


# -----------------------------------------------------------------------------
def prep_popcart(where):
    """
    Get a list of paths and carts from database based on where
    """
    db = CrawlDBI.DBI(dbtype="crawler")
    rows = db.select(table="checkables",
                     fields=["path", "cart"],
                     where=where)
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
        (path, cart, ttype, lcheck) = row
        if lcheck == 0:
            lcheck_s = "unchecked"
        else:
            lcheck_s = time.strftime("%Y.%m%d %H:%M:%S",
                                     time.localtime(lcheck))
        print("%-30s %s %s %s" % (path, cart, ttype, lcheck_s))

    # -------------------------------------------------------------------------
    db = CrawlDBI.DBI(dbtype="crawler")
    row_l = db.select(table="checkables",
                     fields=["path", "cart", "ttypes", "last_check"],
                     where="path = ?",
                     data=(path,))
    db.close()

    if 1 < len(row_l):
        print("Duplicate rows for path %s:" % row_l[0][0])
        for row in row_l:
            report_row(row)
    else:
        report_row(row_l[0])


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
                     fields=["path", "type", "ttypes", "cart"],
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
                         fields=["path", "type", "ttypes", "cart"],
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
    """
    db = CrawlDBI.DBI(dbtype="crawler")
    db.update(table="checkables",
              fields=["ttypes", "cart"],
              where="path = ?",
              data=[data])
    db.close()


# -----------------------------------------------------------------------------
def ttype_lookup(pathname):
    """
    Use hsi to get the name of the cart where this file lives.

    Look up the cart in table pvlpv and get the type and subtype.

    Look up the type/subtype combination in the *_tape_types table and return
    the corresponding string.
    """
    rval = []

    # Get the cart name from hsi
    H = hpss.HSI()
    r = H.lsP(pathname)
    H.quit()

    (type, name, cart, cos) = U.lsp_parse(r)
    if not cart:
        return None

    cartlist = cart.split(',')

    # Get the type/subtype from PVLPV
    for cart in cartlist:
        (type, subtype) = ttype_cart_lookup(cart)
        desc = ttype_map_desc(type, subtype)
        rval.append((cart, desc))

    # Return the media description
    return rval


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
def update_stats(cmf):
    """
    Record the values in tuple cmf in table cvstats in the database. If the
    table does not exist, create it.
    """
    db = CrawlDBI.DBI()
    if not db.table_exists(table=stats_table):
        db.create(table=stats_table,
                  fields=["rowid int",
                          "matches int",
                          "failures int",
                          ])
        db.insert(table=stats_table,
                  fields=["rowid", "matches", "failures"],
                  data=[(1, 0, 0)])

    db.update(table=stats_table,
              fields=["matches", "failures"],
              data=[cmf],
              where="rowid = 1")
    db.close()
