import CrawlDBI
import hpss
import util as U


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
