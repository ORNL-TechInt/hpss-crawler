import hpss
import util as U


# -----------------------------------------------------------------------------
def ttype_lookup(pathname):
    """
    Use hsi to get the name of the cart where this file lives.

    Look up the cart in table pvlpv and get the type and subtype.

    Look up the type/subtype combination in the *_tape_types table and return
    the corresponding string.
    """

    # Get the cart name from hsi
    H = hpss.HSI()
    r = H.lsP(pathname)
    H.quit()

    (type, name, cart, cos) = U.lsp_parse(r)
    if cart is None:
        return None

    # Get the type/subtype from PVLPV
    (type, subtype) = ttype_cart_lookup(cart)

    # Get the human readable string from *_tape_types
    desc = ttype_map_desc(type, subtype)

    # Return the media description
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
                     where="cartridge_id = ?",
                     data=(cartname,))
    db.close()
    return rows[0]


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
    return rows[0]
