import hpss
import util as U


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

    (type, name, cart, cos) = U.lsp_parse(r)  # !@! write util.lsp_parse()

    # Get the type/subtype from PVLPV
    (type, subtype) = ttype_cart_lookup(cart)  # !@! write ttype_cart_lookup()

    # Get the human readable string from *_tape_types
    desc = ttype_map_desc(type, subtype)   # !@! write ttype_map_desc()

    # Return the media description
    return desc
