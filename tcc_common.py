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

