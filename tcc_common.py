# -----------------------------------------------------------------------------
def hexstr(bfid):
    """
    Convert a raw bitfile id into a hexadecimal string as presented by DB2.
    """
    rval = "x'"
    for c in list(bfid):
        rval += "%02x" % ord(c)
    rval += "'"
    return rval

