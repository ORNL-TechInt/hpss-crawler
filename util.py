import sys

# -----------------------------------------------------------------------------
def contents(filename):
    """
    Return the contents of the file as a string
    """
    f = open(filename, 'r')
    rval = f.read()
    f.close()
    return rval

# -----------------------------------------------------------------------------
def my_name():
    """
    Return the caller's name
    """
    return sys._getframe(1).f_code.co_name
