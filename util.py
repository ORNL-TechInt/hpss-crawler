import sys

def my_name():
    """
    Return the caller's name
    """
    return sys._getframe(1).f_code.co_name
