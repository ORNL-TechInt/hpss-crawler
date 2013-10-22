#!/usr/bin/env python

import pdb
import sys
import testhelp
import unittest

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

# -----------------------------------------------------------------------------
class UtilTest(unittest.TestCase):
    # -------------------------------------------------------------------------
    def test_content(self):
        x = contents('./util.py')
        self.assertEqual(type(x), str,
                         "Expected a string but got a %s" % type(x))
        expected = 'def contents('
        self.assertIn(expected, x,
                      "Expected to find '%s' in \"\"\"\n%s\n\"\"\"" %
                      (expected, x))

    # -------------------------------------------------------------------------
    def test_my_name(self):
        actual = my_name()
        expected = 'test_my_name'
        self.assertEqual(expected, actual,
                         "Expected '%s' to match '%s'" %
                         (expected, actual))

# -----------------------------------------------------------------------------
if __name__ == '__main__':
    if '-d' in sys.argv:
        sys.argv.remove('-d')
        pdb.set_trace()
    testhelp.main(sys.argv)