#!/usr/bin/env python
import hpss
import pdb
import pprint
import re
import sys

# -----------------------------------------------------------------------------
def main(cfg):
    cosinfo = get_cos_info()
    pprint.pprint(cosinfo)

# -----------------------------------------------------------------------------
def get_cos_info():
    rval = {}
    H = hpss.HSI("-q", timeout=300)
    result = H.lscos()
    for line in result.split("\n"):
        x = re.match("(\d+)\s.{35}(\d+).*", line)
        if x != None:
            (cos, copies) = x.groups()
            rval[cos] = int(copies)
    H.quit()
    return rval

# -----------------------------------------------------------------------------
if __name__ == '__main__':
    main('x')
