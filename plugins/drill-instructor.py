import Checkable
import ConfigParser
import CrawlConfig
import os
import pexpect
import sys
import time

def main(cfg):
    clog = sys.modules['__main__'].get_logger()
    # clog.info("drill-instructor: cwd = %s" % os.getcwd())
    hsi_prompt = "]:"

    plugdir = cfg.get('crawler', 'plugin-dir')
    dataroot = cfg.get('drill-instructor', 'dataroot')
    dbfilename = cfg.get('drill-instructor', 'dbfile')
    odds = cfg.get('drill-instructor', 'odds')
    # clog.info("drill-instructor: dataroot = %s" % dataroot)
    # clog.info("drill-instructor: dbfile = %s" % dbfilename)
    
    try:
        clist = Checkable.Checkable.get_list()
    except StandardError, e:
        if 'Please call .ex_nihilo()' in str(e):
            Checkable.Checkable.ex_nihilo(filename=dbfilename,
                                          dataroot=dataroot)
            clist = Checkable.Checkable.get_list()
        else:
            raise

    n_ops = int(cfg.get('drill-instructor', 'operations'))
        
    for op in range(n_ops):
        if 0 < len(clist):
            item = clist.pop(0)
            clog.info("drill-instructor: [%d] checking %s" %
                      (item.rowid, item.path))
            ilist = item.check(odds)
            if item.type == 'f':
                clog.info("drill-instructor: file %s: '%s'" % (item.path,
                                                               item.checksum))
            else:
                clog.info("drill-instructor: in %s, found:" % item.path)
                for n in ilist:
                    clog.info("drill-instructor: >>> %s %s %s %f" %
                              (n.path, n.type, n.checksum,
                               n.last_check))
