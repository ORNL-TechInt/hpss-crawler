import Checkable
import ConfigParser
import CrawlConfig
import os
import pexpect
import sys
import time

def main(cfg):
    clog = sys.modules['__main__'].get_logger()
    clog.info("drill-instructor: cwd = %s" % os.getcwd())
    hsi_prompt = "]:"

    plugdir = cfg.get('crawler', 'plugin-dir')
    dataroot = cfg.get('drill-instructor', 'dataroot')
    dbfilename = cfg.get('drill-instructor', 'dbfile')
    clog.info("drill-instructor: dataroot = %s" % dataroot)
    clog.info("drill-instructor: dbfile = %s" % dbfilename)
    
    try:
        clist = Checkable.Checkable.get_list()  # returns a list of Checkable objects
    except StandardError, e:
        if 'Please call .ex_nihilo()' in str(e):
            Checkable.Checkable.ex_nihilo(filename=dbfilename,
                                          dataroot=dataroot)
            clist = Checkable.Checkable.get_list()

    n_ops = int(cfg.get('drill-instructor', 'operations'))
    try:
        n_next = int(cfg.get('drill-instructor', 'next'))
    except ConfigParser.NoOptionError:
        n_next = 0

    if n_next < len(clist):
        for op in range(n_next):
            item = clist.pop(0)
        
    for op in range(n_ops):
        item = clist.pop(0)
        clog.info("drill-instructor: [%d] checking %s" % (item.rowid, item.path))
        n = item.check()
        clog.info("drill-instructor: found %d items" % len(n))
        
    cfg.set('drill-instructor', 'next', str(clist[0].rowid))
    
#     dicfg = ConfigParser.ConfigParser()
#     dicfg.read('plugins/drill-instructor.cfg')

#     # dump drill-instructor config to log
#     for s in dicfg.sections():
#         clog.info("drill-instructor: [%s]" % s)
#         for o in dicfg.options(s):
#             clog.info("drill-instructor: %s = %s" % (o, dicfg.get(s, o)))

    
#     S = pexpect.spawn("/opt/public/bin/hsi")
#     S.logfile = f = open("hsi.out", 'a')

#     for s in dicfg.sections():
#         S.expect(hsi_prompt)
#         S.sendline("ls -X %s" % s)

#         dicfg.set(s, 'last-check', time.strftime("%Y.%m%d %H:%M:%S"))

#     S.expect(hsi_prompt)
#     S.sendline("quit")
#     S.expect(pexpect.EOF)

#     dicfg.write(open('plugins/drill-instructor.cfg', 'w'))
    
#     S.logfile.close()
#     S.close()
    
