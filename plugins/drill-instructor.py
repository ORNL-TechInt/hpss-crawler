import ConfigParser
import os
import pexpect
import sys
import time

def main(cfg):
    clog = sys.modules['__main__'].get_logger()
    clog.info("drill-instructor: cwd = %s" % os.getcwd())
    hsi_prompt = "]:"

    dicfg = ConfigParser.ConfigParser()
    dicfg.read('plugins/drill-instructor.cfg')

    # dump drill-instructor config to log
    for s in dicfg.sections():
        clog.info("drill-instructor: [%s]" % s)
        for o in dicfg.options(s):
            clog.info("drill-instructor: %s = %s" % (o, dicfg.get(s, o)))

    
    S = pexpect.spawn("/opt/public/bin/hsi")
    S.logfile = f = open("hsi.out", 'a')

    for s in dicfg.sections():
        S.expect(hsi_prompt)
        S.sendline("ls -X %s" % s)

        dicfg.set(s, 'last-check', time.strftime("%Y.%m%d %H:%M:%S"))

    S.expect(hsi_prompt)
    S.sendline("quit")
    S.expect(pexpect.EOF)

    dicfg.write(open('plugins/drill-instructor.cfg', 'w'))
    
    S.logfile.close()
    S.close()
    
