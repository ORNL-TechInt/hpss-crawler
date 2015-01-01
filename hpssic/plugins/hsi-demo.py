from hpssic import CrawlConfig
import pexpect
import sys


# -----------------------------------------------------------------------------
def main(cfg):
    """
    HSI demo
    """
    CrawlConfig.log("hsi-demo: sending output to hsi.out")
    hsi_prompt = "]:"

    S = pexpect.spawn("/opt/public/bin/hsi")
    S.logfile = f = open("hsi.out", 'a')
    S.expect(hsi_prompt)
    S.sendline("ls")

    S.expect(hsi_prompt)
    S.sendline("quit")

    S.expect(pexpect.EOF)
    S.logfile.close()
    S.close()
