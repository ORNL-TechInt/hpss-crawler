import pexpect
import sys

# -----------------------------------------------------------------------------
class HSI(object):
    # -------------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        self.prompt = "]:"
        self.verbose = False
        
        cmdopts = " ".join(args)
        for key in kwargs:
            setattr(self, key, kwargs[key])

        cmd = "hsi " + cmdopts
        if hasattr(self, 'timeout'):
            self.xobj = pexpect.spawn(cmd, timeout=self.timeout)
        else:
            self.xobj = pexpect.spawn(cmd)

        if self.verbose:
            self.xobj.logfile = sys.stdout
            
        self.xobj.expect(self.prompt)
        
    # -------------------------------------------------------------------------
    def lscos(self):
        self.xobj.sendline("lscos")
        self.xobj.expect(self.prompt)
        return self.xobj.before
    
    # -------------------------------------------------------------------------
    def quit(self):
        self.xobj.sendline("quit")
        self.xobj.expect(pexpect.EOF)
        self.xobj.close()

