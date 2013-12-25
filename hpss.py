import pexpect
import sys

# -----------------------------------------------------------------------------
class HSIerror(Exception):
    """
    This class is used to return HSI errors to the application
    """
    def __init__(self, value):
        """
        Set the value for the exception. It should be a string.
        """
        self.value = str(value)
    def __str__(self):
        """
        Report the exception value (should be a string).
        """
        return "%s" % (str(self.value))
    
# -----------------------------------------------------------------------------
class HSI(object):
    # -------------------------------------------------------------------------
    def __init__(self, connect=True, *args, **kwargs):
        self.prompt = "]:"
        self.verbose = False
        self.unavailable = False
        
        cmdopts = " ".join(args)
        for key in kwargs:
            setattr(self, key, kwargs[key])

        self.cmd = "hsi " + cmdopts
        if connect:
            self.connect()

    # -------------------------------------------------------------------------
    def connect(self):
        if hasattr(self, 'timeout'):
            self.xobj = pexpect.spawn(self.cmd, timeout=self.timeout)
        else:
            self.xobj = pexpect.spawn(self.cmd)
        if self.verbose:
            self.xobj.logfile = sys.stdout
        which = self.xobj.expect([self.prompt,
                                  "HPSS Unavailable",
                                  "connect: Connection refused"])
        if 0 != which or self.unavailable:
            raise HSIerror("HPSS Unavailable")
        
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

