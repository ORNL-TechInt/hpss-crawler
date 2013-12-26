import pexpect
import sys
import util

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
        self.xobj = None
        
        cmdopts = " ".join(args)
        for key in kwargs:
            setattr(self, key, kwargs[key])

        self.cmd = "hsi " + cmdopts
        if connect:
            self.connect()

    # -------------------------------------------------------------------------
    def chdir(self, dirname):
        self.xobj.sendline("cd %s" % dirname)
        self.xobj.expect(self.prompt)
        return self.xobj.before

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
                                  "connect: Connection refused",
                                  "hpssex_OpenConnection: unable to obtain " +
                                  "remote site info"])
        if 0 != which or self.unavailable:
            raise HSIerror("HPSS Unavailable")
        
    # -------------------------------------------------------------------------
    def hashcreate(self, pathnames):
        """
        Argument pathnames should reference be one or more files. It may be a
        string containing one or more space separated file paths, or a list of
        one or more file paths.
        """
        if type(pathnames) == str:
            pathlist = pathnames.split()
        elif type(pathnames) == list:
            pathlist = pathnames
        else:
            raise HSIerror("%s: Invalid argument ('%s')" %
                           (util.my_name(), pathnames))
        # self.xobj.expect(self.prompt)
        rval = ""
        for path in pathlist:
            self.xobj.sendline("hashcreate %s" % path)
            self.xobj.expect(self.prompt)
            rval += self.xobj.before
        return rval
    
    # -------------------------------------------------------------------------
    def hashdelete(self, pathnames):
        """
        Argument pathnames should reference be one or more files. It may be a
        string containing one or more space separated file paths, or a list of
        one or more file paths.
        """
        if type(pathnames) == str:
            self.xobj.sendline("hashdelete %s" % pathnames)
        elif type(pathnames) == list:
            self.xobj.sendline("hashdelete %s" % " ".join(pathnames))
        else:
            raise HSIerror("%s: Invalid argument ('%s')" %
                           (util.my_name(), pathnames))
        self.xobj.expect(self.prompt)
        return self.xobj.before
    
    # -------------------------------------------------------------------------
    def hashlist(self, pathnames):
        """
        Argument pathnames should reference be one or more files. It may be a
        string containing one or more space separated file paths, or a list of
        one or more file paths.
        """
        if type(pathnames) == str:
            self.xobj.sendline("hashlist %s" % pathnames)
        elif type(pathnames) == list:
            self.xobj.sendline("hashlist %s" % " ".join(pathnames))
        else:
            raise HSIerror("%s: Invalid argument ('%s')" %
                           (util.my_name(), pathnames))
        self.xobj.expect(self.prompt)
        return self.xobj.before
    
    # -------------------------------------------------------------------------
    def hashverify(self, pathnames):
        """
        Argument pathnames should reference be one or more files. It may be a
        string containing one or more space separated file paths, or a list of
        one or more file paths.
        """
        if type(pathnames) == str:
            pathlist = pathnames.split()
        elif type(pathnames) == list:
            pathlist = pathnames
        else:
            raise HSIerror("%s: Invalid argument ('%s')" %
                           (util.my_name(), pathnames))

        rval = ""
        for path in pathlist:
            self.xobj.sendline("hashverify %s" % path)
            self.xobj.expect(self.prompt)
            rval += self.xobj.before
        return rval
    
    # -------------------------------------------------------------------------
    def lscos(self):
        self.xobj.sendline("lscos")
        self.xobj.expect(self.prompt)
        return self.xobj.before
    
    # -------------------------------------------------------------------------
    def lsP(self, pathnames=''):
        """
        Argument pathnames should reference be zero or more files. It may be a
        string containing one or more space separated file paths, or a list of
        one or more file paths.
        """
        if type(pathnames) == str:
            parg = pathnames
        elif type(pathnames) == list:
            parg = " ".join(pathnames)
        else:
            raise HSIerror("%s: Invalid argument ('%s')" %
                           (util.my_name(), pathnames))

        self.xobj.sendline("ls -P %s" % parg)
        self.xobj.expect(self.prompt)
        return self.xobj.before
    
    # -------------------------------------------------------------------------
    def quit(self):
        self.xobj.sendline("quit")
        self.xobj.expect(pexpect.EOF)
        self.xobj.close()

