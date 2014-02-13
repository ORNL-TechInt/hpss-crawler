import os
import pexpect
import pwd
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
    hsierrs = ["Aborting transfer",
               "HPSS Unavailable",
               "connect: Connection refused",
               "hpssex_OpenConnection: unable to obtain " +
               "remote site info",
               "Error -?\d+ on transfer",
               "HPSS_ESYSTEM"]
    
    # -------------------------------------------------------------------------
    def __init__(self, connect=True, *args, **kwargs):
        self.prompt = "]:"
        self.verbose = False
        self.unavailable = False
        self.xobj = None
        self.timeout = 60
        
        cmdopts = " ".join(args)
        for key in kwargs:
            setattr(self, key, kwargs[key])

        self.cmd = "./hsi " + cmdopts
        if connect:
            self.connect()

    # -------------------------------------------------------------------------
    def before(self):
        return self.xobj.before

    # -------------------------------------------------------------------------
    def chdir(self, dirname):
        self.xobj.sendline("cd %s" % dirname)
        self.xobj.expect(self.prompt)
        return self.xobj.before

    # -------------------------------------------------------------------------
    def connect(self):
        self.xobj = pexpect.spawn(self.cmd, timeout=self.timeout)
        if self.verbose:
            self.xobj.logfile = open("hsi.out", 'a')
        which = self.xobj.expect([self.prompt] + self.hsierrs)
        if 0 != which or self.unavailable:
            raise HSIerror("HPSS Unavailable")
        
    # -------------------------------------------------------------------------
    def hashcreate(self, pathnames):
        """
        Argument pathnames should reference one or more files. It may be a
        string containing one or more space separated file paths, or a list of
        one or more file paths. If it has type unicode, it will be encoded to
        'ascii' before being treated as a string.
        """
        if type(pathnames) == str:
            pathlist = pathnames.split()
        elif type(pathnames) == list:
            pathlist = pathnames
        elif type(pathnames) == unicode:
            pathlist = pathnames.encode('ascii', 'ignore').split()
        else:
            raise HSIerror("%s: Invalid argument (%s: '%s')" %
                           (util.my_name(), type(pathnames), pathnames))
        # self.xobj.expect(self.prompt)
        rval = ""
        for path in pathlist:
            self.xobj.sendline("hashcreate %s" % path)
            which = self.xobj.expect([self.prompt, pexpect.TIMEOUT] +
                                     self.hsierrs)
            rval += self.xobj.before
            if 1 == which:
                rval += " TIMEOUT"
            elif 0 != which:
                rval += " ERROR"
        return rval
    
    # -------------------------------------------------------------------------
    def hashdelete(self, pathnames):
        """
        Argument pathnames should reference be one or more files. It may be a
        string containing one or more space separated file paths, or a list of
        one or more file paths.
        """
        if type(pathnames) == str:
            pargs = pathnames
        elif type(pathnames) == list:
            pargs = " ".join(pathnames)
        elif type(pathnames) == unicode:
            pargs = pathname.encode('ascii', 'ignore')
        else:
            raise HSIerror("%s: Invalid argument (%s: '%s')" %
                           (util.my_name(), type(pathnames), pathnames))

        self.xobj.sendline("hashdelete %s" % pargs)
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
            pargs = pathnames
        elif type(pathnames) == list:
            pargs = " ".join(pathnames)
        elif type(pathnames) == unicode:
            pargs = pathnames.encode('ascii', 'ignore')
        else:
            raise HSIerror("%s: Invalid argument (%s: '%s')" %
                           (util.my_name(), type(pathnames), pathnames))

        self.xobj.sendline("hashlist %s" % pargs)
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
        elif type(pathnames) == unicode:
            pathlist = pathnames.encode('ascii', 'ignore').split()
        else:
            raise HSIerror("%s: Invalid argument (%s: '%s')" %
                           (util.my_name(), type(pathnames), pathnames))

        rval = ""
        for path in pathlist:
            self.xobj.sendline("hashverify %s" % path)
            which = self.xobj.expect([self.prompt, pexpect.TIMEOUT] +
                                     self.hsierrs)
            while which == 1 and 1 < len(self.xobj.before):
                rval += self.xobj.before
                which = self.xobj.expect([self.prompt, pexpect.TIMEOUT] +
                                         self.hsierrs)
            if 1 == which:
                rval += " TIMEOUT"
            elif 0 != which:
                rval += " ERROR"
        return rval
    
    # -------------------------------------------------------------------------
    def lscos(self):
        self.xobj.sendline("lscos")
        self.xobj.expect(self.prompt)
        return self.xobj.before
    
    # -------------------------------------------------------------------------
    def lsP(self, pathnames=''):
        """
        Argument pathnames should reference zero or more files. It may be a
        string containing zero or more space separated file paths, or a list of
        zero or more file paths.
        """
        if type(pathnames) == str:
            parg = pathnames
        elif type(pathnames) == list:
            parg = " ".join(pathnames)
        elif type(pathnames) == unicode:
            parg = pathnames.encode('ascii', 'ignore')
        else:
            raise HSIerror("%s: Invalid argument (%s: '%s')" %
                           (util.my_name(), type(pathnames), pathnames))

        self.xobj.sendline("ls -P %s" % parg)
        self.xobj.expect(self.prompt)
        return self.xobj.before
    
    # -------------------------------------------------------------------------
    def pid(self):
        return self.xobj.pid

    # -------------------------------------------------------------------------
    def quit(self):
        self.xobj.sendline("quit")
        self.xobj.expect([pexpect.EOF, pexpect.TIMEOUT])
        self.xobj.close()

