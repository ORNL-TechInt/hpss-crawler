import CrawlConfig
import os
import pexpect
import pwd
import re
import sys
import time
import traceback as tb
import util


# -----------------------------------------------------------------------------
class HSIerror(Exception):
    """
    This class is used to return HSI errors to the application
    """
    # -------------------------------------------------------------------------
    def __init__(self, value):
        """
        Set the value for the exception. It should be a string.
        """
        self.value = str(value)

    # -------------------------------------------------------------------------
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
               "checksum not set",
               "HPSS_ESYSTEM"]

    # -------------------------------------------------------------------------
    def __init__(self, connect=True, *args, **kwargs):
        """
        Initialize the object
        """
        self.prompt = "]:"
        self.verbose = False
        self.unavailable = False
        self.xobj = None
        self.timeout = 60

        cmdopts = " ".join(args)
        for key in kwargs:
            setattr(self, key, kwargs[key])

        cfg = CrawlConfig.get_config()
        if not hasattr(self, 'reset_atime'):
            self.reset_atime = cfg.getboolean('cv', 'reset_atime')

        if not hasattr(self, 'hash_algorithm'):
            self.hash_algorithm = cfg.get_d('cv', 'hash_algorithm', None)

        self.cmd = "./hsi " + cmdopts
        if connect:
            self.connect()

    # -------------------------------------------------------------------------
    def before(self):
        """
        Return the before attribute of the underlying pexpect object
        """
        return self.xobj.before

    # -------------------------------------------------------------------------
    def chdir(self, dirname):
        """
        Change directories in HPSS
        """
        self.xobj.sendline("cd %s" % dirname)
        self.xobj.expect(self.prompt)
        return self.xobj.before

    # -------------------------------------------------------------------------
    def connect(self):
        """
        Connect to HPSS
        """
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
        rval = ""
        for path in pathlist:
            if self.reset_atime:
                prev_time = self.access_time(path)

            if self.hash_algorithm is None:
                cmd = "hashcreate %s" % path
            else:
                cmd = "hashcreate -H %s %s" % (self.hash_algorithm, path)
            self.xobj.sendline(cmd)
            which = self.xobj.expect([self.prompt, pexpect.TIMEOUT] +
                                     self.hsierrs)
            while which == 1 and 1 < len(self.xobj.before):
                CrawlConfig.log("got a timeout, continuing because before " +
                                "is not empty and does not contain an error")
                rval += self.xobj.before
                which = self.xobj.expect([self.prompt, pexpect.TIMEOUT] +
                                         self.hsierrs)
            rval += self.xobj.before
            if 1 == which:
                rval += " TIMEOUT"
            elif 0 != which:
                rval += " ERROR"

            if self.reset_atime:
                self.touch(path, when=prev_time)

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
            if self.reset_atime:
                prev_time = self.access_time(path)

            self.xobj.sendline("hashverify %s" % path)
            which = self.xobj.expect([self.prompt, pexpect.TIMEOUT] +
                                     self.hsierrs)
            while which == 1 and 1 < len(self.xobj.before):
                CrawlConfig.log("got a timeout, continuing because before " +
                                "is not empty and does not contain an error")
                rval += self.xobj.before
                which = self.xobj.expect([self.prompt, pexpect.TIMEOUT] +
                                         self.hsierrs)
            rval += self.xobj.before
            if 1 == which:
                rval += " TIMEOUT"
            elif 0 != which:
                rval += " ERROR"

            if self.reset_atime:
                self.touch(path, when=prev_time)
        return rval

    # -------------------------------------------------------------------------
    def access_time(self, pathname=''):
        """
        Call ls_access() and convert the result to an epoch time
        """
        result = self.ls_access(pathname)

        # get the list of month names and corresponding month numbers
        month = util.month_dict()

        # build the regular expression and parse the string
        rgx = ('(' + '|'.join(month.keys()) + ')' +
               "\s+(\d+)\s+(\d+):(\d+):(\d+)\s+(\d+)")
        x = re.findall(rgx, result)

        # peel the list
        z = x[0]

        # convert the matches to ints and put them in the proper order
        # (y, m, d, h, m, s)
        dt = [int(x) for x in
              [z[5], month[z[0]], z[1], z[2], z[3], z[4], 0, 0, 0]]

        # construct a candidate date
        epoch = time.mktime(dt)

        # run it through localtime to find out whether DST is set or not
        q = time.localtime(epoch)
        dt[-1] = q[-1]

        # now compute the correct epoch time with the correct dst setting
        epoch = time.mktime(dt)

        return epoch

    # -------------------------------------------------------------------------
    def ls_access(self, pathname=''):
        """
        Return the result of 'ls -lDTr *pathname*'
        """
        self.xobj.sendline("ls -lDTr %s" % pathname)
        self.xobj.expect(self.prompt)
        return self.xobj.before

    # -------------------------------------------------------------------------
    def lscos(self):
        """
        Retrieve the COS descriptive info from HPSS and return it
        """
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
        """
        Return the process id of the underlying hsi process
        """
        return self.xobj.pid

    # -------------------------------------------------------------------------
    def quit(self):
        """
        All done here. Let's bail.
        """
        try:
            pid = self.xobj.pid
            self.xobj.sendline("quit")
            self.xobj.expect([pexpect.EOF, pexpect.TIMEOUT])
            self.xobj.close()
            CrawlConfig.log("Closing hsi process %d" % pid)
        except OSError as e:
            tbstr = tb.format_exc()
            CrawlConfig.log("Ignoring OSError '%s'" % str(e))
            for line in tbstr.split("\n"):
                CrawlConfig.log(line)

    # -------------------------------------------------------------------------
    def touch(self, filename, when=None):
        """
        Update the atime of *filename* with *when*
        """
        if when is None:
            return ""

        cmd = "touch -a -t %s %s" % (self.touch_format(when), filename)
        self.xobj.sendline(cmd)
        self.xobj.expect(self.prompt)
        return self.xobj.before

    # -------------------------------------------------------------------------
    def touch_format(self, when):
        """
        Return *when* in the format touch expects
        """
        return time.strftime("%Y%m%d%H%M.%S", time.localtime(when))
