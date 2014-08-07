#!/usr/bin/env python

import copy
import logging
import logging.handlers as logh
import os
import pdb
import re
import shutil
import socket
import sys
import time
import traceback as tb


# -----------------------------------------------------------------------------
class Chdir(object):
    """
    This class allows for doing the following:

        with Chdir('/some/other/directory'):
            assert(in '/some/other/directory')
            do_stuff()
        assert(back at our starting point)

    No matter what happens in do_stuff(), we're guaranteed that at the assert,
    we'll be back in the directory we started from.
    """

    # ------------------------------------------------------------------------
    def __init__(self, target):
        """
        This is called at instantiattion. Here we just initialize.
        """
        self.start = os.getcwd()
        self.target = target

    # ------------------------------------------------------------------------
    def __enter__(self):
        """
        This is called as control enters the with block. We jump to the target
        directory.
        """
        os.chdir(self.target)
        return self.target

    # ------------------------------------------------------------------------
    def __exit__(self, type, value, traceback):
        """
        This is called as control leaves the with block. We jump back to our
        starting directory.
        """
        os.chdir(self.start)


# -----------------------------------------------------------------------------
class ArchiveLogfileHandler(logh.RotatingFileHandler, object):
    """
    This an augmented RotatingFileHandler. At initialization, it accepts an
    archive directory, which is not passed along to the super. At rollover
    time, it also copies the rolled over file to the archive directory with a
    name based on the first and last timestamp in the file.
    """
    # ------------------------------------------------------------------------
    def __init__(self, filename, **kwargs):
        """
        Handle archdir and remove it. Let super() deal with the rest of them.
        """
        if ('archdir' in kwargs):
            if kwargs['archdir'] != '':
                self.archdir = kwargs['archdir']
            del kwargs['archdir']
        super(ArchiveLogfileHandler, self).__init__(filename, **kwargs)

    # ------------------------------------------------------------------------
    def doRollover(self):
        """
        After the normal rollover when a log file fills, we want to copy the
        newly rolled over file (my_log.1) to the archive directory. If an
        archive directory has not been set, we behave just like our parent and
        don't archive the log file at all.

        The copied file will be named my_log.<start-date>-<end-date>
        """
        super(ArchiveLogfileHandler, self).doRollover()

        try:
            archdir = self.archdir
        except AttributeError:
            return

        path1 = self.baseFilename + ".1"
        target = "%s/%s.%s-%s" % (archdir,
                                  os.path.basename(self.baseFilename),
                                  date_start(path1),
                                  date_end(path1))
        if not os.path.isdir(archdir):
            os.makedirs(archdir)
        shutil.copy2(path1, target)


# -----------------------------------------------------------------------------
class RRfile(object):
    """
    This is a thin wrapper around the file type that adds a reverse read method
    so a file can be read backwards.
    """
    # ------------------------------------------------------------------------
    def __init__(self, filename, mode):
        """
        Initialize the file -- open it and position for the first read.
          chunk: how many bytes to read at a time
          bof: beginning of file, are we done yet? -- analogous to eof
          f: file handle
        """
        self.chunk = 128
        self.bof = False
        self.f = open(filename, mode)
        self.f.seek(0, os.SEEK_END)
        size = self.f.tell()
        if self.chunk < size:
            self.f.seek(-self.chunk, os.SEEK_CUR)
        else:
            self.f.seek(0, os.SEEK_SET)

    # ------------------------------------------------------------------------
    @classmethod
    def open(cls, filename, mode):
        """
        This is how we get a new one of these.
        """
        new = RRfile(filename, mode)
        return new

    # ------------------------------------------------------------------------
    def close(self):
        """
        Close the file this object contains.
        """
        self.f.close()

    # ------------------------------------------------------------------------
    def revread(self):
        """
        Read backwards on the file in chunks defined by self.chunk. We start
        out at the place where we should read next. After reading, we seek back
        1.5 * self.chunk so our next read will overlap the last one. This is so
        that date expressions that span a read boundary will be kept together
        on the next read.
        """
        if self.bof:
            return ''

        if self.f.tell() == 0:
            self.bof = True

        rval = self.f.read(self.chunk)
        try:
            self.f.seek(-self.chunk, os.SEEK_CUR)
            self.f.seek(-self.chunk/2, os.SEEK_CUR)
        except IOError:
            self.f.seek(0, os.SEEK_SET)

        return rval


# -----------------------------------------------------------------------------
def abspath(relpath):
    """
    Convenience wrapper for os.path.abspath()
    """
    return os.path.abspath(relpath)


# -----------------------------------------------------------------------------
def basename(path):
    """
    Convenience wrapper for os.path.basename()
    """
    return os.path.basename(path)


# -----------------------------------------------------------------------------
def dirname(path):
    """
    Convenience wrapper for os.path.dirname()
    """
    return os.path.dirname(path)


# -----------------------------------------------------------------------------
def expand(path):
    """
    Expand ~user and environment variables in a string
    """
    def parse(var):
        z = re.search("([^$]*)(\$({([^}]*)}|\w*))(.*)", var)
        if z:
            return(z.groups()[0],
                   z.groups()[3] or z.groups()[2],
                   z.groups()[-1])
        else:
            return(None, None, None)

    rval = os.path.expanduser(os.path.expandvars(path))
    while '$' in rval:
        pre, var, post = parse(rval)
        if ':' in var:
            vname, vdef = var.split(":")
            vval = os.getenv(vname, vdef[1:])
        else:
            vval = os.getenv(var, "")
        rval = pre + os.path.expanduser(vval) + post
    return rval


# -----------------------------------------------------------------------------
def pathjoin(a, *p):
    """
    Convenience wrapper for os.path.join()
    """
    return os.path.join(a, *p)


# -----------------------------------------------------------------------------
def conditional_rm(filepath, tree=False):
    """
    We want to delete filepath but we don't want to generate an error if it
    doesn't exist. Return the existence value of filepath at call time.

    If tree is true, the caller is saying that he knows filepath is a directory
    that may not be empty and he wants to delete it regardless. If the caller
    does not specify tree and the target is a non-empty directory, this call
    will fail.
    """
    rv = False
    if os.path.islink(filepath):
        rv = True
        os.unlink(filepath)
    elif os.path.isdir(filepath):
        rv = True
        if tree:
            shutil.rmtree(filepath)
        else:
            os.rmdir(filepath)
    elif os.path.exists(filepath):
        rv = True
        os.unlink(filepath)
    return rv


# -----------------------------------------------------------------------------
def contents(filename, string=True):
    """
    Return the contents of the file. If string is True, we return a string,
    otherwise a list.
    """
    f = open(filename, 'r')
    if string:
        rval = f.read()
    else:
        rval = f.readlines()
    f.close()
    return rval


# ------------------------------------------------------------------------------
def csv_list(value, delimiter=","):
    """
    Split a string on a delimiter and return the resulting list, stripping away
    whitespace.
    """
    rval = [x.strip() for x in value.split(delimiter)]
    return rval


# -----------------------------------------------------------------------------
def daybase(epoch):
    """
    Given an epoch time, return the beginning of the day containing the input.
    """
    tm = time.localtime(epoch)
    return time.mktime([tm.tm_year, tm.tm_mon, tm.tm_mday,
                        0, 0, 0,
                        tm.tm_wday, tm.tm_yday, tm.tm_isdst])


# -----------------------------------------------------------------------------
def dispatch(modname, prefix, args):
    """
    Look in module *modname* for routine *prefix*_*args*[1]. Call it with
    *args*[2:].
    """
    mod = sys.modules[modname]
    if len(args) < 2:
        dispatch_help(mod, prefix)
    elif len(args) < 3 and args[1] == 'help':
        dispatch_help(mod, prefix)
    elif args[1] == 'help':
        dispatch_help(mod, prefix, args[2])
    else:
        fname = "_".join([prefix, args[1]])
        func = getattr(mod, fname)
        func(args[2:])


# -----------------------------------------------------------------------------
def dispatch_help(mod, prefix, cmd=None):
    if cmd is not None:
        func = getattr(mod, "_".join([prefix, cmd]))
        print func.__doc__
    else:
        print("")
        for fname in [x for x in dir(mod) if x.startswith(prefix)]:
            func = getattr(mod, fname)
            try:
                hstr = func.__doc__.split("\n")[0]
            except AttributeError:
                raise HpssicError(
                    "Function '%s' seems to be missing a docstring" % fname)
            print "    " + hstr
        print("")


# ------------------------------------------------------------------------------
def env_update(cfg):
    """
    Update the environment based on the contents of the 'env' section of the
    config object.
    """
    if not cfg.has_section('env'):
        return

    for var in cfg.options('env'):
        uvar = var.upper()
        value = re.sub("\n\s*", "", cfg.get('env', var))
        pre = os.getenv(uvar)
        if pre is not None and value.startswith('+'):
            os.environ[uvar] = ':'.join(os.environ[uvar].split(':') +
                                        value[1:].split(':'))
        elif value.startswith('+'):
            os.environ[uvar] = value[1:]
        else:
            os.environ[uvar] = value


# -----------------------------------------------------------------------------
def epoch(ymdhms):
    """
    Given a string containing a date and/or time, attempt to parse it into an
    epoch time.
    """
    fmts = ["%Y.%m%d %H:%M:%S",
            "%Y.%m%d.%H.%M.%S",
            "%Y.%m%d %H:%M",
            "%Y.%m%d.%H.%M",
            "%Y.%m%d %H",
            "%Y.%m%d.%H",
            "%Y.%m%d",
            ]
    fp = copy.copy(fmts)
    rval = None
    while rval is None:
        try:
            rval = time.mktime(time.strptime(ymdhms, fp.pop(0)))
        except ValueError:
            rval = None
        except IndexError:
            if ymdhms.isdigit():
                rval = int(ymdhms)
            else:
                err = ("The date '%s' does not match any of the formats: %s" %
                       (ymdhms, fmts))
                raise StandardError(err)

    return rval


# -----------------------------------------------------------------------------
def filename():
    """
    Return the name of the file where the currently running code resides.
    """
    return sys._getframe(1).f_code.co_filename


# -----------------------------------------------------------------------------
def hostname(long=False):
    """
    Return the name of the current host.
    """
    if long:
        rval = socket.gethostname()
    else:
        rval = socket.gethostname().split('.')[0]
    return rval


# -----------------------------------------------------------------------------
def git_repo(path):
    """
    If path is inside a git repo (including the root), return the root of the
    git repo. Otherwise, return ''
    """
    dotgit = pathjoin(path, ".git")
    while not os.path.exists(dotgit) and path != "/":
        path = dirname(path)
        dotgit = pathjoin(path, ".git")

    return path.rstrip('/')


# -----------------------------------------------------------------------------
def line_quote(value):
    """
    Wrap a set of lines with line-oriented quotes (three double quotes in a
    row).
    """
    if type(value) == str and value.startswith("'"):
        rv = value.strip("'")
    elif type(value) == str and value.startswith('"'):
        rv = value.strip('"')
    else:
        rv = value
    return '\n"""\n%s\n"""' % str(rv)


# -----------------------------------------------------------------------------
def lineno():
    """
    Return the line number of the file where the currently running code
    resides.
    """
    return sys._getframe(1).f_lineno


# -----------------------------------------------------------------------------
def memoize(f):
    cache = {}

    # -------------------------------------------------------------------------
    def helper(x):
        try:
            return cache[x]
        except KeyError:
            cache[x] = f(x)
            return cache[x]
    return helper


# -----------------------------------------------------------------------------
def my_name():
    """
    Return the caller's name
    """
    return sys._getframe(1).f_code.co_name


# -----------------------------------------------------------------------------
def pop0(list):
    try:
        rval = list.pop(0)
    except IndexError:
        rval = None
    return rval


# -----------------------------------------------------------------------------
def raiseError(record):
    """
    This is used in the log file handler to cause errors in logging to get
    pushed up the stack so we see them.
    """
    raise


# -----------------------------------------------------------------------------
def realpath(fakepath):
    """
    Convenience wrapper for os.path.realpath()
    """
    return os.path.realpath(fakepath)


# -----------------------------------------------------------------------------
def rgxin(needle, haystack):
    """
    Return True if the regexp needle matches haystack.
    """
    return bool(re.search(needle, haystack))


default_logfile_name = "/var/log/crawl.log"


# -----------------------------------------------------------------------------
def setup_logging(logfile='',
                  logname='crawl',
                  maxBytes=10*1024*1024,
                  backupCount=5,
                  bumper=True,
                  archdir=''):
    """
    Create a new logger and return the object
    """
    if logfile == '':
        logfile = default_logfile_name

    logfile = expand(logfile)

    rval = logging.getLogger(logname)
    rval.setLevel(logging.INFO)
    host = hostname()
    if rval.handlers != [] and logfile != rval.handlers[0].baseFilename:
        rval.handlers[0].close()
        del rval.handlers[0]
    if rval.handlers == []:
        if maxBytes == 0:
            maxBytes = 10*1024*1024
        if backupCount == 0:
            backupCount = 1
        done = False
        while not done:
            try:
                fh = ArchiveLogfileHandler(logfile,
                                           maxBytes=maxBytes,
                                           backupCount=backupCount,
                                           archdir=archdir)
                done = True
            except:
                if logfile == default_logfile_name:
                    logfile = "/tmp/%s" % os.path.basename(logfile)
                else:
                    raise

        strfmt = "%" + "(asctime)s [%s] " % host + "%" + "(message)s"
        fmt = logging.Formatter(strfmt, datefmt="%Y.%m%d %H:%M:%S")
        fh.setFormatter(fmt)
        fh.handleError = raiseError

        rval.addHandler(fh)
    if bumper:
        rval.info('-' * (55 - len(host)))
    return rval


# -----------------------------------------------------------------------------
def date_parse(data, idx):
    """
    Compile and cache the regexp for parsing dates from log files.
    """
    try:
        rgx = date_parse._tsrx
    except AttributeError:
        date_parse._fail = "yyyy.mmmm.hhmm"
        rgx_s = r"^(\d{4})\.(\d{4})\s+\d{2}:\d{2}:\d{2}"
        date_parse._tsrx = re.compile(rgx_s, re.MULTILINE)
        rgx = date_parse._tsrx

    q = rgx.findall(data)
    if q is None or q == []:
        rval = date_parse._fail
    else:
        rval = "%s.%s" % (q[idx][0], q[idx][1])
    return rval


# -----------------------------------------------------------------------------
def date_end(filename):
    """
    Read filename and return the last timestamp.
    """
    rval = date_parse('', 0)
    f = RRfile.open(filename, 'r')

    data = f.revread()
    while rval == date_parse._fail and data != '':
        rval = date_parse(data, -1)
        data = f.revread()

    f.close()
    return rval


# -----------------------------------------------------------------------------
def date_start(filename):
    """
    Read filename and return the first timestamp.
    """
    f = open(filename, 'r')
    # initialize rval to date_parse._fail so the while condition will start out
    # being true
    rval = date_parse('', 0)
    line = f.readline()
    while rval == date_parse._fail and line != '':
        rval = date_parse(line, 0)
        line = f.readline()

    f.close()
    return rval


# -----------------------------------------------------------------------------
def ymdhms(epoch):
    """
    Format an epoch time into YYYY.MMDD HH:MM:SS.
    """
    return time.strftime("%Y.%m%d %H:%M:%S",
                         time.localtime(epoch))


# -----------------------------------------------------------------------------
class HpssicError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)
