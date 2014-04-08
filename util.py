#!/usr/bin/env python

import logging
import logging.handlers
import os
import pdb
import re
import shutil
import socket
import sys
import time

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
        self.start = os.getcwd()
        self.target = target
    # ------------------------------------------------------------------------
    def __enter__(self):
        os.chdir(self.target)
        return self.target
    # ------------------------------------------------------------------------
    def __exit__(self, type, value, traceback):
        os.chdir(self.start)

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

default_logfile_name = "/var/log/crawl.log"
# ------------------------------------------------------------------------------
def get_logger(cmdline='', cfg=None, reset=False, soft=False):
    """
    Return the logging object for this process. Instantiate it if it
    does not exist already.

    cmdline contains the log file name from the command line if one was
    specified.

    cfg contains a configuration object or None.

    reset == True means that the caller wants to close any currently open log
    file and open a new one rather than returning the one that's already open.

    soft == True means that if a logger does not exist, we don't want to open
    one but return None instead. Normally, if a logger does not exist, we
    create it.
    """
    if reset:
        try:
            l = get_logger._logger
            for h in l.handlers:
                h.close()
            del get_logger._logger
        except AttributeError:
            pass

    kwargs = {}
    envval = os.getenv('CRAWL_LOG')

    # setting filename -- first, assume the default, then work down the
    # precedence stack from cmdline to cfg to environment
    filename = default_logfile_name
    if cmdline != '':
        filename = cmdline
    elif cfg != None:
        try:
            filename = cfg.get('crawler', 'logpath')
        except:
            pass
    
        try:
            maxbytes = cfg.get_size('crawler', 'logsize')
            kwargs['maxBytes'] = maxbytes
        except:
            pass
    
        if cfg.has_section('crawler'):
            if cfg.has_option('crawler', 'logmax'):
                kwargs['backupCount'] = cfg.getint('crawler', 'logmax')
        
    elif envval != None:
        filename = envval

    # if a cfg is provided, let's see if it gives us a log file size and backup
    # count
    if cfg != None:
        if cfg.has_section('crawler'):
            if cfg.has_option('crawler', 'logsize'):
                kwargs['maxBytes'] = cfg.get_size('crawler', 'logsize')
            if cfg.has_option('crawler', 'logmax'):
                kwargs['backupCount'] = cfg.getint('crawler', 'logmax')
            
    try:
        rval = get_logger._logger
    except AttributeError:
        if soft:
            return None
        get_logger._logger = setup_logging(filename, 'crawl',
                                           bumper=False, **kwargs)
        rval = get_logger._logger

    return rval

# -----------------------------------------------------------------------------
def hostname(long=False):
    if long:
        rval = socket.gethostname()
    else:
        rval = socket.gethostname().split('.')[0]
    return rval

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
def log(*args):
    """
    Here we use the same logger as the one cached in get_logger() so that if it
    is reset, all handles to it get reset.
    """
    cframe = sys._getframe(1)
    caller_name = cframe.f_code.co_name
    caller_file = cframe.f_code.co_filename
    caller_lineno = cframe.f_lineno
    fmt = (caller_name +
           "(%s:%d): " % (caller_file, caller_lineno) +
           args[0])
    nargs = (fmt,) + args[1:]
    try:
        get_logger._logger.info(*nargs)
    except AttributeError:
        get_logger._logger = get_logger()
        get_logger._logger.info(*nargs)

# -----------------------------------------------------------------------------
def filename():
    return sys._getframe(1).f_code.co_filename

# -----------------------------------------------------------------------------
def lineno():
    return sys._getframe(1).f_lineno

# -----------------------------------------------------------------------------
def my_name():
    """
    Return the caller's name
    """
    return sys._getframe(1).f_code.co_name

# -----------------------------------------------------------------------------
def raiseError(record):
    raise

# -----------------------------------------------------------------------------
def rgxin(needle, haystack):
    return bool(re.search(needle, haystack))

# -----------------------------------------------------------------------------
def setup_logging(logfile='',
                  logname='crawl',
                  maxBytes=10*1024*1024,
                  backupCount=5,
                  bumper=True):
    """
    Create a new logger and return the object
    """
    if logfile == '':
        logfile = default_logfile_name
    
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
                fh = logging.handlers.RotatingFileHandler(logfile,
                                                     maxBytes=maxBytes,
                                                     backupCount=backupCount)
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
def ymdhms(epoch):
    return time.strftime("%Y.%m%d %H:%M:%S",
                         time.localtime(epoch))

# -----------------------------------------------------------------------------
def epoch(ymdhms):
    fmts = ["%Y.%m%d %H:%M:%S",
            "%Y.%m%d",
            ]
    fp = fmts
    rval = None
    while rval is None:
        try:
            rval = time.mktime(time.strptime(ymdhms, fp.pop(0)))
        except ValueError:
            rval = None
        except IndexError:
            print("The date '%s' does not match any of the formats: %s" %
                  (ymdhms, fmts))

    return rval
