#!/usr/bin/env python

import logging
import os
import pdb
import socket
import sys
import time
import toolframe

# -----------------------------------------------------------------------------
def contents(filename):
    """
    Return the contents of the file as a string
    """
    f = open(filename, 'r')
    rval = f.read()
    f.close()
    return rval

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

    envval = os.getenv('CRAWL_LOG')

    if cmdline != '':
        filename = cmdline
    elif cfg != None:
        try:
            filename = cfg.get('crawler', 'logpath')
        except:
            pass
    elif envval != None:
        filename = envval
    else:
        filename = '/var/log/crawl.log'
        
    try:
        rval = get_logger._logger
    except AttributeError:
        if soft:
            return None
        get_logger._logger = setup_logging(filename, 'crawl')
        rval = get_logger._logger

    return rval

# -----------------------------------------------------------------------------
def line_quote(value):
    if type(value) == str and value.startswith("'"):
        rv = value.strip("'")
    elif type(value) == str and value.startswith('"'):
        rv = value.strip('"')
    else:
        rv = value
    return '\n"""\n%s\n"""' % str(rv)

# -----------------------------------------------------------------------------
def my_name():
    """
    Return the caller's name
    """
    return sys._getframe(1).f_code.co_name

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
        raise StandardError("setup_logging: No log file name provided")
    
    rval = logging.getLogger(logname)
    rval.setLevel(logging.INFO)
    host = socket.gethostname().split('.')[0]
    if rval.handlers != [] and logfile != rval.handlers[0].baseFilename:
        rval.handlers[0].close()
        del rval.handlers[0]
    if rval.handlers == []:
        fh = logging.handlers.RotatingFileHandler(logfile,
                                                  maxBytes=10*1024*1024,
                                                  backupCount=5)
        strfmt = "%" + "(asctime)s [%s] " % host + "%" + "(message)s"
        fmt = logging.Formatter(strfmt, datefmt="%Y.%m%d %H:%M:%S")
        fh.setFormatter(fmt)

        rval.addHandler(fh)
    if bumper:
        rval.info('-' * (55 - len(host)))
    return rval

