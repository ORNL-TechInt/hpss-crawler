#!/usr/bin/env python
"""
Configuration class for crawl.py

This class is based on python's standard ConfigParser class. It adds

    1) a function to manage and return a singleton config object (get_config)

    2) sensitivity to updates to the underlying configuration file (changed)

    3) a 'time' type which shows up in the configuration file as '10 sec', '2hr',
    '7 minutes', etc., but is presented to the caller as a number of seconds.

    4) a boolean handler which returns False if the option does not exist
    (rather than throwing an exception)

"""
import ConfigParser
from ConfigParser import NoSectionError, NoOptionError
import os
import re
import stat
import StringIO
import sys
import time
import util
import warnings

# ------------------------------------------------------------------------------
def get_config(cfname='', reset=False, soft=False):
    """
    Open the config file based on cfname, $CRAWL_CONF, or the default, in that
    order. Construct a CrawlConfig object, cache it, and return it. Subsequent
    calls will retrieve the cached object unless reset=True, in which case the
    old object is destroyed and a new one is constructed.

    If reset is True and soft is True, we delete any old cached object but do
    not create a new one.

    Note that values in the default dict passed to CrawlConfig
    must be strings.
    """
    if reset:
        try:
            del get_config._config
        except AttributeError:
            pass
    
    try:
        rval = get_config._config
    except AttributeError:
        if soft:
            return None
        if cfname == '':
            envval = os.getenv('CRAWL_CONF')
            if None != envval:
                cfname = envval
    
        if cfname == '':
            cfname = 'crawl.cfg'

        if not os.path.exists(cfname):
            raise StandardError("%s does not exist" % cfname)
        elif not os.access(cfname, os.R_OK):
            raise StandardError("%s is not readable" % cfname)
        rval = CrawlConfig({'fire': 'no',
                            'frequency': '3600',
                            'heartbeat': '10'})
        rval.read(cfname)
        rval.set('crawler', 'filename', cfname)
        get_config._config = rval
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
        if soft:
            return None

    kwargs = {}
    envval = os.getenv('CRAWL_LOG')
    try:
        dcfg = get_config()
    except:
        dcfg = None
    
    # setting filename -- first, assume the default, then work down the
    # precedence stack from cmdline to cfg to environment
    filename = ''
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

        try:
            kwargs['backupCount'] = cfg.getint('crawler', 'logmax')
        except:
            pass
        
    elif envval != None:
        filename = envval
    elif dcfg != None:
        try:
            filename = dcfg.get('crawler', 'logpath')
        except:
            pass

        try:
            maxbytes = dcfg.get_size('crawler', 'logsize')
            kwargs['maxBytes'] = maxbytes
        except:
            pass

        try:
            kwargs['backupCount'] = dcfg.getint('crawler', 'logmax')
        except:
            pass
        
    try:
        rval = get_logger._logger
    except AttributeError:
        if soft:
            return None
        get_logger._logger = util.setup_logging(filename, 'crawl',
                                                bumper=False, **kwargs)
        rval = get_logger._logger

    return rval

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

# ------------------------------------------------------------------------------
class CrawlConfig(ConfigParser.ConfigParser):
    """
    See the module description for information on this class.
    """
    # -------------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        """
        Initialize the object with a default filename and load time.
        """
        self.filename = '<???>'
        self.loadtime = 0.0
        m = sys.modules[__name__]
        m.NoOptionError = m.ConfigParser.NoOptionError
        ConfigParser.ConfigParser.__init__(self, *args, **kwargs)
        pass
    
    # -------------------------------------------------------------------------
    def changed(self):
        """
        Return True if the file we were loaded from has changed since load time.
        """
        if self.filename != '<???>' and self.loadtime != 0.0:
            s = os.stat(self.filename)
            rval = (self.loadtime < s[stat.ST_MTIME])
        else:
            rval = False
        return rval
    
    # -------------------------------------------------------------------------
    def dump(self, with_defaults=False):
        """
        Write the contents of the config except for the defaults to a string and
        return the string. If with_defaults = True, include the DEFAULTS
        section.
        """
        rstr = StringIO.StringIO()
        self.write(rstr)
        rval = rstr.getvalue()
        rstr.close()
        if not with_defaults:
            rval = re.sub('\[DEFAULT\][^\[]*\[', '[', rval)
        return rval

    # -------------------------------------------------------------------------
    def get_d(self, section, option, default=None):
        try:
            value = self.get(section, option)
        except ConfigParser.NoSectionError:
            if default != None:
                value = default
            else:
                raise
        except ConfigParser.NoOptionError:
            if default != None:
                value = default
            else:
                raise
        return value

    # -------------------------------------------------------------------------
    def get_size(self, section, option, default=None):
        """
        Unit specs are case insensitive.
        
        b  -> 1
        kb -> 1000**1       kib -> 1024
        mb -> 1000**2       mib -> 1024**2
        gb -> 1000**3       gib -> 1024**3
        tb -> 1000**4       tib -> 1024**4
        pb -> 1000**5       pib -> 1024**5
        eb -> 1000**6       eib -> 1024**6
        zb -> 1000**7       zib -> 1024**7
        yb -> 1000**8       yib -> 1024**8
        """
        try:
            spec = self.get(section, option)
            [(mag, unit)] = re.findall("(\d+)\s*(\w*)", spec)
            mult = self.map_size_unit(unit)
            rval = int(mag) * mult
        except ConfigParser.NoSectionError, ConfigParser.NoOptionError:
            if default != None:
                rval = default
            else:
                raise

        return rval

    # -------------------------------------------------------------------------
    def get_time(self, section, option, default=None):
        """
        Retrieve the value of section/option. It is assumed to be a duration
        specification, like -- '10 seconds', '2hr', '7 minutes', or the like. We
        will call map_time_unit to convert the unit into a number of seconds,
        then multiply by the magnitude, and return an int number of seconds. If
        the caller specifies a default and we get a NoSectionError or
        NoOptionError, we will return the caller's default. Otherwise, we raise
        the exception.
        """
        try:
            spec = self.get(section, option)
            rval = self.to_seconds(spec)
        except ConfigParser.NoOptionError as e:
            if default != None:
                rval = default
                log(str(e) + '; using default value %d' % default)
            else:
                raise
        except ConfigParser.NoSectionError as e:
            if default != None:
                rval = default
                log(str(e) + '; using default value %d' % default)
            else:
                raise

        return rval
    
    # -------------------------------------------------------------------------
    def to_seconds(self, spec):
        [(mag, unit)] = re.findall('(\d+)\s*(\w*)', spec)
        mult = self.map_time_unit(unit)
        rval = int(mag) * mult
        return rval

    # -------------------------------------------------------------------------
    def getboolean(self, name, option):
        """
        Retrieve the value of section(name)/option as a boolean. If the option
        does not exist, catch the exception and return False.
        """
        try:
            # rval = super(CrawlConfig, self).getboolean(name, option)
            rval = ConfigParser.ConfigParser.getboolean(self, name, option)
        except ValueError:
            rval = False
        except ConfigParser.NoOptionError:
            rval = False
        return rval
    
    # -------------------------------------------------------------------------
    def load_dict(self, dict, defaults=None):
        """
        Initialize the config from dict. If one of the keys in dict is
        'defaults' or 'DEFAULTS', that sub-dict will be used to initialize the
        _defaults member
        """
        # make sure self is cleaned out
        for k in self._defaults.keys():
            del self._defaults[k]
        for s in self.sections():
            self.remove_section(s)

        # If we got defaults, set them first
        if defaults != None:
            for k in defaults.keys():
                self._defaults[k] = defaults[k]

        # Now fill the config with the material from the dict
        for s in sorted(dict.keys()):
            self.add_section(s)
            for o in sorted(dict[s].keys()):
                self.set(s, o, dict[s][o])
    
    # -------------------------------------------------------------------------
    def map_size_unit(self, spec):
        """
        b  -> 1
        kb -> 1000**1       kib -> 1024
        mb -> 1000**2       mib -> 1024**2
        gb -> 1000**3       gib -> 1024**3
        tb -> 1000**4       tib -> 1024**4
        pb -> 1000**5       pib -> 1024**5
        eb -> 1000**6       eib -> 1024**6
        zb -> 1000**7       zib -> 1024**7
        yb -> 1000**8       yib -> 1024**8
        """
        done = False
        while not done:
            try:
                rval = self._sizemap[spec]
                done = True
            except AttributeError:
                self._sizemap = {'': 1,
                                 'b': 1,
                                 'kb': 1000,
                                 'kib': 1024,
                                 'mb': 1000 * 1000,
                                 'mib': 1024 * 1024,
                                 'gb': 1000 ** 3,
                                 'gib': 1024 ** 3,
                                 'tb': 1000 ** 4,
                                 'tib': 1024 ** 4,
                                 'pb': 1000 ** 5,
                                 'pib': 1024 ** 5,
                                 'eb': 1000 ** 6,
                                 'eib': 1024 ** 6,
                                 'zb': 1000 ** 7,
                                 'zib': 1024 ** 7,
                                 'yb': 1000 ** 8,
                                 'yib': 1024 ** 8,
                                 }
                done = False
            except KeyError:
                rval = 1
                done = True

        return rval
        
    # -------------------------------------------------------------------------
    def map_time_unit(self, spec):
        """
        1s         => 1
        1 min      => 60
        2 days     => 2 * 24 * 3600
        """
        done = False
        while not done:
            try:
                rval = self._map[spec]
                done = True
            except AttributeError:
                self._map = {'': 1,
                             's': 1,
                             'sec': 1,
                             'second': 1,
                             'seconds': 1,
                             'm': 60,
                             'min': 60,
                             'minute': 60,
                             'minutes': 60,
                             'h': 3600,
                             'hr': 3600,
                             'hour': 3600,
                             'hours': 3600,
                             'd': 24 * 3600,
                             'day': 24 * 3600,
                             'days': 24 * 3600,
                             'w': 7 * 24 * 3600,
                             'week': 7 * 24 * 3600,
                             'weeks': 7 * 24 * 3600,
                             'month': 30 * 24 * 3600,
                             'months': 30 * 24 * 3600,
                             'y': 365 * 24 * 3600,
                             'year': 365 * 24 * 3600,
                             'years': 365 * 24 * 3600,
                             }
                done = False
            except KeyError:
                rval = 1
                done = True

        return rval
        
    # -------------------------------------------------------------------------
    def read(self, filename):
        """
        Read the configuration file and cache the file name and load time. Also
        read any included config files.
        """
        ConfigParser.ConfigParser.readfp(self, open(filename), filename)
        self.filename = filename
        self.loadtime = time.time()

        pending = self.update_include_list()   # creates dict self.incl
        while pending != []:
            parsed = ConfigParser.ConfigParser.read(self, pending)
            for fname in pending:
                self.incl[fname] = True
            unparsed = [x for x in pending if x not in parsed]
            if unparsed != []:
                wmsg = "Some config files not loaded: %s" % ", ".join(unparsed)
                warnings.warn(wmsg)
            pending = self.update_include_list()   # update dict self.incl
                
    # -------------------------------------------------------------------------
    def update_include_list(self):
        """
        Rummage through the config object and find all the 'include' options.
        They are added to a dict of the form

             {<filename>: <boolean>, ... }

        If <boolean> is False, the file has not yet been parsed into the config
        object. Once it is, <boolean> is set to True. We return the list of
        pending files to included, i.e., those that have not yet been.
        """
        if not hasattr(self, 'incl'):
            setattr(self, 'incl', {})

        for section in self.sections():
            for option in self.options(section):
                if option == 'include':
                    if self.get(section, option) not in self.incl:
                        self.incl[self.get(section, option)] = False

        return [x for x in self.incl if not self.incl[x]]

    # -------------------------------------------------------------------------
    def crawl_write(self, fp):
        """
        Write the config material to fp with the 'crawler' section first. fp
        must be an already open file descriptor. If there is no 'crawler'
        section, raise a NoSectionError.
        """
        if 'crawler' not in self.sections():
            raise StandardError("section 'crawler' missing from test config")

        # move 'crawler' to the beginning of the section list
        section_l = self.sections()
        section_l.remove('crawler')
        section_l = ['crawler'] + section_l

        for section in section_l:
            fp.write("[%s]\n" % section)
            for item in self.options(section):
                fp.write("%s = %s\n" % (item, self.get(section, item)))
            fp.write("\n")
