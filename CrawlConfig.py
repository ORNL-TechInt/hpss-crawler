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
        rval.set('crawler', 'loadtime', str(time.time()))
        get_config._config = rval
        
    return rval

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
    def get_time(self, section, option, default=None, logger=None):
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
            [(mag, unit)] = re.findall('(\d+)\s*(\w*)', spec)
            mult = self.map_time_unit(unit)
            rval = int(mag) * mult
        except ConfigParser.NoOptionError as e:
            if default != None:
                rval = default
                if logger != None:
                    logger.info(str(e) + '; using default value %d' % default)
            else:
                raise
        except ConfigParser.NoSectionError as e:
            if default != None:
                rval = default
                if logger != None:
                    log.info(str(e) + '; using default value %d' % default)
            else:
                raise

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

# -----------------------------------------------------------------------------
launch_dir = os.path.abspath(os.path.dirname(sys.argv[0]))
if __name__ == '__main__':
    toolframe.ez_launch(test='CrawlConfigTest',
                        logfile='crawl_test.log')
