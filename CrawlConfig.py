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
import copy
import os
import pdb
import re
import shutil
import stat
import StringIO
import sys
import testhelp
import time
import toolframe
import unittest

# # -----------------------------------------------------------------------------
# def main(argv):
#     """
#     Dummy main routine so we can use toolframe and testhelp
#     """
#     print("This is the package for CrawlConfig.")
#     print("Usage:")
#     print("    import CrawlConfig")
#     print("    ...")
#     print("    cfg = CrawlConfig()")
#     print("    cfg.load_dict(dict)")
#     print("    cfg.read(filename)")
#     print("    cfg.get(<section>, <option>)")
#     print("See the documentation for ConfigParser for more detail.")
    
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
        Read the configuration file and cache the file name and load time.
        """
        ConfigParser.ConfigParser.read(self, filename)
        self.filename = filename
        self.loadtime = time.time()
        
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
def setUpModule():
    """
    Set up for the tests.
    """
    testhelp.module_test_setup(CrawlConfigTest.testdir)
    
# -----------------------------------------------------------------------------
def tearDownModule():
    """
    Clean up after the tests
    """
    os.chdir(launch_dir)
    testhelp.module_test_teardown(CrawlConfigTest.testdir)
    
# -----------------------------------------------------------------------------
class CrawlConfigTest(testhelp.HelpedTestCase):
    """
    Test class for CrawlConfig
    """
    default_cfname = 'crawl.cfg'
    env_cfname = 'envcrawl.cfg'
    exp_cfname = 'explicit.cfg'
    testdir = 'test.d'
    default_logpath = '%s/test_default_hpss_crawl.log' % testdir
    cdict = {'crawler': {'plugin-dir': '%s/plugins' % testdir,
                         'logpath': default_logpath,
                         'logsize': '5mb',
                         'logmax': '5',
                         'e-mail-recipients':
                         'tbarron@ornl.gov, tusculum@gmail.com',
                         'trigger': '<command-line>',
                         'plugins': 'plugin_A',
                         },
             'plugin_A': {'frequency': '1h',
                          'operations': '15'
                          }
             }
    sample = {'crawler': {'opt1': 'foo',
                          'opt2': 'fribble',
                          'opt3': 'nice',
                          'heartbeat': '1hr',
                          'frequency': '5 min'},
              'sounds': {'duck': 'quack',
                         'dog':  'bark',
                         'hen':  'cluck'}}
    
    # -------------------------------------------------------------------------
    def test_changed(self):
        """
        Routines exercised: __init__(), changed(), load_dict(), and
        crawl_write()
        """
        cfgfile = '%s/test_changed.cfg' % self.testdir

        obj = CrawlConfig()
        obj.load_dict(self.sample)
        f = open(cfgfile, 'w')
        obj.crawl_write(f)
        f.close()

        changeable = CrawlConfig()
        self.assertEqual(changeable.filename, '<???>')
        self.assertEqual(changeable.loadtime, 0.0)
        changeable.read(cfgfile)
        self.assertEqual(changeable.changed(), False)
        time.sleep(1.0)
        f = open(cfgfile, 'a')
        f.write('\n')
        f.close()
        self.assertEqual(changeable.changed(), True)
        self.assertEqual(changeable.filename, cfgfile)
        
    # -------------------------------------------------------------------------
    def test_dump_nodef(self):
        """
        Routines exercised: __init__(), load_dict(), dump().
        """
        obj = CrawlConfig({'goose': 'honk'})
        obj.load_dict(self.sample)
        dumpstr = obj.dump()

        self.assertEqual("[DEFAULT]" in dumpstr, False)
        self.assertEqual("goose = honk" in dumpstr, False)
        self.assertEqual("[crawler]" in dumpstr, True)
        self.assertEqual("opt1 = foo" in dumpstr, True)
        self.assertEqual("opt2 = fribble" in dumpstr, True)
        self.assertEqual("opt3 = nice" in dumpstr, True)
        self.assertEqual("[sounds]" in dumpstr, True)
        self.assertEqual("dog = bark" in dumpstr, True)
        self.assertEqual("duck = quack" in dumpstr, True)
        self.assertEqual("hen = cluck" in dumpstr, True)

    # -------------------------------------------------------------------------
    def test_dump_withdef(self):
        """
        Routines exercised: __init__(), load_dict(), dump().
        """
        defaults={'goose': 'honk'}
        obj = CrawlConfig()
        # pdb.set_trace()
        obj.load_dict(self.sample, defaults)
        dumpstr = obj.dump(with_defaults=True)
        
        self.assertEqual("[DEFAULT]" in dumpstr, True)
        self.assertEqual("goose = honk" in dumpstr, True)
        self.assertEqual("[crawler]" in dumpstr, True)
        self.assertEqual("opt1 = foo" in dumpstr, True)
        self.assertEqual("opt2 = fribble" in dumpstr, True)
        self.assertEqual("opt3 = nice" in dumpstr, True)
        self.assertEqual("[sounds]" in dumpstr, True)
        self.assertEqual("dog = bark" in dumpstr, True)
        self.assertEqual("duck = quack" in dumpstr, True)
        self.assertEqual("hen = cluck" in dumpstr, True)

    # --------------------------------------------------------------------------
    def test_get_config_def_noread(self):
        """
        TEST: env not set, 'crawl.cfg' does exist but not readable

        EXP: get_config() or get_config('') should throw a
        StandardError about the file not existing or not being
        readable
        """
        get_config(reset=True, soft=True)
        self.cd(self.testdir)
        self.clear_env()
        d = copy.deepcopy(self.cdict)
        d['crawler']['filename'] = self.default_cfname
        self.write_cfg_file(self.default_cfname, self.cdict)
        os.chmod(self.default_cfname, 0000)

        # test get_config with no argument
        try:
            cfg = get_config()
            self.fail("Expected exception was not thrown")
        except AssertionError:
            raise
        except StandardError as e:
            self.expected('%s is not readable' % self.default_cfname, str(e))
        except:
            self.fail("Expected a StandardError, got %s" %
                      util.line_quote(tb.format_exc()))
        
        # test get_config with empty string argument
        try:
            cfg = get_config('')
            self.fail("Expected exception was not thrown")
        except AssertionError:
            raise
        except StandardError as e:
            self.expected('%s is not readable' % self.default_cfname, str(e))
        except:
            self.fail("Expected a StandardError, got %s" %
                      util.line_quote(tb.format_exc()))
        
    # --------------------------------------------------------------------------
    def test_get_config_def_nosuch(self):
        """
        TEST: env not set, 'crawl.cfg' does not exist

        EXP: get_config() or get_config('') should throw a
        StandardError about the file not existing or not being
        readable
        """
        get_config(reset=True, soft=True)
        self.cd(self.testdir)
        self.clear_env()
        if os.path.exists(self.default_cfname):
            os.unlink(self.default_cfname)

        # test with no argument
        try:
            cfg = get_config()
            self.fail("Expected exception was not thrown")
        except AssertionError:
            raise
        except StandardError as e:
            self.expected('%s does not exist' % self.default_cfname,
                          str(e))
        except:
            self.fail("Expected a StandardError, got %s" %
                      util.line_quote(tb.format_exc()))
        
        # test with empty string argument
        try:
            cfg = get_config('')
            self.fail("Expected exception was not thrown")
        except AssertionError:
            raise
        except StandardError as e:
            self.expected('%s does not exist' % self.default_cfname,
                          str(e))
        except:
            self.fail("Expected a StandardError, got %s" %
                      util.line_quote(tb.format_exc()))

        # tearDown will 'cd ..'
        
    # --------------------------------------------------------------------------
    def test_get_config_def_ok(self):
        """
        TEST: env not set, 'crawl.cfg' does exist =>

        EXP: get_config() or get_config('') should load the config
        """
        get_config(reset=True)
        self.cd(self.testdir)
        self.clear_env()
        d = copy.deepcopy(self.cdict)
        d['crawler']['filename'] = self.default_cfname
        self.write_cfg_file(self.default_cfname, d)
        os.chmod(self.default_cfname, 0644)

        got_exception = False
        try:
            cfg = get_config()
        except:
            got_exception = True
        self.assertEqual(got_exception, False)
        self.assertEqual(cfg.get('crawler', 'filename'), self.default_cfname)
        self.assertEqual(cfg.filename, self.default_cfname)
        
        got_exception = False
        try:
            cfg = get_config('')
        except:
            got_exception = True
        self.assertEqual(got_exception, False)
        self.assertEqual(cfg.get('crawler', 'filename'), self.default_cfname)
        self.assertEqual(cfg.filename, self.default_cfname)
        
    # --------------------------------------------------------------------------
    def test_get_config_env_noread(self):
        """
        TEST: env CRAWL_CONF='envcrawl.cfg', envcrawl.cfg exists but
        is not readable

        EXP: get_config(), get_config('') should throw a StandardError
        about the file not existing or not being readable
        """
        get_config(reset=True, soft=True)
        self.cd(self.testdir)
        os.environ['CRAWL_CONF'] = self.env_cfname
        d = copy.deepcopy(self.cdict)
        d['crawler']['filename'] = self.env_cfname
        self.write_cfg_file(self.env_cfname, d)
        os.chmod(self.env_cfname, 0000)

        try:
            cfg = get_config()
            self.fail("Expected exception was not thrown")
        except AssertionError:
            raise
        except StandardError as e:
            self.expected('%s is not readable' % self.env_cfname,
                          str(e))
        except:
            self.fail("Expected a StandardError, got %s" %
                      util.line_quote(tb.format_exc()))

        try:
            cfg = get_config('')
            self.fail("Expected exception was not thrown")
        except AssertionError:
            raise
        except StandardError as e:
            self.expected('%s is not readable' % self.env_cfname,
                          str(e))
        except:
            self.fail("Expected a StandardError, got %s" %
                      util.line_quote(tb.format_exc()))
        
    # --------------------------------------------------------------------------
    def test_get_config_env_nosuch(self):
        """
        TEST: env CRAWL_CONF='envcrawl.cfg', envcrawl.cfg does not exist
        
        EXP: get_config(), get_config('') should throw a StandardError
        about the file not existing or not being readable
        """
        get_config(reset=True, soft=True)
        self.cd(self.testdir)
        os.environ['CRAWL_CONF'] = self.env_cfname
        if os.path.exists(self.env_cfname):
            os.unlink(self.env_cfname)

        got_exception = False
        try:
            cfg = get_config()
        except StandardError as e:
            got_exception = True
            self.assertEqual(str(e),
                             '%s does not exist' %
                             self.env_cfname)
        self.assertEqual(got_exception, True)
        
        got_exception = False
        try:
            cfg = get_config('')
        except StandardError as e:
            got_exception = True
            self.assertEqual(str(e),
                             '%s does not exist' %
                             self.env_cfname)
        self.assertEqual(got_exception, True)

    # --------------------------------------------------------------------------
    def test_get_config_env_ok(self):
        """
        TEST: env CRAWL_CONF='envcrawl.cfg', envcrawl.cfg exists and
        is readable

        EXP: get_config(), get_config('') should load the config
        """
        get_config(reset=True, soft=True)
        self.cd(self.testdir)
        os.environ['CRAWL_CONF'] = self.env_cfname
        d = copy.deepcopy(self.cdict)
        d['crawler']['filename'] = self.env_cfname
        self.write_cfg_file(self.env_cfname, d)
        os.chmod(self.env_cfname, 0644)

        got_exception = False
        try:
            cfg = get_config()
        except:
            got_exception = True
        self.assertEqual(got_exception, False)
        self.assertEqual(cfg.get('crawler', 'filename'), self.env_cfname)
        
        got_exception = False
        try:
            cfg = get_config('')
        except:
            got_exception = True
        self.assertEqual(got_exception, False)
        self.assertEqual(cfg.get('crawler', 'filename'), self.env_cfname)

    # --------------------------------------------------------------------------
    def test_get_config_exp_noread(self):
        """
        TEST: env CRAWL_CONF='envcrawl.cfg', envcrawl.cfg exists and is
              readable, unreadable explicit.cfg exists

        EXP: get_config('explicit.cfg') should should throw a
             StandardError about the file not existing or not being
             readable
        """
        get_config(reset=True, soft=True)
        self.cd(self.testdir)
        os.environ['CRAWL_CONF'] = self.env_cfname
        d = copy.deepcopy(self.cdict)
        d['crawler']['filename'] = self.env_cfname
        self.write_cfg_file(self.env_cfname, d)
        os.chmod(self.env_cfname, 0644)

        d = copy.deepcopy(self.cdict)
        d['crawler']['filename'] = self.exp_cfname
        self.write_cfg_file(self.exp_cfname, d)
        os.chmod(self.exp_cfname, 0000)

        try:
            cfg = get_config(self.exp_cfname)
            self.fail("Expected exception was not thrown")
        except AssertionError:
            raise
        except StandardError as e:
            self.expected('%s is not readable' % self.exp_cfname, str(e))
        except:
            self.fail("Expected a StandardError, got %s" %
                      util.line_quote(tb.format_exc()))

    # --------------------------------------------------------------------------
    def test_get_config_exp_nosuch(self):
        """
        TEST: env CRAWL_CONF='envcrawl.cfg', envcrawl.cfg exists and
              is readable, explicit.cfg does not exist
              
        EXP: get_config('explicit.cfg') should throw a StandardError
             about the file not existing or not being readable
        """
        get_config(reset=True, soft=True)
        self.cd(self.testdir)
        os.environ['CRAWL_CONF'] = self.env_cfname
        d = copy.deepcopy(self.cdict)
        d['crawler']['filename'] = self.env_cfname
        self.write_cfg_file(self.env_cfname, d)
        os.chmod(self.env_cfname, 0644)

        if os.path.exists(self.exp_cfname):
            os.unlink(self.exp_cfname)

        try:
            cfg = get_config(self.exp_cfname)
            self.fail("Expected exception was not thrown")
        except AssertionError:
            raise
        except StandardError as e:
            self.expected('%s does not exist' % self.exp_cfname, str(e))
        except:
            self.fail("Expected a StandardError, got %s" %
                      util.line_quote(tb.format_exc()))

    # --------------------------------------------------------------------------
    def test_get_config_exp_ok(self):
        """
        TEST: env CRAWL_CONF='envcrawl.cfg', envcrawl.cfg exists and is
              readable, readable explicit.cfg does exist

        EXP: get_config('explicit.cfg') should load the explicit.cfg
        """
        get_config(reset=True, soft=True)
        self.cd(self.testdir)
        os.environ['CRAWL_CONF'] = self.env_cfname
        d = copy.deepcopy(self.cdict)
        d['crawler']['filename'] = self.env_cfname
        self.write_cfg_file(self.env_cfname, d)
        os.chmod(self.env_cfname, 0644)

        d = copy.deepcopy(self.cdict)
        d['crawler']['filename'] = self.exp_cfname
        self.write_cfg_file(self.exp_cfname, d)
        os.chmod(self.exp_cfname, 0644)

        cfg = get_config(self.exp_cfname)
        self.assertEqual(cfg.get('crawler', 'filename'), self.exp_cfname)

    # -------------------------------------------------------------------------
    def test_get_time(self):
        """
        Routines exercised: __init__(), load_dict(), get_time().
        """
        obj = CrawlConfig()
        obj.load_dict(self.sample)
        self.assertEqual(obj.get_time('crawler', 'heartbeat'), 3600)
        self.assertEqual(obj.get_time('crawler', 'frequency'), 300)

    # -------------------------------------------------------------------------
    def test_getboolean(self):
        """
        Routines exercised: getboolean().
        """
        obj = CrawlConfig()
        obj.add_section('abc')
        obj.set('abc', 'fire', 'True')
        obj.set('abc', 'other', 'False')
        self.assertEqual(obj.getboolean('abc', 'flip'), False)
        self.assertEqual(obj.getboolean('abc', 'other'), False)
        self.assertEqual(obj.getboolean('abc', 'fire'), True)
        
    # -------------------------------------------------------------------------
    def test_load_dict(self):
        """
        Routines exercised: __init__(), load_dict().
        """
        obj = CrawlConfig()

        self.assertEqual(obj.filename, '<???>')
        self.assertEqual(obj.loadtime, 0.0)
        obj.load_dict(self.sample)

        self.assertEqual(obj.filename, '<???>')
        self.assertEqual(obj.loadtime, 0.0)

        self.assertEqual('crawler' in obj.sections(), True)
        self.assertEqual('sounds' in obj.sections(), True)

        self.assertEqual('opt1' in obj.options('crawler'), True)
        self.assertEqual('opt2' in obj.options('crawler'), True)
        self.assertEqual('opt2' in obj.options('crawler'), True)

        self.assertEqual('duck' in obj.options('sounds'), True)
        self.assertEqual('dog' in obj.options('sounds'), True)
        self.assertEqual('hen' in obj.options('sounds'), True)

    # -------------------------------------------------------------------------
    def test_map_time_unit(self):
        """
        Routines exercised: __init__(), map_time_unit().
        """
        obj = CrawlConfig()
        self.assertEqual(obj.map_time_unit(''), 1)
        self.assertEqual(obj.map_time_unit('s'), 1)
        self.assertEqual(obj.map_time_unit('sec'), 1)
        self.assertEqual(obj.map_time_unit('seconds'), 1)
        self.assertEqual(obj.map_time_unit('m'), 60)
        self.assertEqual(obj.map_time_unit('min'), 60)
        self.assertEqual(obj.map_time_unit('minute'), 60)
        self.assertEqual(obj.map_time_unit('minutes'), 60)
        self.assertEqual(obj.map_time_unit('h'), 3600)
        self.assertEqual(obj.map_time_unit('hr'), 3600)
        self.assertEqual(obj.map_time_unit('hour'), 3600)
        self.assertEqual(obj.map_time_unit('hours'), 3600)
        self.assertEqual(obj.map_time_unit('d'), 24*3600)
        self.assertEqual(obj.map_time_unit('day'), 24*3600)
        self.assertEqual(obj.map_time_unit('days'), 24*3600)
        self.assertEqual(obj.map_time_unit('w'), 7*24*3600)
        self.assertEqual(obj.map_time_unit('week'), 7*24*3600)
        self.assertEqual(obj.map_time_unit('weeks'), 7*24*3600)
        self.assertEqual(obj.map_time_unit('month'), 30*24*3600)
        self.assertEqual(obj.map_time_unit('months'), 30*24*3600)
        self.assertEqual(obj.map_time_unit('y'), 365*24*3600)
        self.assertEqual(obj.map_time_unit('year'), 365*24*3600)
        self.assertEqual(obj.map_time_unit('years'), 365*24*3600)
        
    # --------------------------------------------------------------------------
    def clear_env(self):
        """
        Remove $CRAWL_CFG from the environment.
        """
        try:
            x = os.environ['CRAWL_CFG']
            del os.environ['CRAWL_CFG']
        except KeyError:
            pass
        
    # ------------------------------------------------------------------------
    def tearDown(self):
        """
        Clean up after every test.
        """
        if os.path.exists(self.env_cfname):
            os.unlink(self.env_cfname)
        os.chdir(launch_dir)
        
    # ------------------------------------------------------------------------
    def write_cfg_file(self, fname, cfgdict):
        """
        Write a config file for testing. Put the 'crawler' section first.
        Complain if the 'crawler' section is not present.
        """
        if (not isinstance(cfgdict, dict) and
            not isinstance(cfgdict, CrawlConfig)):
            
            raise StandardError("cfgdict has invalid type %s" % type(cfgdict))
        
        elif isinstance(cfgdict, dict):
            cfg = CrawlConfig()
            cfg.load_dict(cfgdict)

        elif isinstance(cfgdict, CrawlConfig):
            cfg = cfgdict
            
        if 'crawler' not in cfg.sections():
            raise StandardError("section 'crawler' missing from test config file")
        
        f = open(fname, 'w')
        cfg.write(f)
        f.close()

# -----------------------------------------------------------------------------
launch_dir = os.path.abspath(os.path.dirname(sys.argv[0]))
if __name__ == '__main__':
    toolframe.ez_launch(test='CrawlConfigTest',
                        logfile='crawl_test.log')
