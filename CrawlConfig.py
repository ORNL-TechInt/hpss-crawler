#!/usr/bin/env python
"""
CrawlConfig.py - Configuration class for crawl.py
"""
import ConfigParser
import os
import pdb
import re
import shutil
import stat
import StringIO
import testhelp
import time
import toolframe
import unittest

def main(argv):
    """
    Dummy main routine so we can use toolframe and testhelp
    """
    print("This is the package for CrawlConfig.")
    print("Usage:")
    print("    import CrawlConfig")
    print("    ...")
    print("    cfg = CrawlConfig.CrawlConfig()")
    print("    cfg.load_dict(dict)")
    print("    cfg.read(filename)")
    print("    cfg.get(<section>, <option>)")
    print("See the documentation for ConfigParser for more detail.")
    
class CrawlConfig(ConfigParser.ConfigParser):
    # --------------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        self.filename = '<???>'
        self.loadtime = 0.0
        ConfigParser.ConfigParser.__init__(self, *args, **kwargs)
        pass
    
    # --------------------------------------------------------------------------
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
    
    # --------------------------------------------------------------------------
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

    # --------------------------------------------------------------------------
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
    
    # --------------------------------------------------------------------------
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
    
    # --------------------------------------------------------------------------
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
        
    # --------------------------------------------------------------------------
    def read(self, filename):
        ConfigParser.ConfigParser.read(self, filename)
        self.filename = filename
        self.loadtime = time.time()
        
    # --------------------------------------------------------------------------
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

# ------------------------------------------------------------------------------
def CrawlConfig_setup():
    if not os.path.exists(CrawlConfigTest.testdir):
        os.mkdir(CrawlConfigTest.testdir)
    
# ------------------------------------------------------------------------------
def CrawlConfig_teardown():
    if os.path.exists(CrawlConfigTest.testdir):
        shutil.rmtree(CrawlConfigTest.testdir)
    
# ------------------------------------------------------------------------------
class CrawlConfigTest(unittest.TestCase):

    testdir = 'test.d'
    sample = {'crawler': {'opt1': 'foo',
                          'opt2': 'fribble',
                          'opt3': 'nice',
                          'heartbeat': '1hr',
                          'frequency': '5 min'},
              'sounds': {'duck': 'quack',
                         'dog':  'bark',
                         'hen':  'cluck'}}
    
    # --------------------------------------------------------------------------
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
        
    # --------------------------------------------------------------------------
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

    # --------------------------------------------------------------------------
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
    def test_get_time(self):
        """
        Routines exercised: __init__(), load_dict(), get_time().
        """
        obj = CrawlConfig()
        obj.load_dict(self.sample)
        self.assertEqual(obj.get_time('crawler', 'heartbeat'), 3600)
        self.assertEqual(obj.get_time('crawler', 'frequency'), 300)

    # --------------------------------------------------------------------------
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

    # --------------------------------------------------------------------------
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
        
# ------------------------------------------------------------------------------
if __name__ == '__main__':
    toolframe.ez_launch(setup=CrawlConfig_setup ,
                        cleanup=CrawlConfig_teardown,
                        test='CrawlConfigTest')
