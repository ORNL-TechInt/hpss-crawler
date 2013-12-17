#!/usr/bin/env python
"""
Test class for CrawlConfig.py
"""
import ConfigParser
import CrawlConfig
import copy
import os
import sys
import testhelp
import time
import toolframe
import util
import warnings

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

        obj = CrawlConfig.CrawlConfig()
        obj.load_dict(self.sample)
        f = open(cfgfile, 'w')
        obj.crawl_write(f)
        f.close()

        changeable = CrawlConfig.CrawlConfig()
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
        obj = CrawlConfig.CrawlConfig({'goose': 'honk'})
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
        obj = CrawlConfig.CrawlConfig()
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
        CrawlConfig.get_config(reset=True, soft=True)
        self.cd(self.testdir)
        self.clear_env()
        d = copy.deepcopy(self.cdict)
        d['crawler']['filename'] = self.default_cfname
        self.write_cfg_file(self.default_cfname, self.cdict)
        os.chmod(self.default_cfname, 0000)

        # test get_config with no argument
        try:
            cfg = CrawlConfig.get_config()
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
            cfg = CrawlConfig.get_config('')
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
        CrawlConfig.get_config(reset=True, soft=True)
        self.cd(self.testdir)
        self.clear_env()
        util.conditional_rm(self.default_cfname)

        # test with no argument
        try:
            cfg = CrawlConfig.get_config()
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
            cfg = CrawlConfig.get_config('')
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
        CrawlConfig.get_config(reset=True)
        self.cd(self.testdir)
        self.clear_env()
        d = copy.deepcopy(self.cdict)
        d['crawler']['filename'] = self.default_cfname
        self.write_cfg_file(self.default_cfname, d)
        os.chmod(self.default_cfname, 0644)

        got_exception = False
        try:
            cfg = CrawlConfig.get_config()
        except:
            got_exception = True
        self.assertEqual(got_exception, False)
        self.assertEqual(cfg.get('crawler', 'filename'), self.default_cfname)
        self.assertEqual(cfg.filename, self.default_cfname)
        
        got_exception = False
        try:
            cfg = CrawlConfig.get_config('')
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
        CrawlConfig.get_config(reset=True, soft=True)
        self.cd(self.testdir)
        os.environ['CRAWL_CONF'] = self.env_cfname
        d = copy.deepcopy(self.cdict)
        d['crawler']['filename'] = self.env_cfname
        self.write_cfg_file(self.env_cfname, d)
        os.chmod(self.env_cfname, 0000)

        try:
            cfg = CrawlConfig.get_config()
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
            cfg = CrawlConfig.get_config('')
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
        CrawlConfig.get_config(reset=True, soft=True)
        self.cd(self.testdir)
        os.environ['CRAWL_CONF'] = self.env_cfname
        util.conditional_rm(self.env_cfname)

        got_exception = False
        try:
            cfg = CrawlConfig.get_config()
        except StandardError as e:
            got_exception = True
            self.assertEqual(str(e),
                             '%s does not exist' %
                             self.env_cfname)
        self.assertEqual(got_exception, True)
        
        got_exception = False
        try:
            cfg = CrawlConfig.get_config('')
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
        CrawlConfig.get_config(reset=True, soft=True)
        self.cd(self.testdir)
        os.environ['CRAWL_CONF'] = self.env_cfname
        d = copy.deepcopy(self.cdict)
        d['crawler']['filename'] = self.env_cfname
        self.write_cfg_file(self.env_cfname, d)
        os.chmod(self.env_cfname, 0644)

        got_exception = False
        try:
            cfg = CrawlConfig.get_config()
        except:
            got_exception = True
        self.assertEqual(got_exception, False)
        self.assertEqual(cfg.get('crawler', 'filename'), self.env_cfname)
        
        cfg = CrawlConfig.get_config('')
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
        CrawlConfig.get_config(reset=True, soft=True)
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
            cfg = CrawlConfig.get_config(self.exp_cfname)
            self.fail("Expected exception was not thrown")
        except AssertionError:
            raise
        except StandardError as e:
            self.expected('%s is not readable' % self.exp_cfname, str(e))

    # --------------------------------------------------------------------------
    def test_get_config_exp_nosuch(self):
        """
        TEST: env CRAWL_CONF='envcrawl.cfg', envcrawl.cfg exists and
              is readable, explicit.cfg does not exist
              
        EXP: get_config('explicit.cfg') should throw a StandardError
             about the file not existing or not being readable
        """
        CrawlConfig.get_config(reset=True, soft=True)
        self.cd(self.testdir)
        os.environ['CRAWL_CONF'] = self.env_cfname
        d = copy.deepcopy(self.cdict)
        d['crawler']['filename'] = self.env_cfname
        self.write_cfg_file(self.env_cfname, d)
        os.chmod(self.env_cfname, 0644)

        util.conditional_rm(self.exp_cfname)

        try:
            cfg = CrawlConfig.get_config(self.exp_cfname)
            self.fail("Expected exception was not thrown")
        except AssertionError:
            raise
        except StandardError as e:
            self.expected('%s does not exist' % self.exp_cfname, str(e))

    # --------------------------------------------------------------------------
    def test_get_config_exp_ok(self):
        """
        TEST: env CRAWL_CONF='envcrawl.cfg', envcrawl.cfg exists and is
              readable, readable explicit.cfg does exist

        EXP: get_config('explicit.cfg') should load the explicit.cfg
        """
        CrawlConfig.get_config(reset=True, soft=True)
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

        cfg = CrawlConfig.get_config(self.exp_cfname)
        self.assertEqual(cfg.get('crawler', 'filename'), self.exp_cfname)

    # -------------------------------------------------------------------------
    def test_get_time(self):
        """
        Routines exercised: __init__(), load_dict(), get_time().
        """
        obj = CrawlConfig.CrawlConfig()
        obj.load_dict(self.sample)
        self.assertEqual(obj.get_time('crawler', 'heartbeat'), 3600)
        self.assertEqual(obj.get_time('crawler', 'frequency'), 300)

    # -------------------------------------------------------------------------
    def test_getboolean(self):
        """
        Routines exercised: getboolean().
        """
        obj = CrawlConfig.CrawlConfig()
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
        obj = CrawlConfig.CrawlConfig()

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
        obj = CrawlConfig.CrawlConfig()
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
    def test_read_include(self):
        """
        Read a config file which contains an include option
        """
        cfname_root = "%s/%s.cfg" % (self.testdir, "inclroot")
        cfname_inc1 = "%s/%s.cfg" % (self.testdir, "include1")
        cfname_inc2 = "%s/%s.cfg" % (self.testdir, "include2")

        root_d = copy.deepcopy(self.cdict)
        root_d['crawler']['include'] = cfname_inc1

        inc1_d = {'crawler': {'logmax': '17',
                              'coal': 'anthracite'
                              },
                  'newsect': {'newopt': 'newval',
                              'include': cfname_inc2}
            }

        inc2_d = {'fiddle': {'bar': 'wumpus'}}

        self.write_cfg_file(cfname_root, root_d)
        self.write_cfg_file(cfname_inc1, inc1_d)
        self.write_cfg_file(cfname_inc2, inc2_d, includee=True)

        obj = CrawlConfig.CrawlConfig()
        obj.read(cfname_root)

        root_d['crawler']['logmax'] = '17'
        root_d['crawler']['coal'] = 'anthracite'

        for D in [root_d, inc1_d, inc2_d]:
            for section in D:
                for option in D[section]:
                    self.expected(D[section][option], obj.get(section, option))

    # --------------------------------------------------------------------------
    def test_read_warn(self):
        """
        Read a config file which contains an include option. If some included
        files don't exist, we should get a warning that they were not loaded.
        """
        cfname_root = "%s/%s.cfg" % (self.testdir, "inclroot")
        cfname_inc1 = "%s/%s.cfg" % (self.testdir, "include1")
        cfname_inc2 = "%s/%s.cfg" % (self.testdir, "includez")

        root_d = copy.deepcopy(self.cdict)
        root_d['crawler']['include'] = cfname_inc1

        inc1_d = {'crawler': {'logmax': '17',
                              'coal': 'anthracite'
                              },
                  'newsect': {'newopt': 'newval',
                              'include': cfname_inc2}
            }

        # inc2_d = {'fiddle': {'bar': 'wumpus'}}

        self.write_cfg_file(cfname_root, root_d)
        self.write_cfg_file(cfname_inc1, inc1_d)

        obj = CrawlConfig.CrawlConfig()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            # this read should raise a warning about some config file(s) not
            # being loaded
            obj.read(cfname_root)
            # we only should have gotten one warning
            self.expected(1, len(w))
            # it should be a UserWarning
            self.assertTrue(issubclass(w[-1].category, UserWarning),
                            "Expected a UserWarning, but got a %s" %
                            w[-1].category)
            # it should contain this string
            self.assertTrue("Some config files not loaded" in str(w[-1]),
                            "Unexpected message: '%s'" % str(w[-1]))

        root_d['crawler']['logmax'] = '17'
        root_d['crawler']['coal'] = 'anthracite'

        for D in [root_d, inc1_d]:
            for section in D:
                for option in D[section]:
                    self.expected(D[section][option], obj.get(section, option))

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
        util.conditional_rm(self.env_cfname)
        os.chdir(launch_dir)
        
# -----------------------------------------------------------------------------
launch_dir = os.path.abspath(os.path.dirname(sys.argv[0]))
if __name__ == '__main__':
    toolframe.ez_launch(test='CrawlConfigTest',
                        logfile='crawl_test.log')
