#!/usr/bin/env python
"""
Test class for CrawlConfig.py
"""
import CrawlConfig
import copy
import logging
import os
import pdb
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
    CrawlConfig.get_logger(reset=True, soft=True)
    testhelp.module_test_teardown(CrawlConfigTest.testdir)
    
# -----------------------------------------------------------------------------
class CrawlConfigTest(testhelp.HelpedTestCase):
    """
    Test class for CrawlConfig
    """
    default_cfname = 'crawl.cfg'
    env_cfname = 'envcrawl.cfg'
    exp_cfname = 'explicit.cfg'
    testdir = testhelp.testdata(__name__)
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
        with util.Chdir(self.testdir):
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
        with util.Chdir(self.testdir):
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
        
    # --------------------------------------------------------------------------
    def test_get_config_def_ok(self):
        """
        TEST: env not set, 'crawl.cfg' does exist =>

        EXP: get_config() or get_config('') should load the config
        """
        CrawlConfig.get_config(reset=True, soft=True)
        with util.Chdir(self.testdir):
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
        with util.Chdir(self.testdir):
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
        with util.Chdir(self.testdir):
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
        with util.Chdir(self.testdir):
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
        with util.Chdir(self.testdir):
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
        with util.Chdir(self.testdir):
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
        with util.Chdir(self.testdir):
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
    def test_get_d_with(self):
        """
        Calling get_d() with a default value should 

          1) return the option value if it's defined
          2) return the default value otherwise

        If a default value is provided, get_d should not throw NoOptionError or
        NoSectionError
        """
        obj = CrawlConfig.CrawlConfig()
        obj.load_dict(self.sample)
        # section and option are in the config object
        self.expected('quack', obj.get_d('sounds', 'duck', 'foobar'))
        # section is defined, option is not, should get the default
        self.expected('whistle', obj.get_d('sounds', 'dolphin', 'whistle'))
        # section not defined, should get the default
        self.expected('buck', obj.get_d('malename', 'deer', 'buck'))
        
    # -------------------------------------------------------------------------
    def test_get_d_without(self):
        """
        Calling get_d() without a default value should

          1) return the option value if it's defined
          2) otherwise throw a NoSectionError or NoOptionError
        """
        obj = CrawlConfig.CrawlConfig()
        obj.load_dict(self.sample)
        # section and option are in the config object
        self.expected('quack', obj.get_d('sounds', 'duck'))

        # section is defined, option is not, should get exception
        try:
            self.expected('whistle', obj.get_d('sounds', 'dolphin'))
            self.fail("Expected exception not thrown")
        except CrawlConfig.NoOptionError:
            pass
        
        # section not defined, should get the default
        try:
            self.expected('buck', obj.get_d('malename', 'deer'))
            self.fail("Expected exception not thrown")
        except CrawlConfig.NoSectionError:
            pass
        
    # -------------------------------------------------------------------------
    def test_get_logger_00(self):
        """
        With no logger cached, reset=False and soft=False should create a
        new logger. If a logger has been created, this case should return the
        cached logger.
        """
        # throw away any logger that has been set
        CrawlConfig.get_logger(reset=True, soft=True)

        # get_logger(..., reset=False, soft=False) should create a new one
        actual = CrawlConfig.get_logger(cmdline='%s/util.log' % self.testdir,
                                 reset=False, soft=False)
        self.assertTrue(isinstance(actual, logging.Logger),
                        "Expected logging.Logger, got %s" % (actual))
        self.expected(os.path.abspath("%s/util.log" % self.testdir),
                      actual.handlers[0].baseFilename)
    
        # now ask for a logger with a different name, with reset=False,
        # soft=False. Since one has already been created, the new name should
        # be ignored and we should get back the one already cached.
        CrawlConfig.get_logger(cmdline='%s/util_foobar.log' % self.testdir,
                        reset=False, soft=False)
        self.assertTrue(isinstance(actual, logging.Logger),
                        "Expected logging.Logger, got %s" % (actual))
        self.expected(os.path.abspath("%s/util.log" % self.testdir),
                      actual.handlers[0].baseFilename)

        
    # -------------------------------------------------------------------------
    def test_get_logger_01(self):
        """
        With no logger cached, reset=False and soft=True should not create a
        new logger. If a logger has been created, this case should return the
        cached logger.
        """
        # throw away any logger that has been set
        CrawlConfig.get_logger(reset=True, soft=True)
        
        # then see what happens with reset=False, soft=True
        actual = CrawlConfig.get_logger(cmdline='%s/util.log' % self.testdir,
                                 reset=False, soft=True)
        self.expected(None, actual)

        # now create a logger
        CrawlConfig.get_logger(cmdline='%s/util.log' % self.testdir)
        # now reset=False, soft=True should return the one just created
        actual = CrawlConfig.get_logger(reset=False, soft=True)
        self.assertTrue(isinstance(actual, logging.Logger),
                      "Expected logging.Logger, got %s" % (actual))
        self.expected(os.path.abspath("%s/util.log" % self.testdir),
                      actual.handlers[0].baseFilename)


    # -------------------------------------------------------------------------
    def test_get_logger_10(self):
        """
        Calling get_logger with reset=True, soft=False should get rid of the
        previously cached logger and make a new one.
        """
        # throw away any logger that has been set and create one to be
        # overridden
        tmp = CrawlConfig.get_logger(cmdline='%s/throwaway.log' % self.testdir,
                              reset=True)
                              
        # verify that it's there with the expected attributes
        self.assertTrue(isinstance(tmp, logging.Logger),
                        "Expected logging.Logger, got %s" % (tmp))
        self.expected(1, len(tmp.handlers))
        self.expected(os.path.abspath("%s/throwaway.log" % self.testdir),
                      tmp.handlers[0].baseFilename)

        # now override it
        actual = CrawlConfig.get_logger(cmdline='%s/util.log' % self.testdir,
                                 reset=True, soft=False)
        # and verify that it got replaced
        self.assertTrue(isinstance(actual, logging.Logger),
                        "Expected logging.Logger, got %s" % (actual))
        self.expected(1, len(actual.handlers))
        self.expected(os.path.abspath("%s/util.log" % self.testdir),
                      actual.handlers[0].baseFilename)
        
    # -------------------------------------------------------------------------
    def test_get_logger_11(self):
        """
        Calling get_logger with both reset=True and soft=True should throw away
        any cached logger and return None without creating a new one.
        """
        exp = None
        actual = CrawlConfig.get_logger(reset=True, soft=True)
        self.expected(exp, actual)
        
    # -------------------------------------------------------------------------
    def test_get_logger_cfg(self):
        """
        Call get_logger with a config that specifies non default values for log
        file name, log file size, and max log files on disk. Verify that the
        resulting logger has the correct parameters.
        """
        cfname = "%s/%s.cfg" % (self.testdir, util.my_name())
        lfname = "%s/%s.log" % (self.testdir, util.my_name())
        cdict = {'crawler': {'logpath': lfname,
                             'logsize': '17mb',
                             'logmax': '13'
                             }
                 }
        c = CrawlConfig.CrawlConfig()
        c.load_dict(cdict)

        # reset any logger that has been initialized
        CrawlConfig.get_logger(reset=True, soft=True)

        # now ask for one that matches the configuration
        l = CrawlConfig.get_logger(cfg=c)

        # and check that it has the right handler
        self.assertNotEqual(l, None)
        self.expected(1, len(l.handlers))
        self.expected(os.path.abspath(lfname), l.handlers[0].stream.name)
        self.expected(17*1000*1000, l.handlers[0].maxBytes)
        self.expected(13, l.handlers[0].backupCount)

        self.assertTrue(os.path.exists(lfname),
                        "%s should exist but does not" % lfname)
        
    # --------------------------------------------------------------------------
    def test_get_logger_default(self):
        """
        TEST: Call get_logger() with no argument

        EXP: Attempts to log to '/var/log/crawl.log', falls back to
        '/tmp/crawl.log' if we can't access the protected file
        """
        with util.Chdir(self.testdir):
            util.conditional_rm('crawl.cfg')
            CrawlConfig.get_config(reset=True, soft=True)
            CrawlConfig.get_logger(reset=True, soft=True)
            lobj = CrawlConfig.get_logger()

            # if I'm root, I should be looking at /var/log/crawl.log
            if os.getuid() == 0:
                self.expected('/var/log/crawl.log',
                              lobj.handlers[0].stream.name)
            
            # otherwise, I should be looking at /tmp/crawl.log
            else:
                self.expected('/tmp/crawl.log',
                              lobj.handlers[0].stream.name)
        
    # -------------------------------------------------------------------------
    def test_get_logger_def_cfg(self):
        """
        Call get_logger with no cmdline or cfg arguments but with a default
        config file available. The result should be a logger open on the log
        path named in the default config file (retrieved by
        CrawlConfig.get_config()).
        """
        with util.Chdir(self.testdir):
            # reset any logger that has been initialized
            CrawlConfig.get_logger(reset=True, soft=True)
            CrawlConfig.get_config(reset=True, soft=True)

            logpath = os.path.basename(self.default_logpath)
            self.clear_env()
            d = copy.deepcopy(self.cdict)
            d['crawler']['filename'] = self.default_cfname
            d['crawler']['logpath'] = logpath
            self.write_cfg_file(self.default_cfname, d)
            os.chmod(self.default_cfname, 0644)

            # now ask for a default logger
            l = CrawlConfig.get_logger()

            # and check that it has the right handler
            self.expected(1, len(l.handlers))
            self.expected(os.path.abspath(logpath),
                          l.handlers[0].stream.name)
            self.expected(5000000, l.handlers[0].maxBytes)
            self.expected(5, l.handlers[0].backupCount)

    # -------------------------------------------------------------------------
    def test_get_logger_nocfg(self):
        """
        Call get_logger with no cmdline or cfg arguments and make sure the
        resulting logger has the correct parameters.
        """
        with util.Chdir(self.testdir):
            # reset any logger and config that has been initialized
            CrawlConfig.get_config(reset=True, soft=True)
            CrawlConfig.get_logger(reset=True, soft=True)

            # now ask for a default logger
            l = CrawlConfig.get_logger()

            # and check that it has the right handler
            self.expected(1, len(l.handlers))
            if os.getuid() == 0:
                self.expected("/var/log/crawl.log", l.handlers[0].stream.name)
            else:
                self.expected("/tmp/crawl.log", l.handlers[0].stream.name)
            self.expected(10*1024*1024, l.handlers[0].maxBytes)
            self.expected(5, l.handlers[0].backupCount)

    # --------------------------------------------------------------------------
    def test_get_logger_path(self):
        """
        TEST: Call get_logger() with a pathname

        EXP: Attempts to log to pathname
        """
        CrawlConfig.get_logger(reset=True, soft=True)
        logpath = '%s/%s.log' % (self.testdir, util.my_name())
        util.conditional_rm(logpath)
        self.assertEqual(os.path.exists(logpath), False,
                         '%s should not exist but does' % logpath)
        lobj = CrawlConfig.get_logger(logpath)
        self.assertEqual(os.path.exists(logpath), True,
                         '%s should exist but does not' % logpath)
        
    # -------------------------------------------------------------------------
    def test_get_size(self):
        """
        Routine get_size() translates expressions like '30 mib' to 30 * 1024 *
        1024 or '10mb' to 10,000,000
        """
        section = util.my_name()
        obj = CrawlConfig.CrawlConfig()
        obj.add_section(section)
        obj.set(section, 'tenmb', '10mb')
        obj.set(section, 'thirtymib', '30mib')
        self.expected(10*1000*1000, obj.get_size(section, 'tenmb'))
        self.expected(30*1024*1024, obj.get_size(section, 'thirtymib'))
                      
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
            x = os.environ['CRAWL_CONF']
            del os.environ['CRAWL_CONF']
        except KeyError:
            pass
        
    # ------------------------------------------------------------------------
    def tearDown(self):
        """
        Clean up after every test.
        """
        util.conditional_rm(self.env_cfname)
        
# -----------------------------------------------------------------------------
launch_dir = os.path.abspath(os.path.dirname(sys.argv[0]))
if __name__ == '__main__':
    toolframe.ez_launch(test='CrawlConfigTest',
                        logfile=testhelp.testlog(__name__))
