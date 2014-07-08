#!/usr/bin/env python
"""
Test class for CrawlConfig.py
"""
from hpssic import CrawlConfig
import copy
import logging
import os
import pdb
import sys
from hpssic import testhelp
import time
from hpssic import toolframe
from hpssic import util
import warnings


# -----------------------------------------------------------------------------
def logErr(record):
    raise


# -----------------------------------------------------------------------------
def setUpModule():
    """
    Set up for the tests.
    """
    testhelp.module_test_setup(CrawlConfigTest.testdir)
    CrawlConfig.get_logger(CrawlConfigTest.default_logpath, reset=True)


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
        # time.sleep(1.0)
        os.utime(cfgfile, (time.time() + 5, time.time() + 5))
        # f = open(cfgfile, 'a')
        # f.write('\n')
        # f.close()
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
        defaults = {'goose': 'honk'}
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

            expmsg = "%s is not readable" % self.default_cfname
            # test get_config with no argument
            self.assertRaisesMsg(StandardError, expmsg, CrawlConfig.get_config)

            # test get_config with empty string argument
            self.assertRaisesMsg(StandardError, expmsg,
                                 CrawlConfig.get_config, '')

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

            expmsg = "%s does not exist" % self.default_cfname
            # test with no argument
            self.assertRaisesMsg(StandardError, expmsg, CrawlConfig.get_config)

            # test with empty string argument
            self.assertRaisesMsg(StandardError, expmsg,
                                 CrawlConfig.get_config, '')

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
            self.assertEqual(cfg.get('crawler', 'filename'),
                             self.default_cfname)
            self.assertEqual(cfg.filename, self.default_cfname)

            got_exception = False
            try:
                cfg = CrawlConfig.get_config('')
            except:
                got_exception = True
            self.assertEqual(got_exception, False)
            self.assertEqual(cfg.get('crawler', 'filename'),
                             self.default_cfname)
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

            expmsg = "%s is not readable" % self.env_cfname
            self.assertRaisesMsg(StandardError, expmsg,
                                 CrawlConfig.get_config)

            self.assertRaisesMsg(StandardError, expmsg,
                                 CrawlConfig.get_config, '')

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

            self.assertRaisesMsg(StandardError,
                                 "%s is not readable" % self.exp_cfname,
                                 CrawlConfig.get_config,
                                 self.exp_cfname)

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

            self.assertRaisesMsg(StandardError,
                                 "%s does not exist" % self.exp_cfname,
                                 CrawlConfig.get_config,
                                 self.exp_cfname)

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
        actual = CrawlConfig.get_logger(cmdline='%s/CrawlConfig.log' %
                                        self.testdir,
                                        reset=False, soft=False)
        self.assertTrue(isinstance(actual, logging.Logger),
                        "Expected logging.Logger, got %s" % (actual))
        self.expected(os.path.abspath("%s/CrawlConfig.log" % self.testdir),
                      actual.handlers[0].baseFilename)

        # now ask for a logger with a different name, with reset=False,
        # soft=False. Since one has already been created, the new name should
        # be ignored and we should get back the one already cached.
        CrawlConfig.get_logger(cmdline='%s/util_foobar.log' % self.testdir,
                               reset=False, soft=False)
        self.assertTrue(isinstance(actual, logging.Logger),
                        "Expected logging.Logger, got %s" % (actual))
        self.expected(os.path.abspath("%s/CrawlConfig.log" % self.testdir),
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
        actual = CrawlConfig.get_logger(cmdline='%s/CrawlConfig.log' %
                                        self.testdir, reset=False, soft=True)
        self.expected(None, actual)

        # now create a logger
        CrawlConfig.get_logger(cmdline='%s/CrawlConfig.log' % self.testdir)
        # now reset=False, soft=True should return the one just created
        actual = CrawlConfig.get_logger(reset=False, soft=True)
        self.assertTrue(isinstance(actual, logging.Logger),
                        "Expected logging.Logger, got %s" % (actual))
        self.expected(os.path.abspath("%s/CrawlConfig.log" % self.testdir),
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
        actual = CrawlConfig.get_logger(cmdline='%s/CrawlConfig.log' %
                                        self.testdir,
                                        reset=True, soft=False)
        # and verify that it got replaced
        self.assertTrue(isinstance(actual, logging.Logger),
                        "Expected logging.Logger, got %s" % (actual))
        self.expected(1, len(actual.handlers))
        self.expected(os.path.abspath("%s/CrawlConfig.log" % self.testdir),
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
    def test_interpolation_ok(self):
        d = copy.deepcopy(self.cdict)
        d['crawler']['logpath'] = "%(root)s/fiddle.log"
        obj = CrawlConfig.CrawlConfig()
        obj.load_dict(d, {'root': '/the/root/directory'})
        exp = "/the/root/directory/fiddle.log"
        actual = obj.get('crawler', 'logpath')
        self.assertEqual(exp, actual, "Expected '%s', got '%s'")

    # -------------------------------------------------------------------------
    def test_interpolation_fail(self):
        d = copy.deepcopy(self.cdict)
        d['crawler']['logpath'] = "%(root)s/fiddle.log"
        obj = CrawlConfig.CrawlConfig()
        obj.load_dict(d, {'xroot': '/there/is/no/root'})
        exp = "/the/root/directory/fiddle.log"
        self.assertRaisesMsg(CrawlConfig.InterpolationMissingOptionError,
                             "Bad value substitution",
                             obj.get, 'crawler', 'logpath')

    # -------------------------------------------------------------------------
    def test_interpolation_ok(self):
        d = copy.deepcopy(self.cdict)
        d['crawler']['logpath'] = "%(root)s/fiddle.log"
        obj = CrawlConfig.CrawlConfig()
        obj.load_dict(d, {'root': '/the/root/directory'})
        exp = "/the/root/directory/fiddle.log"
        actual = obj.get('crawler', 'logpath')
        self.assertEqual(exp, actual, "Expected '%s', got '%s'")

    # -------------------------------------------------------------------------
    def test_interpolation_fail(self):
        d = copy.deepcopy(self.cdict)
        d['crawler']['logpath'] = "%(root)s/fiddle.log"
        obj = CrawlConfig.CrawlConfig()
        obj.load_dict(d, {'xroot': '/there/is/no/root'})
        exp = "/the/root/directory/fiddle.log"
        self.assertRaisesMsg(CrawlConfig.InterpolationMissingOptionError,
                             "Bad value substitution",
                             obj.get, 'crawler', 'logpath')

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
    def test_log_default(self):
        """
        If CrawlConfig.log() is called with no logger already instantiated, and
        no default config file available, it should resort to the default log
        file names in either /var/log/crawl.log or /tmp/crawl.log, depending on
        the privilege of the running user.
        """
        with util.Chdir(self.testdir):
            # reset any config and logger already initialized
            CrawlConfig.get_config(reset=True, soft=True)
            CrawlConfig.get_logger(reset=True, soft=True)

            # now attempt to log a message to the default file
            msg = "This is a test log message %s"
            arg = "with a format specifier"
            if 0 == os.getuid():
                exp_logfile = "/var/log/crawl.log"
            else:
                exp_logfile = "/tmp/crawl.log"
            exp = (util.my_name() +
                   "(%s:%d): " % (sys._getframe().f_code.co_filename,
                                  sys._getframe().f_lineno + 2) +
                   msg % arg)
            CrawlConfig.log(msg, arg)
            result = util.contents(exp_logfile)
            self.assertTrue(exp in result,
                            "Expected '%s' in %s" %
                            (exp, util.line_quote(result)))

    # -------------------------------------------------------------------------
    def test_log_rollover_archive(self):
        """
        Create a logger with a low rollover threshold and an archive dir. Write
        to it until it rolls over. Verify that the archive file was handled
        correctly.
        """
        logbase = '%s.log' % util.my_name()
        logpath = "%s/%s" % (self.testdir, logbase)
        logpath_1 = logpath + ".1"
        archdir = "%s/history" % (self.testdir)
        ym = time.strftime("%Y.%m%d")
        archlogpath = '%s/%s.%s-%s' % (archdir, logbase, ym, ym)
        lcfg_d = {'crawler': {'logpath': logpath,
                              'logsize': '500',
                              'archive_dir': archdir,
                              'logmax': '10'}}
        lcfg = CrawlConfig.CrawlConfig()
        lcfg.load_dict(lcfg_d)
        CrawlConfig.get_logger(cfg=lcfg, reset=True)
        lmsg = "This is a test " + "-" * 35
        for x in range(0, 5):
            CrawlConfig.log(lmsg)

        self.assertTrue(os.path.isdir(archdir),
                        "Expected directory %s to be created" % archdir)
        self.assertTrue(os.path.isfile(logpath),
                        "Expected file %s to exist" % logpath)
        self.assertTrue(os.path.isfile(archlogpath),
                        "Expected file %s to exist" % archlogpath)

    # -------------------------------------------------------------------------
    def test_log_rollover_cwd(self):
        """
        Create a logger with a low rollover threshold and no archive dir. Write
        to it until it rolls over. Verify that the archive file was handled
        correctly.
        """
        logbase = '%s.log' % util.my_name()
        logpath = "%s/%s" % (self.testdir, logbase)
        logpath_1 = logpath + ".1"
        ym = time.strftime("%Y.%m%d")
        archlogpath = '%s/%s.%s-%s' % (self.testdir, logbase, ym, ym)
        lcfg_d = {'crawler': {'logpath': logpath,
                              'logsize': '500',
                              'logmax': '10'}}
        lcfg = CrawlConfig.CrawlConfig()
        lcfg.load_dict(lcfg_d)
        CrawlConfig.get_logger(cfg=lcfg, reset=True)
        lmsg = "This is a test " + "-" * 35
        for x in range(0, 5):
            CrawlConfig.log(lmsg)

        self.assertTrue(os.path.isfile(logpath),
                        "Expected file %s to exist" % logpath)
        self.assertFalse(os.path.isfile(archlogpath),
                         "Expected file %s to not exist" % archlogpath)

    # -------------------------------------------------------------------------
    def test_log_multfmt(self):
        # """
        # Tests for routine CrawlConfig.log():
        #  - simple string in first argument
        #  - 1 % formatter in first arg
        #  - multiple % formatters in first arg
        #  - too many % formatters for args
        #  - too many args for % formatters
        # """
        fpath = "%s/%s.log" % (self.testdir, util.my_name())
        CrawlConfig.get_logger(reset=True, soft=True)
        log = CrawlConfig.get_logger(cmdline=fpath)

        # multiple % formatters in first arg
        a1 = "Here's a string: '%s'; here's an int: %d; here's a float: %f"
        a2 = "zebedee"
        a3 = 94
        a4 = 23.12348293402
        exp = (util.my_name() +
               "(%s:%d): " % (util.filename(), util.lineno()+2) +
               a1 % (a2, a3, a4))
        CrawlConfig.log(a1, a2, a3, a4)
        result = util.contents(fpath)
        self.assertTrue(exp in result,
                        "Expected '%s' in %s" %
                        (exp, util.line_quote(result)))

    # -------------------------------------------------------------------------
    def test_log_onefmt(self):
        # """
        # Tests for routine CrawlConfig.log():
        #  - simple string in first argument
        #  - 1 % formatter in first arg
        #  - multiple % formatters in first arg
        #  - too many % formatters for args
        #  - too many args for % formatters
        # """
        fpath = "%s/%s.log" % (self.testdir, util.my_name())
        CrawlConfig.get_logger(reset=True, soft=True)
        log = CrawlConfig.get_logger(cmdline=fpath)

        # 1 % formatter in first arg
        a1 = "This has a formatter and one argument: %s"
        a2 = "did that work?"
        exp = (util.my_name() +
               "(%s:%d): " % (util.filename(), util.lineno()+2) +
               a1 % a2)
        CrawlConfig.log(a1, a2)
        result = util.contents(fpath)
        self.assertTrue(exp in result,
                        "Expected '%s' in %s" %
                        (exp, util.line_quote(result)))

    # -------------------------------------------------------------------------
    def test_log_simple(self):
        """
        Tests for routine CrawlConfig.log():
         - simple string in first argument
         - 1 % formatter in first arg
         - multiple % formatters in first arg
         - too many % formatters for args
         - too many args for % formatters
        """
        fpath = "%s/%s.log" % (self.testdir, util.my_name())
        CrawlConfig.get_logger(reset=True, soft=True)
        log = CrawlConfig.get_logger(cmdline=fpath)

        # simple string in first arg
        exp = util.my_name() + ": " + "This is a simple string"
        CrawlConfig.log(exp)
        result = util.contents(fpath)
        self.assertTrue(exp in result,
                        "Expected '%s' in %s" %
                        (exp, util.line_quote(result)))

    # -------------------------------------------------------------------------
    def test_log_toomany_fmt(self):
        # """
        # Tests for routine CrawlConfig.log():
        #  - simple string in first argument
        #  - 1 % formatter in first arg
        #  - multiple % formatters in first arg
        #  - too many % formatters for args
        #  - too many args for % formatters
        # """
        fpath = "%s/%s.log" % (self.testdir, util.my_name())
        CrawlConfig.get_logger(reset=True, soft=True)
        log = CrawlConfig.get_logger(cmdline=fpath)

        # this allows exceptions thrown from inside the logging handler to
        # propagate up so we can catch it.
        log.handlers[0].handleError = logErr

        # multiple % formatters in first arg
        a1 = "Here's a string: '%s'; here's an int: %d; here's a float: %f; %g"
        a2 = "zebedee"
        a3 = 94
        a4 = 23.12348293402
        exp = util.my_name() + ": " + a1 % (a2, a3, a4, 17.9)
        try:
            CrawlConfig.log(a1, a2, a3, a4)
            self.fail("Expected exception not thrown")
        except TypeError, e:
            self.assertEqual("not enough arguments for format string", str(e),
                             "Wrong TypeError thrown")

        result = util.contents(fpath)
        self.assertFalse(exp in result,
                         "Expected '%s' in %s" %
                         (exp, util.line_quote(result)))

    # -------------------------------------------------------------------------
    def test_log_toomany_args(self):
        # """
        # Tests for routine CrawlConfig.log():
        #  - simple string in first argument
        #  - 1 % formatter in first arg
        #  - multiple % formatters in first arg
        #  - too many % formatters for args
        #  - too many args for % formatters
        # """
        fpath = "%s/%s.log" % (self.testdir, util.my_name())
        CrawlConfig.get_logger(reset=True, soft=True)
        log = CrawlConfig.get_logger(cmdline=fpath)

        # this allows exceptions thrown from inside the logging handler to
        # propagate up so we can catch it.
        log.handlers[0].handleError = logErr

        # multiple % formatters in first arg
        a1 = "Here's a string: '%s'; here's an int: %d; here's a float: %f"
        a2 = "zebedee"
        a3 = 94
        a4 = 23.12348293402
        a5 = "friddle"
        exp = (util.my_name() + ": " + a1 % (a2, a3, a4))
        try:
            CrawlConfig.log(a1, a2, a3, a4, a5)
            self.fail("Expected exception not thrown")
        except TypeError, e:
            exc = "not all arguments converted during string formatting"
            self.assertEqual(exc, str(e),
                             "Expected '%s', got '%s'" % (exc, str(e)))

        result = util.contents(fpath)
        self.assertFalse(exp in result,
                         "Expected '%s' in %s" %
                         (exp, util.line_quote(result)))

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
    def test_quiet_time_bound_mt(self):
        """
        Test a quiet time spec. Empty boundary -- hi == lo
        """
        ldict = copy.deepcopy(self.cdict)
        ldict['crawler']['quiet_time'] = "19:17-19:17"
        cfg = CrawlConfig.CrawlConfig()
        cfg.load_dict(ldict)

        # front of day
        self.try_qt_spec(cfg, False, "2014.0331 23:59:59")
        self.try_qt_spec(cfg, False, "2014.0401 00:00:00")
        self.try_qt_spec(cfg, False, "2014.0401 00:00:01")

        # interval trailing edge
        self.try_qt_spec(cfg, False, "2014.0101 19:16:59")
        self.try_qt_spec(cfg, True, "2014.0101 19:17:00")
        self.try_qt_spec(cfg, False, "2014.0101 19:17:01")

        # end of day
        self.try_qt_spec(cfg, False, "2014.0401 23:59:59")
        self.try_qt_spec(cfg, False, "2014.0402 00:00:00")
        self.try_qt_spec(cfg, False, "2014.0402 00:00:01")

    # --------------------------------------------------------------------------
    def test_quiet_time_bound_rsu(self):
        """
        Test a quiet time spec. Right side up (rsu) boundary -- lo < hi
        """
        ldict = copy.deepcopy(self.cdict)
        ldict['crawler']['quiet_time'] = "14:00-19:00"
        cfg = CrawlConfig.CrawlConfig()
        cfg.load_dict(ldict)

        # non-interval time
        self.try_qt_spec(cfg, False, "2014.0101 11:19:58")

        # interval leading edge
        self.try_qt_spec(cfg, False, "2014.0101 13:59:59")
        self.try_qt_spec(cfg, True, "2014.0101 14:00:00")
        self.try_qt_spec(cfg, True, "2014.0101 14:00:01")

        # interval time
        self.try_qt_spec(cfg, True, "2014.0101 15:28:19")

        # interval trailing edge
        self.try_qt_spec(cfg, True, "2014.0101 18:59:59")
        self.try_qt_spec(cfg, True, "2014.0101 19:00:00")
        self.try_qt_spec(cfg, False, "2014.0101 19:00:01")

    # --------------------------------------------------------------------------
    def test_quiet_time_bound_usd(self):
        """
        Test a quiet time spec. Upside-down (usd) boundary -- hi < lo
        """
        ldict = copy.deepcopy(self.cdict)
        ldict['crawler']['quiet_time'] = "19:00-03:00"
        cfg = CrawlConfig.CrawlConfig()
        cfg.load_dict(ldict)

        # front of day
        self.try_qt_spec(cfg, True, "2014.0331 23:59:59")
        self.try_qt_spec(cfg, True, "2014.0401 00:00:00")
        self.try_qt_spec(cfg, True, "2014.0401 00:00:01")

        # mid quiet interval
        self.try_qt_spec(cfg, True, "2014.0101 02:19:32")

        # interval trailing edge
        self.try_qt_spec(cfg, True, "2014.0101 02:59:59")
        self.try_qt_spec(cfg, True, "2014.0101 03:00:00")
        self.try_qt_spec(cfg, False, "2014.0101 03:00:01")

        # mid non-interval
        self.try_qt_spec(cfg, False, "2014.0101 12:17:09")

        # interval leading edge
        self.try_qt_spec(cfg, False, "2014.0101 18:59:59")
        self.try_qt_spec(cfg, True, "2014.0101 19:00:00")
        self.try_qt_spec(cfg, True, "2014.0101 19:00:01")

        # mid quiet interval
        self.try_qt_spec(cfg, True, "2014.0101 21:32:19")

        # end of day
        self.try_qt_spec(cfg, True, "2014.0401 23:59:59")
        self.try_qt_spec(cfg, True, "2014.0402 00:00:00")
        self.try_qt_spec(cfg, True, "2014.0402 00:00:01")

    # --------------------------------------------------------------------------
    def test_quiet_time_d_w(self):
        """
        Test a multiple quiet time spec. Date plus weekday.
        """
        ldict = copy.deepcopy(self.cdict)
        ldict['crawler']['quiet_time'] = "2013.0428,fri"
        cfg = CrawlConfig.CrawlConfig()
        cfg.load_dict(ldict)

        # leading edge of date
        self.try_qt_spec(cfg, False, "2013.0427 23:59:59")
        self.try_qt_spec(cfg, True, "2013.0428 00:00:00")
        self.try_qt_spec(cfg, True, "2013.0428 00:00:01")

        # inside date
        self.try_qt_spec(cfg, True, "2013.0428 08:00:01")

        # trailing edge of date
        self.try_qt_spec(cfg, True, "2013.0428 23:59:59")
        self.try_qt_spec(cfg, False, "2013.0429 00:00:00")
        self.try_qt_spec(cfg, False, "2013.0429 00:00:01")

        # outside date, outside weekday
        self.try_qt_spec(cfg, False, "2013.0501 04:17:49")

        # leading edge of weekday
        self.try_qt_spec(cfg, False, "2013.0502 23:59:59")
        self.try_qt_spec(cfg, True, "2013.0503 00:00:00")
        self.try_qt_spec(cfg, True, "2013.0503 00:00:01")

        # inside weekday
        self.try_qt_spec(cfg, True, "2013.0503 11:23:01")

        # trailing edge of weekday
        self.try_qt_spec(cfg, True, "2013.0503 23:59:59")
        self.try_qt_spec(cfg, False, "2013.0504 00:00:00")
        self.try_qt_spec(cfg, False, "2013.0504 00:00:01")

    # --------------------------------------------------------------------------
    def test_quiet_time_date(self):
        """
        Test a date quiet time spec. The edges are inclusive.
        """
        ldict = copy.deepcopy(self.cdict)
        ldict['crawler']['quiet_time'] = "2014.0401"
        cfg = CrawlConfig.CrawlConfig()
        cfg.load_dict(ldict)

        # before date
        self.try_qt_spec(cfg, False, "2014.0331 23:00:00")

        # leading edge of date
        self.try_qt_spec(cfg, False, "2014.0331 23:59:59")
        self.try_qt_spec(cfg, True, "2014.0401 00:00:00")
        self.try_qt_spec(cfg, True, "2014.0401 00:00:01")

        # inside date
        self.try_qt_spec(cfg, True, "2014.0401 13:59:59")
        self.try_qt_spec(cfg, True, "2014.0401 14:00:00")
        self.try_qt_spec(cfg, True, "2014.0401 14:00:01")

        # trailing edge of date
        self.try_qt_spec(cfg, True, "2014.0401 23:59:59")
        self.try_qt_spec(cfg, False, "2014.0402 00:00:00")
        self.try_qt_spec(cfg, False, "2014.0402 00:00:01")

    # --------------------------------------------------------------------------
    def test_quiet_time_missing(self):
        """
        When the config item is missing, quiet_time() should always return
        False
        """
        cfg = CrawlConfig.CrawlConfig()
        cfg.load_dict(self.cdict)

        # before date
        self.try_qt_spec(cfg, False, "2014.0331 23:00:00")

        # leading edge of date
        self.try_qt_spec(cfg, False, "2014.0331 23:59:59")
        self.try_qt_spec(cfg, False, "2014.0401 00:00:00")
        self.try_qt_spec(cfg, False, "2014.0401 00:00:01")

        # inside date
        self.try_qt_spec(cfg, False, "2014.0401 13:59:59")
        self.try_qt_spec(cfg, False, "2014.0401 14:00:00")
        self.try_qt_spec(cfg, False, "2014.0401 14:00:01")

        # trailing edge of date
        self.try_qt_spec(cfg, False, "2014.0401 23:59:59")
        self.try_qt_spec(cfg, False, "2014.0402 00:00:00")
        self.try_qt_spec(cfg, False, "2014.0402 00:00:01")

    # --------------------------------------------------------------------------
    def test_quiet_time_rb_d(self):
        """
        Test a multiple quiet time spec. RSU boundary plus date.
        """
        ldict = copy.deepcopy(self.cdict)
        ldict['crawler']['quiet_time'] = "2014.0401, 17:00 - 23:00"
        cfg = CrawlConfig.CrawlConfig()
        cfg.load_dict(ldict)

        # day before, before interval
        self.try_qt_spec(cfg, False, "2014.0331 03:07:18")

        # day before, leading edge of interval
        self.try_qt_spec(cfg, False, "2014.0331 16:59:59")
        self.try_qt_spec(cfg, True, "2014.0331 17:00:00")
        self.try_qt_spec(cfg, True, "2014.0331 17:00:01")

        # day before, trailing edge of interval
        self.try_qt_spec(cfg, True, "2014.0331 22:59:59")
        self.try_qt_spec(cfg, True, "2014.0331 23:00:00")
        self.try_qt_spec(cfg, False, "2014.0331 23:00:01")

        # leading edge of date
        self.try_qt_spec(cfg, False, "2014.0331 23:59:59")
        self.try_qt_spec(cfg, True, "2014.0401 00:00:00")
        self.try_qt_spec(cfg, True, "2014.0401 00:00:01")

        # inside date, before interval
        self.try_qt_spec(cfg, True, "2014.0401 16:19:11")

        # inside date, inside interval
        self.try_qt_spec(cfg, True, "2014.0401 18:19:11")

        # inside date, after interval
        self.try_qt_spec(cfg, True, "2014.0401 23:17:11")

        # trailing edge of date
        self.try_qt_spec(cfg, True, "2014.0401 23:59:59")
        self.try_qt_spec(cfg, False, "2014.0402 00:00:00")
        self.try_qt_spec(cfg, False, "2014.0402 00:00:01")

        # day after, before interval
        self.try_qt_spec(cfg, False, "2014.0402 16:19:11")

        # day after, leading edge of interval
        self.try_qt_spec(cfg, False, "2014.0402 16:59:59")
        self.try_qt_spec(cfg, True, "2014.0402 17:00:00")
        self.try_qt_spec(cfg, True, "2014.0402 17:00:01")

        # day after, inside interval
        self.try_qt_spec(cfg, True, "2014.0402 22:58:01")

        # day after, trailing edge of interval
        self.try_qt_spec(cfg, True, "2014.0402 22:59:59")
        self.try_qt_spec(cfg, True, "2014.0402 23:00:00")
        self.try_qt_spec(cfg, False, "2014.0402 23:00:01")

        # day after, after interval
        self.try_qt_spec(cfg, False, "2014.0402 23:19:20")

    # --------------------------------------------------------------------------
    def test_quiet_time_rb_d_w(self):
        """
        Test a multiple quiet time spec. rsu boundary plus date plus weekday.
        """
        ldict = copy.deepcopy(self.cdict)
        ldict['crawler']['quiet_time'] = "14:00-19:00,2012.0117,Wednes"
        cfg = CrawlConfig.CrawlConfig()
        cfg.load_dict(ldict)

        # before any of them, on Monday
        self.try_qt_spec(cfg, False, "2012.0116 11:38:02")

        # leading edge of the interval on Monday
        self.try_qt_spec(cfg, False, "2012.0116 13:59:59")
        self.try_qt_spec(cfg, True, "2012.0116 14:00:00")
        self.try_qt_spec(cfg, True, "2012.0116 14:00:01")

        # trailing edge of the interval on Monday
        self.try_qt_spec(cfg, True, "2012.0116 18:59:59")
        self.try_qt_spec(cfg, True, "2012.0116 19:00:00")
        self.try_qt_spec(cfg, False, "2012.0116 19:00:01")

        # leading edge of Tuesday, the 17th
        self.try_qt_spec(cfg, False, "2012.0116 23:59:59")
        self.try_qt_spec(cfg, True, "2012.0117 00:00:00")
        self.try_qt_spec(cfg, True, "2012.0117 00:00:01")

        # lunchtime on Tuesday
        self.try_qt_spec(cfg, True, "2012.0117 12:00:00")

        # interval on Tuesday
        self.try_qt_spec(cfg, True, "2012.0117 15:00:00")

        # trailing edge of Tuesday, leading edge of Wednesday
        self.try_qt_spec(cfg, True, "2012.0117 23:59:59")
        self.try_qt_spec(cfg, True, "2012.0118 00:00:00")
        self.try_qt_spec(cfg, True, "2012.0118 00:00:01")

        # lunchtime on Wednesday
        self.try_qt_spec(cfg, True, "2012.0118 12:00:00")

        # interval on Wednesday
        self.try_qt_spec(cfg, True, "2012.0118 15:00:00")

        # trailing edge of Wednesday
        self.try_qt_spec(cfg, True, "2012.0118 23:59:59")
        self.try_qt_spec(cfg, False, "2012.0119 00:00:00")
        self.try_qt_spec(cfg, False, "2012.0119 00:00:01")

    # --------------------------------------------------------------------------
    def test_quiet_time_rb_w(self):
        """
        Test a multiple quiet time spec. RSU boundary plus weekday.
        """
        ldict = copy.deepcopy(self.cdict)
        ldict['crawler']['quiet_time'] = "14:00-19:00,sat,Wednes"
        cfg = CrawlConfig.CrawlConfig()
        cfg.load_dict(ldict)

        # 2014.0301 is a saturday -- all times quiet
        self.try_qt_spec(cfg, True, "2014.0301 13:59:59")
        self.try_qt_spec(cfg, True, "2014.0301 14:00:00")
        self.try_qt_spec(cfg, True, "2014.0301 14:00:01")

        # 2014.0305 is a wednesday -- all times quiet
        self.try_qt_spec(cfg, True, "2014.0305 18:59:59")
        self.try_qt_spec(cfg, True, "2014.0305 19:00:00")
        self.try_qt_spec(cfg, True, "2014.0305 19:00:01")

        # 2014.0330 is a sunday -- leading edge of interval
        self.try_qt_spec(cfg, False, "2014.0330 13:59:59")
        self.try_qt_spec(cfg, True, "2014.0330 14:00:00")
        self.try_qt_spec(cfg, True, "2014.0330 14:00:01")

        # 2014.0330 is a sunday -- trailing edge of interval
        self.try_qt_spec(cfg, True, "2014.0330 18:59:59")
        self.try_qt_spec(cfg, True, "2014.0330 19:00:00")
        self.try_qt_spec(cfg, False, "2014.0330 19:00:01")

    # --------------------------------------------------------------------------
    def test_quiet_time_ub_d(self):
        """
        Test a multiple quiet time spec. USD boundary plus date.
        """
        ldict = copy.deepcopy(self.cdict)
        ldict['crawler']['quiet_time'] = "19:00-8:15,2015.0217"
        cfg = CrawlConfig.CrawlConfig()
        cfg.load_dict(ldict)

        # in the early interval the day before
        self.try_qt_spec(cfg, True, "2015.0216 08:00:00")

        # trailing edge of the early interval the day before
        self.try_qt_spec(cfg, True, "2015.0216 08:14:59")
        self.try_qt_spec(cfg, True, "2015.0216 08:15:00")
        self.try_qt_spec(cfg, False, "2015.0216 08:15:01")

        # outside the intervals the day before
        self.try_qt_spec(cfg, False, "2015.0216 18:30:00")

        # leading edge of late interval the day before
        self.try_qt_spec(cfg, False, "2015.0216 18:59:59")
        self.try_qt_spec(cfg, True, "2015.0216 19:00:00")
        self.try_qt_spec(cfg, True, "2015.0216 19:00:01")

        # leading edge of the date
        self.try_qt_spec(cfg, True, "2015.0216 23:59:59")
        self.try_qt_spec(cfg, True, "2015.0217 00:00:00")
        self.try_qt_spec(cfg, True, "2015.0217 00:00:01")

        # trailing edge of the early interval in the date
        self.try_qt_spec(cfg, True, "2015.0217 08:14:59")
        self.try_qt_spec(cfg, True, "2015.0217 08:15:00")
        self.try_qt_spec(cfg, True, "2015.0217 08:15:01")

        # outside interval, in date
        self.try_qt_spec(cfg, True, "2015.0217 12:13:58")

        # leading edge of late interval in the date
        self.try_qt_spec(cfg, True, "2015.0217 18:59:59")
        self.try_qt_spec(cfg, True, "2015.0217 19:00:00")
        self.try_qt_spec(cfg, True, "2015.0217 19:00:01")

        # trailing edge of the date
        self.try_qt_spec(cfg, True, "2015.0217 23:59:59")
        self.try_qt_spec(cfg, True, "2015.0218 00:00:00")
        self.try_qt_spec(cfg, True, "2015.0218 00:00:01")

        # trailing edge of early interval the day after
        self.try_qt_spec(cfg, True, "2015.0218 08:14:59")
        self.try_qt_spec(cfg, True, "2015.0218 08:15:00")
        self.try_qt_spec(cfg, False, "2015.0218 08:15:01")

        # after early interval day after
        self.try_qt_spec(cfg, False, "2015.0218 11:12:13")

    # --------------------------------------------------------------------------
    def test_quiet_time_ub_d_w(self):
        """
        Test a multiple quiet time spec. usd boundary plus date plus weekday.
        """
        ldict = copy.deepcopy(self.cdict)
        ldict['crawler']['quiet_time'] = "14:00-19:00,sat"
        cfg = CrawlConfig.CrawlConfig()
        cfg.load_dict(ldict)

        # 2014.0301 is a saturday
        self.try_qt_spec(cfg, True, "2014.0301 13:59:59")
        self.try_qt_spec(cfg, True, "2014.0301 14:00:00")
        self.try_qt_spec(cfg, True, "2014.0301 14:00:01")

        # saturday trailing edge
        self.try_qt_spec(cfg, True, "2014.0301 23:59:59")
        self.try_qt_spec(cfg, False, "2014.0302 00:00:00")
        self.try_qt_spec(cfg, False, "2014.0302 00:00:01")

        # 2014.0305 is a wednesday -- interval leading edge
        self.try_qt_spec(cfg, False, "2014.0305 13:59:59")
        self.try_qt_spec(cfg, True, "2014.0305 14:00:00")
        self.try_qt_spec(cfg, True, "2014.0305 14:00:01")

        # 2014.0305 is a wednesday -- interval trailing edge
        self.try_qt_spec(cfg, True, "2014.0305 18:59:59")
        self.try_qt_spec(cfg, True, "2014.0305 19:00:00")
        self.try_qt_spec(cfg, False, "2014.0305 19:00:01")

        # 2014.0330 is a sunday -- interval trailing edge
        self.try_qt_spec(cfg, True, "2014.0330 18:59:59")
        self.try_qt_spec(cfg, True, "2014.0330 19:00:00")
        self.try_qt_spec(cfg, False, "2014.0330 19:00:01")

    # --------------------------------------------------------------------------
    def test_quiet_time_ub_w(self):
        """
        Test a multiple quiet time spec. USD boundary plus weekday.
        """
        ldict = copy.deepcopy(self.cdict)
        ldict['crawler']['quiet_time'] = "sat, sunday, 20:17 -06:45"
        cfg = CrawlConfig.CrawlConfig()
        cfg.load_dict(ldict)

        # Friday before 20:17
        self.try_qt_spec(cfg, False, "2012.0224 20:00:05")

        # friday at 20:17
        self.try_qt_spec(cfg, False, "2012.0224 20:16:59")
        self.try_qt_spec(cfg, True, "2012.0224 20:17:00")
        self.try_qt_spec(cfg, True, "2012.0224 20:17:01")

        # friday into saturday
        self.try_qt_spec(cfg, True, "2012.0224 23:59:59")
        self.try_qt_spec(cfg, True, "2012.0225 00:00:00")
        self.try_qt_spec(cfg, True, "2012.0225 00:00:01")

        # end of early interval saturday
        self.try_qt_spec(cfg, True, "2012.0225 06:44:59")
        self.try_qt_spec(cfg, True, "2012.0225 06:45:00")
        self.try_qt_spec(cfg, True, "2012.0225 06:45:01")

        # during day saturday
        self.try_qt_spec(cfg, True, "2012.0225 13:25:01")

        # start of late interval saturday
        self.try_qt_spec(cfg, True, "2012.0225 20:16:59")
        self.try_qt_spec(cfg, True, "2012.0225 20:17:00")
        self.try_qt_spec(cfg, True, "2012.0225 20:17:01")

        # saturday into sunday
        self.try_qt_spec(cfg, True, "2012.0225 23:59:59")
        self.try_qt_spec(cfg, True, "2012.0226 00:00:00")
        self.try_qt_spec(cfg, True, "2012.0226 00:00:01")

        # end of early interval sunday
        self.try_qt_spec(cfg, True, "2012.0226 06:44:59")
        self.try_qt_spec(cfg, True, "2012.0226 06:45:00")
        self.try_qt_spec(cfg, True, "2012.0226 06:45:01")

        # during day sunday
        self.try_qt_spec(cfg, True, "2012.0226 17:28:13")

        # start of late interval sunday
        self.try_qt_spec(cfg, True, "2012.0226 20:16:59")
        self.try_qt_spec(cfg, True, "2012.0226 20:17:00")
        self.try_qt_spec(cfg, True, "2012.0226 20:17:01")

        # sunday into monday
        self.try_qt_spec(cfg, True, "2012.0226 23:59:59")
        self.try_qt_spec(cfg, True, "2012.0227 00:00:00")
        self.try_qt_spec(cfg, True, "2012.0227 00:00:01")

        # end of early interval monday
        self.try_qt_spec(cfg, True, "2012.0227 06:44:59")
        self.try_qt_spec(cfg, True, "2012.0227 06:45:00")
        self.try_qt_spec(cfg, False, "2012.0227 06:45:01")

        # during day monday
        self.try_qt_spec(cfg, False, "2012.0227 20:00:19")

    # --------------------------------------------------------------------------
    def test_quiet_time_wday(self):
        """
        Test a weekday. The edges are inclusive.
        """
        ldict = copy.deepcopy(self.cdict)
        ldict['crawler']['quiet_time'] = "Wednes"
        cfg = CrawlConfig.CrawlConfig()
        cfg.load_dict(ldict)

        # 2014.0305 is a wednesday -- beginning of day
        self.try_qt_spec(cfg, False, "2014.0304 23:59:59")
        self.try_qt_spec(cfg, True, "2014.0305 00:00:00")
        self.try_qt_spec(cfg, True, "2014.0305 00:00:01")

        # 2014.0305 is a wednesday -- inside day
        self.try_qt_spec(cfg, True, "2014.0305 18:59:59")
        self.try_qt_spec(cfg, True, "2014.0305 19:00:00")
        self.try_qt_spec(cfg, True, "2014.0305 19:00:01")

        # 2014.0305 is a wednesday -- end of day
        self.try_qt_spec(cfg, True, "2014.0305 23:59:59")
        self.try_qt_spec(cfg, False, "2014.0306 00:00:00")
        self.try_qt_spec(cfg, False, "2014.0306 00:00:01")

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
                              'include': cfname_inc2
                              }
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
    def try_qt_spec(self, cfg, exp, tval):
        """
        Single quiet_time test.
        """
        try:
            qtspec = cfg.get('crawler', 'quiet_time')
        except CrawlConfig.NoOptionError:
            qtspec = "<empty>"

        actual = cfg.quiet_time(util.epoch(tval))
        self.assertEqual(exp, actual,
                         "'%s'/%s => expected '%s', got '%s'" %
                         (qtspec,
                          tval,
                          exp,
                          actual))

    # ------------------------------------------------------------------------
    def tearDown(self):
        """
        Clean up after every test.
        """
        util.conditional_rm(self.env_cfname)
