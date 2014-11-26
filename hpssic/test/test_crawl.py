"""
Tests for code in crawl.py
"""
from hpssic import crawl
from hpssic import crawl_lib
from hpssic import CrawlConfig
from hpssic import CrawlDBI
import copy
from hpssic import dbschem
from hpssic import fakesmtp
import glob
from hpssic import messages as MSG
import os
import pdb
import pexpect
import pytest
import re
import shutil
import sys
from hpssic import testhelp
import time
import tempfile
from hpssic import util
from hpssic import util as U


# ------------------------------------------------------------------------------
def tearDownModule():
    """
    Clean up after a sequence of tests.
    """
    CrawlConfig.log(close=True)

    if crawl.is_running(context=CrawlTest.ctx):
        rpl = crawl.running_pid()
        for c in rpl:
            util.touch(c[2])


# -----------------------------------------------------------------------------
class CrawlTest(testhelp.HelpedTestCase):
    """
    Tests for the code in crawl.py
    """
    piddir = "/tmp/crawler"
    pidglob = piddir + "/*"
    ctx = 'TEST'
    # more or less constant strings
    cstr = {
        'traceback': 'Traceback',
        'pfctx': 'pidfile for context ',
        'cdown': 'The crawler is not running.',
        'crun': 'The crawler is running as process',
        'prepstop': 'Preparing to stop TEST crawler. Proceed\? > ',
        'ldaemon': 'leaving daemonize',
        }

    # --------------------------------------------------------------------------
    def cfg_dict(self):
        cdict = {'crawler': {'plugin-dir': self.tmpdir('plugins'),
                             'logpath': self.logpath(),
                             'logsize': '5mb',
                             'logmax': '5',
                             'e-mail-recipients':
                             'tbarron@ornl.gov, tusculum@gmail.com',
                             'trigger': '<command-line>',
                             'plugins': 'plugin_A',
                             'sleep_time': '0.25',
                             'stopwait_timeout': '5.0',
                             },
                 'plugin_A': {'module': 'plugin_A',
                              'frequency': '1h',
                              'operations': '15'
                              },
                 'dbi-crawler': {'dbtype': 'sqlite',
                                 'dbname': self.dbname(),
                                 'tbl_prefix': 'test'
                                 }
                 }
        return cdict

    # --------------------------------------------------------------------------
    def logpath(self, tname='test_default_hpss_crawl'):
        return self.tmpdir('hpssic_crawl.log')


# -----------------------------------------------------------------------------
class CrawlMiscTest(CrawlTest):
    """
    Tests for the code in crawl.py
    """
    # --------------------------------------------------------------------------
    def crawl_test_setup(self):
        cfname = self.tmpdir('hpssic_test.cfg')
        lfname = self.logpath()
        exitpath = self.tmpdir("%s.exit" % self._testMethodName)
        plugdir = self.tmpdir("plugins")
        return(cfname, lfname, exitpath, plugdir)

    # --------------------------------------------------------------------------
    @pytest.mark.skipif(pytest.config.getvalue("fast"),
                        reason="slow -- omit --fast to run this one")
    def test_crawl_cfgdump_log_nopath(self):
        """
        TEST: "crawl cfgdump -c <cfgpath> --to log"
        EXP: what is written to log matches what was written to
        cfgpath. output should go to log path named in cfg.
        """
        self.dbgfunc()
        cfname = self.tmpdir('crawl.cfg')
        cdict = self.cfg_dict()
        self.write_cfg_file(cfname, cdict)
        cmd = '%s cfgdump -c %s --to log' % (self.crawl_cmd(), cfname)
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.assertPathPresent(self.logpath())
        lcontent = util.contents(self.logpath())
        for section in cdict.keys():
            self.vassert_in('[%s]' % section, lcontent)

            for item in cdict[section].keys():
                self.vassert_in('%s = %s' %
                                (item, cdict[section][item]), lcontent)

        self.vassert_nin('heartbeat', lcontent)
        self.vassert_nin('fire', lcontent)

    # --------------------------------------------------------------------------
    @pytest.mark.skipif(pytest.config.getvalue("fast"),
                        reason="slow -- omit --fast to run this one")
    def test_crawl_cfgdump_log_path(self):
        """
        TEST: "crawl cfgdump -c <cfgpath> --to log --log <logpath>"
        EXP: what is written to log matches what was written to
        cfgpath. output should go to logpath specified on command
        line.
        """
        self.dbgfunc()
        cfname = self.tmpdir('crawl.cfg')
        cdict = self.cfg_dict()
        self.write_cfg_file(cfname, cdict)
        cmd = ('%s cfgdump -c %s --to log --logpath %s'
               % (self.crawl_cmd(), cfname, self.logpath()))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.assertPathPresent(self.logpath())
        lcontent = util.contents(self.logpath())
        for section in cdict.keys():
            self.vassert_in('[%s]' % section, lcontent)

            for item in cdict[section].keys():
                self.vassert_in('%s = %s' %
                                (item, cdict[section][item]), lcontent)

        self.vassert_nin('heartbeat', lcontent)
        self.vassert_nin('fire', lcontent)

    # --------------------------------------------------------------------------
    @pytest.mark.skipif(pytest.config.getvalue("fast"),
                        reason="slow -- omit --fast to run this one")
    def test_crawl_cfgdump_nosuch(self):
        """
        TEST: "crawl cfgdump -c test.d/nosuch.cfg"
        EXP: attempting to open a nonexistent config file throws an error
        """
        cfname = self.tmpdir('nosuch.cfg')
        util.conditional_rm(cfname)
        cmd = '%s cfgdump -c %s' % (self.crawl_cmd(), cfname)
        result = testhelp.rm_cov_warn(pexpect.run(cmd))
        self.expected(util.squash(MSG.no_cfg_found), util.squash(result))

    # --------------------------------------------------------------------------
    def test_crawl_cfgdump_read(self):
        """
        TEST: "crawl cfgdump -c test.d/unreadable.cfg"
        EXP: attempting to open an unreadable config file throws an error
        """
        cfname = self.tmpdir('unreadable.cfg')
        open(cfname, 'w').close()
        os.chmod(cfname, 0000)

        cmd = '%s cfgdump -c %s' % (self.crawl_cmd(), cfname)
        result = pexpect.run(cmd)
        self.vassert_in("Traceback", result)
        self.vassert_in("%s is not readable" % cfname,
                        result)

    # --------------------------------------------------------------------------
    def test_crawl_cfgdump_stdout(self):
        """
        TEST: "crawl cfgdump -c <cfgpath> --to stdout"
        EXP: what is written to stdout matches what was written to cfgpath
        """
        cfname = self.tmpdir("test_crawl.cfg")
        cdict = self.cfg_dict()
        self.write_cfg_file(cfname, cdict)
        cmd = '%s cfgdump -c %s --to stdout' % (self.crawl_cmd(), cfname)
        result = pexpect.run(cmd)
        for section in cdict.keys():
            self.vassert_in('[%s]' % section, result)

            for item in cdict[section].keys():
                self.vassert_in('%s = %s' %
                                (item, cdict[section][item]), result)

    # --------------------------------------------------------------------------
    def test_crawl_fire_badplug(self):
        """
        TEST: crawl fire -p <not-a-plugmod>
        EXP: error message that plugin not found in config
        """
        cfname = self.tmpdir("crawl.cfg")
        self.write_cfg_file(cfname, self.cfg_dict())
        with U.Chdir(self.tmpdir()):
            plugname = 'no_such_plugin'
            result = pexpect.run("%s fire -p %s" % (self.crawl_cmd(),
                                                    plugname))
            exp = "No plugin named '%s' found in configuration" % plugname
            self.vassert_in(exp, result)

    # --------------------------------------------------------------------------
    @pytest.mark.skipif(pytest.config.getvalue("fast"),
                        reason="slow -- omit --fast to run this one")
    def test_crawl_fire_log_path(self):
        """
        TEST: crawl fire --plugin <plugmod>
        EXP: plugin fired and output went to specified log path
        """
        (cfname, lfname, exitpath, plugdir) = self.crawl_test_setup()
        plugname = 'plugin_1'

        # create a plug module
        self.write_plugmod(plugdir, plugname)

        # add the plug module to the config
        t = CrawlConfig.CrawlConfig.dictor(self.cfg_dict())
        t.add_section(plugname)
        t.set(plugname, 'frequency', '1m')
        f = open(cfname, 'w')
        t.crawl_write(f)
        f.close()

        # carry out the test
        cmd = ('%s fire -c %s --plugin %s --logpath %s' %
               (self.crawl_cmd(), cfname, plugname, lfname))
        result = pexpect.run(cmd)

        # verify that command ran successfully
        self.vassert_nin("Traceback", result)

        # test.d/plugins/plugin_1.py should exist
        if not plugname.endswith('.py'):
            plugname += '.py'
        self.assertPathPresent('%s/%s' % (plugdir, plugname))

        # fired should exist and contain 'plugin plugin_1 fired'
        filename = self.tmpdir('fired')
        self.assertPathPresent(filename)
        self.vassert_in('plugin plugin_1 fired', util.contents(filename))

        # lfname should exist and contain specific strings
        self.assertPathPresent(lfname)
        self.vassert_in('firing plugin_1', util.contents(lfname))

    # --------------------------------------------------------------------------
    def test_crawl_fire_noplug(self):
        """
        TEST: crawl fire <plugmod>
        EXP: error message that "-p <plugin-name>" is required
        """
        plugname = 'plugin_1'
        result = pexpect.run("%s fire %s" % (self.crawl_cmd(), plugname))
        exp = "'-p <plugin-name>' is required"
        self.vassert_in(exp, result)

    # --------------------------------------------------------------------------
    def test_crawl_lib_drop_table_000(self):
        """
        TEST: crawl_lib.drop_table()
        EXP: MSG.nothing_to_drop returned
        """
        CrawlConfig.add_config(close=True, dct=self.cfg_dict())
        rsp = dbschem.drop_table()
        self.expected(MSG.nothing_to_drop, rsp)

    # --------------------------------------------------------------------------
    def test_crawl_lib_drop_table_001(self):
        """
        TEST: crawl_lib.drop_table(table=NAME)
        EXP: table NAME is dropped
        """
        self.dbgfunc()
        tname = util.my_name()
        cfg = CrawlConfig.add_config(close=True, dct=self.cfg_dict())
        db = CrawlDBI.DBI(dbtype='crawler')
        db.create(table=tname,
                  fields=['rowid integer primary key autoincrement'])
        actual = dbschem.drop_table(table=tname)
        exp = ("Attempt to drop table '%s' was successful" % tname)
        self.expected(exp, actual)

    # --------------------------------------------------------------------------
    def test_crawl_lib_drop_table_010(self):
        """
        TEST: crawl_lib.drop_table(prefix=PFX)
        EXP: MSG.nothing_to_drop returned
        """
        self.dbgfunc()
        CrawlConfig.add_config(close=True, dct=self.cfg_dict())
        rsp = dbschem.drop_table(prefix="BORF")
        self.expected(MSG.nothing_to_drop, rsp)

    # --------------------------------------------------------------------------
    def test_crawl_lib_drop_table_011(self):
        """
        TEST: crawl_lib.drop_table(prefix=PFX, table=NAME)
        EXP: table PFX.NAME already does not exist, fails because PFX does not
        match cfg
        """
        self.dbgfunc()
        tname = util.my_name()
        pfx = "nosuch"
        cfg = CrawlConfig.add_config(close=True, dct=self.cfg_dict())
        db = CrawlDBI.DBI(dbtype='crawler')
        if not db.table_exists(table=tname):
            db.create(table=tname,
                      fields=['rowid integer primary key autoincrement'])
        actual = dbschem.drop_table(prefix=pfx, table=tname)
        exp = ("Table '%s' does not exist" % (tname))
        self.expected(exp, actual)
        db.drop(table=tname)

    # --------------------------------------------------------------------------
    def test_crawl_lib_drop_table_100(self):
        """
        TEST: crawl_lib.drop_table(cfg=CFG)
        EXP: MSG.nothing_to_drop returned
        """
        t = CrawlConfig.CrawlConfig.dictor(self.cfg_dict())
        rsp = dbschem.drop_table(cfg=t)
        self.expected(MSG.nothing_to_drop, rsp)

    # --------------------------------------------------------------------------
    def test_crawl_lib_drop_table_101(self):
        """
        TEST: crawl_lib.drop_table(cfg=CFG, table=NAME)
        EXP: table NAME is dropped
        """
        tname = util.my_name()
        cfg = CrawlConfig.add_config(close=True, dct=self.cfg_dict())
        cfg.set('dbi-crawler', 'tbl_prefix', 'DTST')
        db = CrawlDBI.DBI(dbtype='crawler')
        db.create(table=tname,
                  fields=['rowid integer primary key autoincrement'])
        actual = dbschem.drop_table(cfg=cfg, table=tname)
        exp = ("Attempt to drop table '%s' was successful" % tname)
        self.expected(exp, actual)

    # --------------------------------------------------------------------------
    def test_crawl_lib_drop_table_110(self):
        """
        TEST: crawl_lib.drop_table(cfg=CFG, prefix=PFX)
        EXP: MSG.nothing_to_drop returned
        """
        t = CrawlConfig.CrawlConfig.dictor(self.cfg_dict())
        rsp = dbschem.drop_table(cfg=t, prefix="SEVEN")
        self.expected(MSG.nothing_to_drop, rsp)

    # --------------------------------------------------------------------------
    def test_crawl_lib_drop_table_111(self):
        """
        TEST: crawl_lib.drop_table(cfg=CFG, prefix=PFX, table=NAME)
        EXP: table named PFX_NAME is dropped if PFX matches cfg
        """
        tname = util.my_name()
        pfx = 'DTST'
        cfg = CrawlConfig.add_config(close=True, dct=self.cfg_dict())
        cfg.set('dbi-crawler', 'tbl_prefix', pfx)
        db = CrawlDBI.DBI(dbtype='crawler')
        db.create(table=tname,
                  fields=['rowid integer primary key autoincrement'])

        actual = dbschem.drop_table(cfg=cfg, prefix=pfx+'x', table=tname)
        exp = ("Table '%s' does not exist" % (tname))
        self.expected(exp, actual)

        actual = dbschem.drop_table(cfg=cfg, prefix=pfx, table=tname)
        exp = ("Attempt to drop table '%s' was successful" % tname)
        self.expected(exp, actual)

    # --------------------------------------------------------------------------
    def test_crawl_log(self):
        """
        TEST: "crawl log --log filename message" will write message to filename
        """
        lfname = self.logpath()
        msg = "this is a test log message"
        cmd = ("%s log --log %s %s" % (self.crawl_cmd(), lfname, msg))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_in(msg, util.contents(lfname))

    # --------------------------------------------------------------------------
    @pytest.mark.skipif(pytest.config.getvalue("fast"),
                        reason="slow -- omit --fast to run this one")
    def test_crawl_start_already(self):
        """
        TEST: If the crawler is already running, decline to run a second copy.
        """
        (cfgpath, logpath, exitpath, plugdir) = self.crawl_test_setup()

        xdict = copy.deepcopy(self.cfg_dict())
        xdict['crawler']['exitpath'] = exitpath
        xdict['crawler']['context'] = self.ctx
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(plugdir, 'plugin_A')
        cmd = ('%s start --log %s --cfg %s --context %s' %
               (self.crawl_cmd(), logpath, cfgpath, self.ctx))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)

        self.assertTrue(crawl.is_running(context=self.ctx),
                        "%s result=%s" %
                        ("Expected crawler to be running but it is not.",
                         util.line_quote(result)))

        result = pexpect.run(cmd)
        self.expected_in(self.cstr['pfctx'], result)

        util.touch(exitpath)

        crawl.stop_wait(cfg=CrawlConfig.CrawlConfig.dictor(xdict))
        self.assertFalse(crawl.is_running(context=self.ctx),
                         "Expected crawler %s to be down but it is running" %
                         self.ctx)

    # --------------------------------------------------------------------------
    @pytest.mark.skipif(pytest.config.getvalue("fast"),
                        reason="slow -- omit --fast to run this one")
    def test_crawl_start_cfg(self):
        """
        TEST: 'crawl start' should fire up a daemon crawler which will exit
        when the exit file is touched. Verify that the correct config
        file is loaded.
        """
        (cfgpath, logpath, exitpath, plugdir) = self.crawl_test_setup()

        xdict = copy.deepcopy(self.cfg_dict())
        xdict['crawler']['exitpath'] = exitpath
        xdict['crawler']['context'] = self.ctx
        xdict['crawler']['plugins'] = 'plugin_A, other_plugin'
        xdict['crawler']['logpath'] = logpath
        xdict['other_plugin'] = {'unplanned': 'silver',
                                 'simple': 'check for this',
                                 'module': 'other_plugin'}
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(plugdir, 'plugin_A')
        self.write_plugmod(plugdir, 'other_plugin')
        pre_l = glob.glob(self.pidglob)
        cmd = ('%s start --log %s --cfg %s --context %s'
               % (self.crawl_cmd(), logpath, cfgpath, self.ctx))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)

        self.assertTrue(crawl.is_running(context=self.ctx),
                        "Expected crawler to be running but it is not")
        up_l = glob.glob(self.pidglob)
        pidfile = (set(up_l) - set(pre_l)).pop()

        self.assertPathPresent(logpath)
        exp = 'crawl: CONFIG: [other_plugin]'
        self.expected_in(exp, util.contents(logpath))
        self.expected_in('crawl: CONFIG: unplanned: silver',
                         util.contents(logpath))
        self.expected_in('crawl: CONFIG: simple: check for this',
                         util.contents(logpath))

        util.touch(exitpath)

        crawl.stop_wait(cfg=CrawlConfig.CrawlConfig.dictor(xdict))
        self.assertFalse(crawl.is_running(context=self.ctx),
                         "Expected crawler %s to be down but it is running" %
                         self.ctx)
        self.assertPathNotPresent(pidfile)

    # --------------------------------------------------------------------------
    @pytest.mark.skipif(pytest.config.getvalue("fast"),
                        reason="slow -- omit --fast to run this one")
    def test_crawl_start_cfgctx(self):
        """
        TEST: 'crawl start' should fire up a daemon crawler which will exit
        when the exit file is touched. Verify that the crawler is started with
        the context specified in the config file.
        """
        (cfgpath, logpath, exitpath, plugdir) = self.crawl_test_setup()

        xdict = copy.deepcopy(self.cfg_dict())
        xdict['crawler']['context'] = self.ctx
        xdict['crawler']['exitpath'] = exitpath
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(plugdir, 'plugin_A')
        pre_l = glob.glob(self.pidglob)
        cmd = ('%s start --log %s --cfg %s'
               % (self.crawl_cmd(), logpath, cfgpath))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'] + self.ctx, result)

        self.assertTrue(crawl.is_running(context=self.ctx),
                        "Expected crawler to still be running but it is not")
        up_l = glob.glob(self.pidglob)
        pidfile = (set(up_l) - set(pre_l)).pop()
        x = util.contents(pidfile).strip().split()
        self.expected(self.ctx, x[0])
        self.expected(exitpath, x[1])
        self.assertPathPresent(logpath)
        self.expected_in(self.cstr['ldaemon'], util.contents(logpath))

        util.touch(exitpath)

        crawl.stop_wait(cfg=CrawlConfig.CrawlConfig.dictor(xdict))
        self.assertFalse(crawl.is_running(context=self.ctx),
                         "Expected crawler %s to be down but it is running" %
                         self.ctx)
        self.assertPathNotPresent(pidfile)

    # --------------------------------------------------------------------------
    @pytest.mark.skipif(pytest.config.getvalue("fast"),
                        reason="slow -- omit --fast to run this one")
    def test_crawl_start_cmdctx(self):
        """
        TEST: 'crawl start' should fire up a daemon crawler which will exit
        when the exit file is touched. Verify that the crawler is started with
        the context provided on the command line.
        """
        (cfgpath, logpath, exitpath, plugdir) = self.crawl_test_setup()

        xdict = copy.deepcopy(self.cfg_dict())
        xdict['crawler']['exitpath'] = exitpath
        xdict['crawler']['context'] = self.ctx
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(plugdir, 'plugin_A')
        pre_l = glob.glob(self.pidglob)
        cmd = ('%s start --log %s --cfg %s --context %s' %
               (self.crawl_cmd(), logpath, cfgpath, self.ctx))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'] + self.ctx, result)

        self.assertTrue(crawl.is_running(context=self.ctx),
                        "Expected crawler to still be running but it is not")
        up_l = glob.glob(self.pidglob)
        pidfile = (set(up_l) - set(pre_l)).pop()
        x = util.contents(pidfile).strip().split()
        self.expected(self.ctx, x[0])
        self.assertPathPresent(logpath)
        self.expected_in(self.cstr['ldaemon'], util.contents(logpath))

        util.touch(exitpath)

        crawl.stop_wait(cfg=CrawlConfig.CrawlConfig.dictor(xdict))
        self.assertFalse(crawl.is_running(context=self.ctx),
                         "Expected crawler %s to be down but it is running" %
                         self.ctx)
        self.assertPathNotPresent(pidfile)

    # --------------------------------------------------------------------------
    # def test_crawl_start_defctx(self):
    #     """
    #     There is no default context, so there's no point in having a test for
    #     it. This entry is left in place to document that fact.
    #     """

    # --------------------------------------------------------------------------
    @pytest.mark.skipif(pytest.config.getvalue("fast"),
                        reason="slow -- omit --fast to run this one")
    def test_crawl_start_fire(self):
        """
        TEST: 'crawl start' should fire up a daemon crawler which will exit
        when the exit file is touched. Verify that at least one plugin
        fires and produces some output.
        """
        (cfgpath, logpath, exitpath, plugdir) = self.crawl_test_setup()

        xdict = copy.deepcopy(self.cfg_dict())
        xdict['crawler']['exitpath'] = exitpath
        xdict['crawler']['context'] = self.ctx
        xdict['crawler']['logpath'] = logpath
        xdict['other'] = {'frequency': '1s',
                          'fire': 'true',
                          'module': 'other'}
        xdict['crawler']['verbose'] = 'true'
        xdict['crawler']['plugins'] = 'other'
        del xdict['plugin_A']
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(plugdir, 'other')
        pre_l = glob.glob(self.pidglob)
        cmd = ('%s start --log %s --cfg %s --context %s'
               % (self.crawl_cmd(), logpath, cfgpath, self.ctx))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)

        self.assertTrue(crawl.is_running(context=self.ctx),
                        "Expected crawler to still be running but it isn't")
        up_l = glob.glob(self.pidglob)
        pidfile = (set(up_l) - set(pre_l)).pop()

        self.assertPathPresent(logpath)
        self.expected_in(self.cstr['ldaemon'], util.contents(logpath))
        time.sleep(2)
        self.expected_in('other: firing', util.contents(logpath))
        self.assertTrue(self.tmpdir('fired'),
                        "File %s/fired does not exist" % self.tmpdir())
        self.expected('plugin other fired\n',
                      util.contents(self.tmpdir('fired')))

        util.touch(exitpath)

        crawl.stop_wait(cfg=CrawlConfig.CrawlConfig.dictor(xdict))
        self.assertFalse(crawl.is_running(context=self.ctx),
                         "Expected crawler %s to be down but it is running" %
                         self.ctx)
        self.assertPathNotPresent(pidfile)

    # --------------------------------------------------------------------------
    def test_crawl_start_nocfg(self):
        """
        'crawl start' with no configuration available should throw an exception
        """
        with util.tmpenv('CRAWL_CONF', None):
            where = tempfile.mkdtemp(dir=self.tmpdir())
            exp = util.squash(MSG.no_cfg_found)
            cmd = "%s start" % self.crawl_cmd()
            with util.Chdir(where):
                result = util.squash(testhelp.rm_cov_warn(pexpect.run(cmd)))
                self.expected(exp, result)

    # --------------------------------------------------------------------------
    @pytest.mark.skipif(pytest.config.getvalue("fast"),
                        reason="slow -- omit --fast to run this one")
    def test_crawl_start_noexit(self):
        """
        'crawl start' with no exit path in the config file should throw an
        exception.
        """
        (cfgpath, logpath, exitpath, plugdir) = self.crawl_test_setup()

        self.write_cfg_file(cfgpath, self.cfg_dict())
        self.write_plugmod(plugdir, 'plugin_A')

        cmd = ('%s start --log %s --cfg %s --context %s' %
               (self.crawl_cmd(), logpath, cfgpath, self.ctx))
        result = pexpect.run(cmd)

        self.vassert_in("No exit path is specified in the configuration",
                        result)

        self.assertFalse(crawl.is_running(context=self.ctx),
                         "Crawler should not have started")

    # --------------------------------------------------------------------------
    @pytest.mark.skipif(pytest.config.getvalue("fast"),
                        reason="slow -- omit --fast to run this one")
    def test_crawl_start_nonplugin_sections(self):
        """
        TEST: 'crawl start' should fire up a daemon crawler which will exit
        when the exit file is touched. Verify that a config file with
        non-plugin sections works properly. Sections that are not listed with
        the 'plugin' option in the 'crawler' section should not be loaded as
        plugins.
        """
        (cfgpath, logpath, exitpath, plugdir) = self.crawl_test_setup()

        xdict = copy.deepcopy(self.cfg_dict())
        xdict['alerts'] = {}
        xdict['alerts']['email'] = 'one@somewhere.com, two@elsewhere.org'
        xdict['alerts']['log'] = '!!!ALERT!!! %s'
        xdict['crawler']['exitpath'] = exitpath
        xdict['crawler']['context'] = self.ctx
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(plugdir, 'plugin_A')

        pre_l = glob.glob(self.pidglob)
        cmd = ('%s start --log %s --cfg %s --context %s' %
               (self.crawl_cmd(), logpath, cfgpath, self.ctx))
        result = pexpect.run(cmd)

        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)

        up_l = glob.glob(self.pidglob)
        pidfile = (set(up_l) - set(pre_l)).pop()

        util.touch(exitpath)

        crawl.stop_wait(cfg=CrawlConfig.CrawlConfig.dictor(xdict))
        self.vassert_nin("Traceback", util.contents(logpath))
        self.assertFalse(crawl.is_running(context=self.ctx),
                         "Expected crawler %s to be down but it is running" %
                         self.ctx)
        self.assertPathNotPresent(pidfile)

    # --------------------------------------------------------------------------
    @pytest.mark.skipif(pytest.config.getvalue("fast"),
                        reason="slow -- omit --fast to run this one")
    def test_crawl_start_x(self):
        """
        TEST: 'crawl start' should fire up a daemon crawler which will exit
        when the exit file is touched. Verify that crawler_pid exists
        while crawler is running and that it is removed when it stops.
        """
        (cfgpath, logpath, exitpath, plugdir) = self.crawl_test_setup()

        xdict = copy.deepcopy(self.cfg_dict())
        xdict['crawler']['exitpath'] = exitpath
        xdict['crawler']['context'] = self.ctx
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(plugdir, 'plugin_A')
        pre_l = glob.glob(self.pidglob)
        cmd = ('%s start --log %s --cfg %s --context %s' %
               (self.crawl_cmd(), logpath, cfgpath, self.ctx))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'] + self.ctx, result)

        self.assertTrue(crawl.is_running(context=self.ctx),
                        "Expected crawler to still be running but it is not")
        up_l = glob.glob(self.pidglob)
        self.expected(len(pre_l) + 1, len(up_l))
        self.assertPathPresent(logpath)
        self.expected_in(self.cstr['ldaemon'], util.contents(logpath))

        util.touch(exitpath)

        crawl.stop_wait(cfg=CrawlConfig.CrawlConfig.dictor(xdict))
        self.assertFalse(crawl.is_running(context=self.ctx),
                         "Expected crawler %s to be down but it is running" %
                         self.ctx)
        down_l = glob.glob(self.pidglob)
        self.expected(len(pre_l), len(down_l))

    # --------------------------------------------------------------------------
    @pytest.mark.skipif(pytest.config.getvalue("fast"),
                        reason="slow -- omit --fast to run this one")
    def test_crawl_status(self):
        """
        TEST: 'crawl status' should report the crawler status correctly.
        """
        self.dbgfunc()
        (cfgpath, logpath, exitpath, plugdir) = self.crawl_test_setup()

        xdict = copy.deepcopy(self.cfg_dict())
        xdict['crawler']['context'] = self.ctx
        xdict['crawler']['exitpath'] = exitpath
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(plugdir, 'plugin_A')
        cmd = '%s status' % (self.crawl_cmd())
        result = testhelp.rm_cov_warn(pexpect.run(cmd))
        self.expected(result.strip(), self.cstr['cdown'])

        pre_l = glob.glob(self.pidglob)
        cmd = ('%s start --log %s --cfg %s' % (self.crawl_cmd(),
                                               logpath, cfgpath))
        result = testhelp.rm_cov_warn(pexpect.run(cmd))
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)

        self.assertTrue(crawl.is_running(context=self.ctx),
                        "Expected crawler to be running but it is not")
        up_l = glob.glob(self.pidglob)
        pidfile = (set(up_l) - set(pre_l)).pop()
        self.assertPathPresent(pidfile)

        cmd = '%s status' % self.crawl_cmd()
        result = testhelp.rm_cov_warn(pexpect.run(cmd))
        self.expected_in(self.cstr['crun'], result)
        self.expected_in('context=%s' % self.ctx, result)

        cmd = '%s stop --log %s --context %s' % (self.crawl_cmd(),
                                                 logpath, self.ctx)
        result = testhelp.rm_cov_warn(pexpect.run(cmd))
        self.vassert_nin("Traceback", result)

        crawl.stop_wait(cfg=CrawlConfig.CrawlConfig.dictor(xdict))
        self.assertFalse(crawl.is_running(context=self.ctx),
                         "Expected crawler %s to be down but it is running" %
                         self.ctx)
        self.assertPathNotPresent(pidfile)

        cmd = '%s status' % self.crawl_cmd()
        result = testhelp.rm_cov_warn(pexpect.run(cmd))
        self.expected(self.cstr['cdown'], result.strip())

    # --------------------------------------------------------------------------
    @pytest.mark.skipif(pytest.config.getvalue("fast"),
                        reason="slow -- omit --fast to run this one")
    def test_crawl_status_multi(self):
        """
        TEST: 'crawl status' should report the crawler status correctly.
        """
        logpath = self.logpath()
        cfgpath_a = self.tmpdir('%s_a.cfg' % (util.my_name()))
        exitpath_a = self.tmpdir("%s.exit_a" % (util.my_name()))
        ctx_a = self.ctx

        cfgpath_b = self.tmpdir("%s_b.cfg" % util.my_name())
        exitpath_b = self.tmpdir("%s.exit_b" % util.my_name())
        ctx_b = 'DEV'

        plugdir = self.tmpdir("plugins")

        xdict = copy.deepcopy(self.cfg_dict())
        xdict['crawler']['context'] = ctx_a
        xdict['crawler']['exitpath'] = exitpath_a
        cfg_a = CrawlConfig.CrawlConfig.dictor(xdict)
        self.write_cfg_file(cfgpath_a, xdict)

        xdict['crawler']['context'] = ctx_b
        xdict['crawler']['exitpath'] = exitpath_b
        cfg_b = CrawlConfig.CrawlConfig.dictor(xdict)
        self.write_cfg_file(cfgpath_b, xdict)

        self.write_plugmod(plugdir, 'plugin_A')
        cmd = '%s status' % self.crawl_cmd()
        result = testhelp.rm_cov_warn(pexpect.run(cmd))
        self.expected(self.cstr['cdown'], result.strip())

        pre_l = glob.glob(self.pidglob)
        # start crawler A
        cmd = ('%s start --log %s --cfg %s' %
               (self.crawl_cmd(), logpath, cfgpath_a))
        result = testhelp.rm_cov_warn(pexpect.run(cmd))
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)
        a_up_l = glob.glob(self.pidglob)

        # start crawler B
        cmd = ('%s start --log %s --cfg %s'
               % (self.crawl_cmd(), logpath, cfgpath_b))
        result = testhelp.rm_cov_warn(pexpect.run(cmd))
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)
        b_up_l = glob.glob(self.pidglob)

        pidfile_a = (set(a_up_l) - set(pre_l)).pop()
        pidfile_b = (set(b_up_l) - set(a_up_l)).pop()

        self.assertTrue(crawl.is_running(context=ctx_a),
                        "Expected crawler %s to be running but it is not" %
                        ctx_a)
        self.assertTrue(crawl.is_running(context=ctx_b),
                        "Expected crawler %s to be running but it is not" %
                        ctx_b)
        self.assertPathPresent(pidfile_a)
        self.assertPathPresent(pidfile_b)

        cmd = '%s status' % self.crawl_cmd()
        result = testhelp.rm_cov_warn(pexpect.run(cmd))
        self.vassert_in(self.cstr['crun'], result)
        self.expected_in('context=%s' % ctx_a, result)
        self.expected_in('context=%s' % ctx_b, result)

        cmd = '%s stop --log %s --context %s' % (self.crawl_cmd(),
                                                 logpath, ctx_a)
        result = testhelp.rm_cov_warn(pexpect.run(cmd))
        self.vassert_nin("Traceback", result)

        cmd = '%s stop --log %s --context %s' % (self.crawl_cmd(),
                                                 logpath, ctx_b)
        result = testhelp.rm_cov_warn(pexpect.run(cmd))
        self.vassert_nin("Traceback", result)

        crawl.stop_wait(cfg=cfg_a)
        crawl.stop_wait(cfg=cfg_b)

        self.assertFalse(crawl.is_running(context=ctx_a),
                         "Crawler %s should not be running but it is" % ctx_a)
        self.assertFalse(crawl.is_running(context=ctx_b),
                         "Crawler %s should not be running but it is" % ctx_b)
        self.assertPathNotPresent(pidfile_a)
        self.assertPathNotPresent(pidfile_b)

        cmd = '%s status' % self.crawl_cmd()
        result = testhelp.rm_cov_warn(pexpect.run(cmd))
        self.expected(self.cstr['cdown'], result.strip())

    # --------------------------------------------------------------------------
    @pytest.mark.skipif(pytest.config.getvalue("fast"),
                        reason="slow -- omit --fast to run this one")
    def test_crawl_stop_confirm(self):
        """
        TEST: 'crawl stop' should cause a running daemon to shut down. If no
        context is specified, the user should be asked to confirm the shutdown.
        """
        (cfgpath, logpath, exitpath, plugdir) = self.crawl_test_setup()

        xdict = copy.deepcopy(self.cfg_dict())
        xdict['crawler']['context'] = self.ctx
        xdict['crawler']['exitpath'] = exitpath
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(plugdir, 'plugin_A')

        pre_l = glob.glob(self.pidglob)
        cmd = ('%s start --log %s --cfg %s --context %s' %
               (self.crawl_cmd(), logpath, cfgpath, self.ctx))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)

        self.assertTrue(crawl.is_running(context=self.ctx),
                        "Expected the crawler to be running but it is not")
        up_l = glob.glob(self.pidglob)
        pidfile = (set(up_l) - set(pre_l)).pop()

        cmd = '%s stop --log %s' % (self.crawl_cmd(), logpath)
        S = pexpect.spawn(cmd)
        S.expect(self.cstr['prepstop'])
        S.sendline("no")
        S.expect(pexpect.EOF)
        self.vassert_in("No action taken", S.before)
        S.close()

        self.assertTrue(crawl.is_running(context=self.ctx))
        self.assertPathPresent(pidfile)

        cmd = '%s stop --log %s' % (self.crawl_cmd(), logpath)
        S = pexpect.spawn(cmd)
        S.expect(self.cstr['prepstop'])
        S.sendline("yes")
        S.expect(pexpect.EOF)
        self.vassert_in("Stopping the crawler...", S.before)
        S.close()

        crawl.stop_wait(cfg=CrawlConfig.CrawlConfig.dictor(xdict))
        self.assertFalse(crawl.is_running(context=self.ctx),
                         "Expected crawler %s to be down but it is running" %
                         self.ctx)
        self.assertPathNotPresent(pidfile)

    # --------------------------------------------------------------------------
    @pytest.mark.skipif(pytest.config.getvalue("fast"),
                        reason="slow -- omit --fast to run this one")
    def test_crawl_stop_ctx(self):
        """
        TEST: 'crawl stop' should cause a running daemon to shut down. If the
        correct context is specified, the crawler will be shutdown without
        prompting.
        """
        (cfgpath, logpath, exitpath, plugdir) = self.crawl_test_setup()

        xdict = copy.deepcopy(self.cfg_dict())
        xdict['crawler']['context'] = self.ctx
        xdict['crawler']['exitpath'] = exitpath
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(plugdir, 'plugin_A')

        pre_l = glob.glob(self.pidglob)
        cmd = ('%s start --log %s --cfg %s --context %s' %
               (self.crawl_cmd(), logpath, cfgpath, self.ctx))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)

        self.assertTrue(crawl.is_running(context=self.ctx),
                        "Expected the crawler to be running but it is not")
        up_l = glob.glob(self.pidglob)
        pidfile = (set(up_l) - set(pre_l)).pop()

        cmd = '%s stop --log %s --context %s' % (self.crawl_cmd(),
                                                 logpath, self.ctx)
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_in("Stopping the TEST crawler", result)

        crawl.stop_wait(cfg=CrawlConfig.CrawlConfig.dictor(xdict))
        self.assertFalse(crawl.is_running(context=self.ctx),
                         "Expected crawler %s to be down but it is running" %
                         self.ctx)
        self.assertPathNotPresent(pidfile)

    # --------------------------------------------------------------------------
    @pytest.mark.skipif(pytest.config.getvalue("fast"),
                        reason="slow -- omit --fast to run this one")
    def test_crawl_stop_ctxoth(self):
        """
        TEST: 'crawl stop' with a context other than the running crawler will
        do nothing.
        """
        (cfgpath, logpath, exitpath, plugdir) = self.crawl_test_setup()
        xdict = copy.deepcopy(self.cfg_dict())
        xdict['crawler']['context'] = self.ctx
        xdict['crawler']['exitpath'] = exitpath
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(plugdir, 'plugin_A')

        pre_l = glob.glob(self.pidglob)
        cmd = ('%s start --log %s --cfg %s --context %s' %
               (self.crawl_cmd(), logpath, cfgpath, self.ctx))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)

        self.assertTrue(crawl.is_running(context=self.ctx),
                        "Expected the crawler to be running but it is not")

        up_l = glob.glob(self.pidglob)
        pidfile = (set(up_l) - set(pre_l)).pop()

        cmd = '%s stop --log %s --context DEV' % (self.crawl_cmd(), logpath)
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_in("No DEV crawler is running", result)

        self.assertTrue(crawl.is_running(context=self.ctx))

        cmd = '%s stop --log %s --context TEST' % (self.crawl_cmd(), logpath)
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin("No DEV crawler is running", result)

        crawl.stop_wait(cfg=CrawlConfig.CrawlConfig.dictor(xdict))
        self.assertFalse(crawl.is_running(context=self.ctx),
                         "Expected crawler %s to be down but it is running" %
                         self.ctx)
        self.assertPathNotPresent(pidfile)

    # --------------------------------------------------------------------------
    @pytest.mark.skipif(pytest.config.getvalue("fast"),
                        reason="slow -- omit --fast to run this one")
    def test_crawl_stop_noctx(self):
        """
        TEST: 'crawl stop' with multiple crawlers running and no args should
        ask the user to specify a context.
        """
        logpath = self.logpath()
        cfgpath_a = self.tmpdir('%s_a.cfg' % util.my_name())
        exitpath_a = self.tmpdir('%s_a.exit' % util.my_name())
        ctx_a = self.ctx

        cfgpath_b = self.tmpdir("%s_b.cfg" % util.my_name())
        exitpath_b = self.tmpdir("%s_b.exit" % util.my_name())
        ctx_b = 'DEV'

        plugdir = self.tmpdir('plugins')

        xdict = copy.deepcopy(self.cfg_dict())
        xdict['crawler']['context'] = ctx_a
        xdict['crawler']['exitpath'] = exitpath_a
        cfg_a = CrawlConfig.CrawlConfig.dictor(xdict)
        self.write_cfg_file(cfgpath_a, xdict)

        xdict['crawler']['context'] = ctx_b
        xdict['crawler']['exitpath'] = exitpath_b
        cfg_b = CrawlConfig.CrawlConfig.dictor(xdict)
        self.write_cfg_file(cfgpath_b, xdict)

        self.write_plugmod(plugdir, 'plugin_A')
        cmd = '%s status' % self.crawl_cmd()
        result = testhelp.rm_cov_warn(pexpect.run(cmd))
        self.expected(self.cstr['cdown'], result.strip())

        pre_l = glob.glob(self.pidglob)
        # start crawler A
        cmd = ('%s start --log %s --cfg %s'
               % (self.crawl_cmd(), logpath, cfgpath_a))
        result = testhelp.rm_cov_warn(pexpect.run(cmd))
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)

        self.assertTrue(crawl.is_running(context=ctx_a),
                        "Expected crawler %s to be running but it is not" %
                        ctx_a)
        a_up_l = glob.glob(self.pidglob)
        pidfile_a = (set(a_up_l) - set(pre_l)).pop()

        # start crawler B
        cmd = ('%s start --log %s --cfg %s'
               % (self.crawl_cmd(), logpath, cfgpath_b))
        result = testhelp.rm_cov_warn(pexpect.run(cmd))
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)

        b_up_l = glob.glob(self.pidglob)
        pidfile_b = (set(b_up_l) - set(a_up_l)).pop()

        self.assertTrue(crawl.is_running(context=ctx_b),
                        "Expected crawler %s to be running but it is not" %
                        ctx_b)
        self.assertPathPresent(pidfile_a)
        self.assertPathPresent(pidfile_b)

        cmd = '%s status' % self.crawl_cmd()
        result = testhelp.rm_cov_warn(pexpect.run(cmd))
        self.expected_in(self.cstr['crun'], result)
        self.expected_in('context=TEST', result)
        self.expected_in('context=DEV', result)

        cmd = '%s stop --log %s' % (self.crawl_cmd(), logpath)
        result = testhelp.rm_cov_warn(pexpect.run(cmd))
        self.vassert_nin("Traceback", result)
        self.vassert_in("Please specify a context", result)

        cmd = '%s stop --log %s --context %s' % (self.crawl_cmd(),
                                                 logpath, ctx_a)
        result = testhelp.rm_cov_warn(pexpect.run(cmd))
        self.vassert_nin("Traceback", result)
        time.sleep(1.5)

        cmd = '%s stop --log %s --context %s' % (self.crawl_cmd(),
                                                 logpath, ctx_b)
        result = testhelp.rm_cov_warn(pexpect.run(cmd))
        self.vassert_nin("Traceback", result)

        crawl.stop_wait(cfg=cfg_a)
        crawl.stop_wait(cfg=cfg_b)
        self.assertFalse(crawl.is_running(context=ctx_a),
                         "Expected crawler %s to be down but it is running" %
                         ctx_a)
        self.assertFalse(crawl.is_running(context=ctx_b),
                         "Expected crawler %s to be down but it is running" %
                         ctx_b)

        cmd = '%s status' % self.crawl_cmd()
        result = testhelp.rm_cov_warn(pexpect.run(cmd))
        self.expected("The crawler is not running.", result.strip())

        self.assertPathNotPresent(pidfile_a)
        self.assertPathNotPresent(pidfile_b)

    # --------------------------------------------------------------------------
    def test_crawl_stop_none(self):
        """
        TEST: 'crawl stop' when no crawler is running should report that there
        is nothing to do.
        """
        (cfgpath, logpath, exitpath, plugdir) = self.crawl_test_setup()

        cdict = copy.deepcopy(self.cfg_dict())
        cdict['crawler']['context'] = self.ctx
        self.write_cfg_file(cfgpath, cdict)
        self.write_plugmod(plugdir, 'plugin_A')

        cmd = '%s stop --log %s --context %s' % (self.crawl_cmd(),
                                                 logpath, self.ctx)
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_in("No crawlers are running -- nothing to stop",
                        result)

        self.assertFalse(crawl.is_running(context=self.ctx))

    # --------------------------------------------------------------------------
    def test_get_timeval(self):
        """
        TEST: various calls to CrawlConfig.get_time()

        EXP: correct number of seconds returned
        """
        with util.tmpenv('CRAWL_LOG', self.tmpdir('test_get_timeval.log')):
            t = CrawlConfig.CrawlConfig.dictor(self.cfg_dict())
            result = t.get_time('plugin_A', 'frequency', 1900)
            self.expected(int, type(result))
            self.expected(3600, result)

            t.set('plugin_A', 'frequency', '5')
            result = t.get_time('plugin_A', 'frequency', 1900)
            self.expected(5, result)

            t.set('plugin_A', 'frequency', '5min')
            result = t.get_time('plugin_A', 'frequency', 1900)
            self.expected(300, result)

            t.set('plugin_A', 'frequency', '3 days')
            result = t.get_time('plugin_A', 'frequency', 1900)
            self.expected(3 * 24 * 3600, result)

            t.set('plugin_A', 'frequency', '2     w')
            result = t.get_time('plugin_A', 'frequency', 1900)
            self.expected(2 * 7 * 24 * 3600, result)

            t.set('plugin_A', 'frequency', '4 months')
            result = t.get_time('plugin_A', 'frequency', 1900)
            self.expected(4 * 30 * 24 * 3600, result)

            t.set('plugin_A', 'frequency', '8 y')
            result = t.get_time('plugin_A', 'frequency', 1900)
            self.expected(8 * 365 * 24 * 3600, result)

    # --------------------------------------------------------------------------
    def test_make_pidfile_ctx(self):
        """
        Preconditions:
         - /tmp/crawler does exist and contains a pid file with known context
        Postconditions to be checked:
         - make_pidfile() throws an exception with message
           "The pidfile for context XXX exists"
        """
        # make sure /tmp/crawler exists and is empty
        util.conditional_rm(self.piddir, tree=True)
        os.mkdir(self.piddir)
        pid = 6700
        exitpath = "%s/%s.exit" % (self.piddir, util.my_name())
        with open("%s/%d" % (self.piddir, pid), 'w') as f:
            f.write("%s %s\n" % (self.ctx, exitpath))

        # run the target routine
        try:
            crawl.make_pidfile(pid, self.ctx, exitpath)
            self.fail("Expected exception was not thrown")
        except StandardError as e:
            exp = "The pidfile for context TEST exists"
            self.assertTrue(exp in str(e),
                            "Expected '%s', got '%s'" % (exp, str(e)))

    # --------------------------------------------------------------------------
    def test_make_pidfile_mtdir(self):
        """
        Preconditions:
         - /tmp/crawler does exist and is empty
        Postconditions to be checked:
         - /tmp/crawler exists
         - /tmp/crawler contains a pid file with the right file name
         - the pid file contains the right context and exitpath
        """
        # make sure /tmp/crawler exists and is empty
        util.conditional_rm(self.piddir, tree=True)
        os.mkdir(self.piddir)

        # run the target routine
        pid = 6700
        exitpath = "%s/%s.exit" % (self.piddir, util.my_name())
        crawl.make_pidfile(pid, self.ctx, exitpath)

        # verify the post conditions
        self.assertTrue(os.path.isdir(self.piddir),
                        "Directory %s should exist" % self.piddir)
        [pidfile] = glob.glob("%s/%d" % (self.piddir, pid))
        exp = "%s/6700" % self.piddir
        self.expected(exp, pidfile)
        (rctx, rxpath) = util.contents(pidfile).strip().split()
        self.expected(self.ctx, rctx)
        self.expected(exitpath, rxpath)

    # --------------------------------------------------------------------------
    def test_make_pidfile_nodir(self):
        """
        Preconditions:
         - /tmp/crawler does not exist
        Postconditions to be checked:
         - /tmp/crawler exists
         - /tmp/crawler contains a pid file with the right file name
         - the pid file contains the right context and exitpath
        """
        # make sure /tmp/crawler does not exist
        util.conditional_rm(self.piddir, tree=True)

        # run the target routine
        pid = 6700
        exitpath = "%s/%s.exit" % (self.piddir, util.my_name())
        crawl.make_pidfile(pid, self.ctx, exitpath)

        # verify the post conditions
        self.assertTrue(os.path.isdir(self.piddir),
                        "Directory %s should exist" % self.piddir)
        [pidfile] = glob.glob("%s/%d" % (self.piddir, pid))
        self.assertTrue(pidfile == ("%s/6700" % self.piddir),
                        "Pidfile '%s' does not look right" % pidfile)
        (rctx, rxpath) = util.contents(pidfile).strip().split()
        self.expected(self.ctx, rctx)
        self.expected(exitpath, rxpath)

    # --------------------------------------------------------------------------
    def test_map_time_unit(self):
        """
        TEST: return value from map_time_unit should reflect the number of
              seconds in the indicated unit or 1 if unit not known

        EXP: expected return values encoded in umap
        """
        with util.tmpenv('CRAWL_LOG', self.tmpdir('test_map_time_unit.log')):
            umap = {'s': 1, 'sec': 1, 'second': 1, 'seconds': 1,
                    'm': 60, 'min': 60, 'minute': 60, 'minutes': 60,
                    'h': 3600, 'hr': 3600, 'hour': 3600, 'hours': 3600,
                    'd': 24 * 3600, 'day': 24 * 3600, 'days': 24 * 3600,
                    'w': 7 * 24 * 3600, 'week': 7 * 24 * 3600,
                    'weeks': 7 * 24 * 3600,
                    'month': 30 * 24 * 3600, 'months': 30 * 24 * 3600,
                    'y': 365 * 24 * 3600, 'year': 365 * 24 * 3600,
                    'years': 365 * 24 * 3600,
                    }
            cfg = CrawlConfig.CrawlConfig()
            for unit in umap.keys():
                result = cfg.map_time_unit(unit)
                self.expected(umap[unit], result)

                unit += '_x'
                result = cfg.map_time_unit(unit)
                self.expected(1, result)

    # --------------------------------------------------------------------------
    def test_running_pid_mtdir(self):
        """
        Preconditions:
         - /tmp/crawler exists but is empty
        Postconditions:
         - running_pid() returns an empty list
        """
        # make sure /tmp/crawler is empty
        util.conditional_rm(self.piddir, tree=True)
        os.mkdir(self.piddir)

        # run the target routine
        result = crawl.running_pid()

        # verify postconditions
        self.expected([], result)

    # --------------------------------------------------------------------------
    def test_running_pid_nodir(self):
        """
        Preconditions:
         - /tmp/crawler does not exist
        Postconditions:
         - running_pid() returns an empty list
        """
        # make sure /tmp/crawler does not exist
        util.conditional_rm(self.piddir, tree=True)

        # run the target routine
        result = crawl.running_pid()

        # verify postconditions
        self.expected([], result)

    # --------------------------------------------------------------------------
    def test_running_pid_noproc1(self):
        """
        Preconditions:
         - /tmp/crawler contains a two pid files, procs not running (use
           make_pidfile to set up pidfiles)
         - running_pid() called with proc_required=False
        Postconditions:
         - running_pid() returns list containing two tuples with pid,
           context, exitpath
        """
        testdata = [(6700, 'TEST', '%s/first.exit' % self.piddir),
                    (6701, 'DEV',  '%s/other.exit' % self.piddir)]
        # set up two pidfiles
        util.conditional_rm(self.piddir, tree=True)
        for tup in testdata:
            crawl.make_pidfile(*tup)

        # run the target routine
        result = crawl.running_pid(proc_required=False)

        # verify postconditions
        exp = testdata
        self.expected(exp, result)

    # --------------------------------------------------------------------------
    def test_running_pid_proc1(self):
        """
        Preconditions:
         - /tmp/crawler contains a single pid file, pid not running (use
           make_pidfile to set up pidfile)
         - running_pid() called with proc_required=True
        Postconditions:
         - running_pid() returns an empty list
        """
        # set up single pidfile
        util.conditional_rm(self.piddir, tree=True)
        pid = 6700
        context = 'TEST'
        exitpath = "%s/%s.exit" % (self.piddir, util.my_name())
        crawl.make_pidfile(pid, context, exitpath)

        # run the target routine
        result = crawl.running_pid()

        # verify postconditions
        self.expected([], result)

    # --------------------------------------------------------------------------
    def test_running_pid_proc2(self):
        """
        Preconditions:
         - /tmp/crawler contains a two pid files, procs not running (use
           make_pidfile to set up pidfiles)
         - running_pid() called with proc_required=True
        Postconditions:
         - running_pid() returns an empty list
        """
        testdata = [(6700, 'TEST', '%s/first.exit' % self.piddir),
                    (6701, 'DEV',  '%s/other.exit' % self.piddir)]
        # set up two pidfiles
        util.conditional_rm(self.piddir, tree=True)
        for tup in testdata:
            crawl.make_pidfile(*tup)

        # run the target routine
        result = crawl.running_pid()

        # verify postconditions
        exp = []
        self.expected(exp, result)

    # --------------------------------------------------------------------------
    @util.memoize
    def crawl_cmd(self):
        if pexpect.which("crawl"):
            return "crawl"
        else:
            raise util.HpssicError("'crawl' is not in $PATH")

    # --------------------------------------------------------------------------
    def vassert_in(self, expected, actual):
        """
        If expected does not occur in actual, report it as an error.
        """
        if expected not in actual:
            self.fail('\n"""\n%s\n"""\n\n   NOT FOUND IN\n\n"""\n%s\n"""' %
                      (expected, actual))

    # ------------------------------------------------------------------------
    def vassert_nin(self, expected, actual):
        """
        If expected occurs in actual, report it as an error.
        """
        if expected in actual:
            self.fail('\n"""\n%s\n"""\n\n   SHOULD NOT BE IN\n\n"""\n%s\n"""' %
                      (expected, actual))

    # ------------------------------------------------------------------------
    def write_plugmod(self, plugdir, plugname):
        """
        Create a plugin module to test firing
        """
        if not os.path.exists(plugdir):
            os.makedirs(plugdir)

        if plugname.endswith('.py'):
            plugname = re.sub(r'\.py$', '', plugname)

        plugfname = plugname + '.py'

        f = open('%s/%s' % (plugdir, plugfname), 'w')
        f.write("#!/bin/env python\n")
        f.write("def main(cfg):\n")
        f.write("    q = open('%s/fired', 'w')\n" % self.tmpdir())
        f.write(r"    q.write('plugin %s fired\n')" % plugname)
        f.write("\n")
        f.write("    q.close()\n")
        f.close()

    # ------------------------------------------------------------------------
    def tearDown(self):
        """
        Clean up to do after each test
        """
        if crawl.is_running(context='TEST'):
            rpl = crawl.running_pid()
            for c in rpl:
                util.touch(c[2])
            time.sleep(1.0)

        if crawl.is_running(context='TEST'):
            result = pexpect.run("ps -ef")
            for line in result.split("\n"):
                if 'crawl start' in line:
                    pid = line.split()[1]

        util.conditional_rm(self.piddir, tree=True)


# ------------------------------------------------------------------------------
class CrawlGiveUpYetTest(CrawlTest):
    # --------------------------------------------------------------------------
    def setUp(self):
        fakesmtp.inbox = []
        self.email_targets = 'tbarron@ornl.gov, tusculum@gmail.com'
        t = CrawlConfig.CrawlConfig.dictor(self.cfg_dict())
        t.set('crawler', 'xlim_time', "2.0")
        t.set('crawler', 'xlim_count', "7")
        t.set('crawler', 'xlim_ident', "3")
        t.set('crawler', 'xlim_total', "10")
        t.add_section('alerts')
        t.set('alerts', 'email', self.email_targets)

        logobj = CrawlConfig.log(logpath=self.logpath(), close=True)
        self.D = crawl.CrawlDaemon("fake_pidfile", logger=logobj)
        self.D.cfg = t
        self.sender = 'hpssic@%s' % util.hostname(long=True)
        self.tbstr = ["abc", 'def', 'ghi', 'jkl', 'mno', 'pqr', "xyz"]

    # --------------------------------------------------------------------------
    def test_give_up_yet_identical(self):
        """
        CrawlDaemon.give_up_yet() should return True if it gets a configured
        number of identical traceback strings. Otherwise, it should return
        False.
        """
        self.assertFalse(self.D.give_up_yet(self.tbstr[0]))
        self.assertFalse(self.D.give_up_yet(self.tbstr[1]))
        self.assertFalse(self.D.give_up_yet(self.tbstr[0]))
        self.assertFalse(self.D.give_up_yet(self.tbstr[1]))
        self.assertTrue(self.D.give_up_yet(self.tbstr[0]))
        self.assertTrue(self.D.give_up_yet(self.tbstr[1]))

        shutdown_msg = "shutting down because we got 3 identical errors"
        self.expected_in(shutdown_msg, util.contents(self.logpath()))
        self.expected_in("sent mail to ", util.contents(self.logpath()))

        for m in fakesmtp.inbox:
            self.expected(self.email_targets, ', '.join(m.to_address))
            self.expected(self.sender, m.from_address)
            self.expected_in("crawl: %s" % shutdown_msg, m.fullmessage)

    # --------------------------------------------------------------------------
    def test_give_up_yet_total(self):
        """
        CrawlDaemon.give_up_yet() should return True if it gets a configured
        number total number of non-identical traceback strings. Otherwise, it
        should return False.
        """
        self.D.cfg.set('crawler', 'xlim_total', "6")

        self.assertFalse(self.D.give_up_yet(self.tbstr[0]))
        self.assertFalse(self.D.give_up_yet(self.tbstr[1]))
        self.assertFalse(self.D.give_up_yet(self.tbstr[2]))
        self.assertFalse(self.D.give_up_yet(self.tbstr[3]))
        self.assertFalse(self.D.give_up_yet(self.tbstr[4]))
        self.assertTrue(self.D.give_up_yet(self.tbstr[5]))

        shutdown_msg = "shutting down because we got 6 total errors"
        self.expected_in(shutdown_msg, util.contents(self.logpath()))
        self.expected_in("sent mail to ", util.contents(self.logpath()))

        for m in fakesmtp.inbox:
            self.expected(self.email_targets, ', '.join(m.to_address))
            self.expected(self.sender, m.from_address)
            self.expected_in("crawl: %s" % shutdown_msg, m.fullmessage)

    # --------------------------------------------------------------------------
    @pytest.mark.skipif(pytest.config.getvalue("fast"),
                        reason="slow -- omit --fast to run this one")
    def test_give_up_yet_window(self):
        """
        CrawlDaemon.give_up_yet() should return True if it gets a configured
        number of traceback strings within a specified time window.
        """
        self.D.cfg.set('crawler', 'xlim_time', "2.0")
        self.D.cfg.set('crawler', 'xlim_count', "4")

        self.assertFalse(self.D.give_up_yet(self.tbstr[0]))
        time.sleep(0.7)
        self.assertFalse(self.D.give_up_yet(self.tbstr[1]))
        time.sleep(0.7)
        self.assertFalse(self.D.give_up_yet(self.tbstr[2]))
        time.sleep(0.7)
        self.assertFalse(self.D.give_up_yet(self.tbstr[3]))
        self.assertTrue(self.D.give_up_yet(self.tbstr[4]))

        shutdown_msg = "shutting down because we got 4 exceptions in "
        self.expected_in(shutdown_msg, util.contents(self.logpath()))
        self.expected_in("sent mail to ", util.contents(self.logpath()))

        for m in fakesmtp.inbox:
            self.expected(self.email_targets, ', '.join(m.to_address))
            self.expected(self.sender, m.from_address)
            self.expected_in("crawl: %s" % shutdown_msg, m.fullmessage)
