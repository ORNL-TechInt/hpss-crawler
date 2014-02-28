#!/usr/bin/env python
"""
Tests for code in crawl.py
"""
import CrawlConfig
import crawl
import copy
import glob
import os
import pexpect
import shutil
import sys
import testhelp
import time
import toolframe
import util

# ------------------------------------------------------------------------------
def setUpModule():
    """
    Setup for running the tests.
    """
    testhelp.module_test_setup(CrawlTest.testdir)

    if not os.path.islink('crawl'):
        os.symlink('crawl.py', 'crawl')
        
# ------------------------------------------------------------------------------
def tearDownModule():
    """
    Clean up after a sequence of tests.
    """
    testhelp.module_test_teardown(CrawlTest.testdir)

    if crawl.is_running():
        rpl = crawl.running_pid()
        for c in rpl:
            testhelp.touch(c[2])

# -----------------------------------------------------------------------------
class CrawlTest(testhelp.HelpedTestCase):
    """
    Tests for the code in crawl.py
    """
    testdir = testhelp.testdata(__name__)
    plugdir = '%s/plugins' % testdir
    piddir = "/tmp/crawler"
    pidglob = piddir + "/*"
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
    def test_crawl_cfgdump_log_nopath(self):
        """
        TEST: "crawl cfgdump -c <cfgpath> --to log"
        EXP: what is written to log matches what was written to
        cfgpath. output should go to log path named in cfg.
        """
        cfname = "%s/test_crawl_cfgdump_log_n.cfg" % self.testdir
        self.write_cfg_file(cfname, self.cdict)
        cmd = 'crawl cfgdump -c %s --to log' % cfname
        result = pexpect.run(cmd)
        # print(">>>\n%s\n<<<" % result)
        self.vassert_nin("Traceback", result)
        self.assertEqual(os.path.exists(self.default_logpath), True)
        lcontent = util.contents(self.default_logpath)
        for section in self.cdict.keys():
            self.vassert_in('[%s]' % section, lcontent)

            for item in self.cdict[section].keys():
                self.vassert_in('%s = %s' %
                                (item, self.cdict[section][item]), lcontent)

        self.vassert_nin('heartbeat', lcontent)
        self.vassert_nin('fire', lcontent)

    # --------------------------------------------------------------------------
    def test_crawl_cfgdump_log_path(self):
        """
        TEST: "crawl cfgdump -c <cfgpath> --to log --log <logpath>"
        EXP: what is written to log matches what was written to
        cfgpath. output should go to logpath specified on command
        line.
        """
        cfname = "%s/test_crawl_cfgdump_log_p.cfg" % self.testdir
        logpath = "%s/test_local.log" % self.testdir
        self.write_cfg_file(cfname, self.cdict)
        cmd = ('crawl cfgdump -c %s --to log --logpath %s'
               % (cfname, logpath))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.assertEqual(os.path.exists(logpath), True)
        lcontent = util.contents(logpath)
        # print(">>>\n%s\n<<<" % result)
        for section in self.cdict.keys():
            self.vassert_in('[%s]' % section, lcontent)

            for item in self.cdict[section].keys():
                self.vassert_in('%s = %s' %
                                (item, self.cdict[section][item]), lcontent)
        
        self.vassert_nin('heartbeat', lcontent)
        self.vassert_nin('fire', lcontent)

    # --------------------------------------------------------------------------
    def test_crawl_cfgdump_nosuch(self):
        """
        TEST: "crawl cfgdump -c test.d/nosuch.cfg"
        EXP: attempting to open a nonexistent config file throws an error
        """
        cfname = '%s/nosuch.cfg' % self.testdir
        util.conditional_rm(cfname)
        cmd = 'crawl cfgdump -c %s' % cfname
        result = pexpect.run(cmd)
        self.vassert_in("Traceback", result)
        self.vassert_in("%s does not exist" % cfname,
                        result)

    # --------------------------------------------------------------------------
    def test_crawl_cfgdump_read(self):
        """
        TEST: "crawl cfgdump -c test.d/unreadable.cfg"
        EXP: attempting to open an unreadable config file throws an error
        """
        cfname = '%s/unreadable.cfg' % self.testdir
        open(cfname, 'w').close()
        os.chmod(cfname, 0000)

        cmd = 'crawl cfgdump -c %s' % cfname
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
        cfname = "%s/test_crawl_cfgdump_stdout.cfg" % self.testdir
        self.write_cfg_file(cfname, self.cdict)
        cmd = 'crawl cfgdump -c %s --to stdout' % cfname
        result = pexpect.run(cmd)
        # print(">>>\n%s\n<<<" % result)
        for section in self.cdict.keys():
            self.vassert_in('[%s]' % section, result)

            for item in self.cdict[section].keys():
                self.vassert_in('%s = %s' %
                                (item, self.cdict[section][item]), result)
        
    # --------------------------------------------------------------------------
    def test_crawl_fire_log_path(self):
        """
        TEST: crawl fire --plugin <plugmod>
        EXP: plugin fired and output went to specified log path
        """
        cfname = "%s/test_crawl_fire_log.cfg" % self.testdir
        lfname = "%s/test_crawl_fire.log" % self.testdir
        # plugdir = '%s/plugins' % self.testdir
        plugname = 'plugin_1'
        
        # create a plug module
        self.write_plugmod(self.plugdir, plugname)
        
        # add the plug module to the config
        t = CrawlConfig.CrawlConfig()
        t.load_dict(self.cdict)
        t.add_section(plugname)
        t.set(plugname, 'frequency', '1m')
        f = open(cfname, 'w')
        t.crawl_write(f)
        f.close()
        
        # carry out the test
        cmd = ('crawl fire -c %s --plugin %s --logpath %s' %
               (cfname, plugname, lfname))
        result = pexpect.run(cmd)

        # verify that command ran successfully
        self.vassert_nin("Traceback", result)
        
        # test.d/plugins/plugin_1.py should exist
        if not plugname.endswith('.py'):
            plugname += '.py'
        self.assertEqual(os.path.exists('%s/%s' % (self.plugdir, plugname)), True)
        
        # test.d/fired should exist and contain 'plugin plugin_1 fired'
        filename = '%s/fired' % self.testdir
        self.assertEqual(os.path.exists(filename), True)
        self.vassert_in('plugin plugin_1 fired', util.contents(filename))
        
        # lfname should exist and contain specific strings
        self.assertEqual(os.path.exists(lfname), True)
        self.vassert_in('firing plugin_1', util.contents(lfname))
    
    # --------------------------------------------------------------------------
    def test_crawl_log(self):
        """
        TEST: "crawl log --log filename message" will write message to filename
        """
        lfname = '%s/test_crawl.log' % self.testdir
        msg = "this is a test log message"
        cmd = "crawl log --log %s %s" % (lfname, msg)
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_in(msg, util.contents(lfname))
        
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
        ctx = "TEST"
        exitpath = "%s/%s.exit" % (self.piddir, util.my_name())
        crawl.make_pidfile(pid, ctx, exitpath)

        # verify the post conditions
        self.assertTrue(os.path.isdir(self.piddir),
                        "Directory %s should exist" % self.piddir)
        [pidfile] = glob.glob("%s/%d" % (self.piddir, pid))
        self.assertTrue(pidfile == ("%s/6700" % self.piddir),
                        "Pidfile '%s' does not look right" % pidfile)
        (rctx, rxpath) = util.contents(pidfile).strip().split()
        self.assertEqual(rctx, ctx,
                         "'%s' should match '%s'" % (rctx, ctx))
        self.assertEqual(rxpath, exitpath,
                         "'%s' should match '%s'" % (rxpath, exitpath))

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
        ctx = "TEST"
        exitpath = "%s/%s.exit" % (self.piddir, util.my_name())
        crawl.make_pidfile(pid, ctx, exitpath)

        # verify the post conditions
        self.assertTrue(os.path.isdir(self.piddir),
                        "Directory %s should exist" % self.piddir)
        [pidfile] = glob.glob("%s/%d" % (self.piddir, pid))
        exp = "%s/6700" % self.piddir
        self.assertEqual(pidfile, exp,
                        "Pidfile: '%s' should match '%s'" % (pidfile, exp))
        (rctx, rxpath) = util.contents(pidfile).strip().split()
        self.assertEqual(rctx, ctx,
                         "context: '%s' should match '%s'" % (rctx, ctx))
        self.assertEqual(rxpath, exitpath,
                         "exitpath: '%s' should match '%s'" %
                         (rxpath, exitpath))


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
        ctx = "TEST"
        exitpath = "%s/%s.exit" % (self.piddir, util.my_name())
        with open("%s/%d" % (self.piddir, pid), 'w') as f:
            f.write("%s %s\n" % (ctx, exitpath))
            
        # run the target routine
        try:
            crawl.make_pidfile(pid, ctx, exitpath)
            self.fail("Expected exception was not thrown")
        except StandardError, e:
            exp = "The pidfile for context TEST exists"
            self.assertTrue(exp in str(e),
                            "Expected '%s', got '%s'" % (exp, str(e)))

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
        self.assertEqual(result, [],
                         "Expected [], got '%s'" % result)
        
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
        self.assertEqual(result, [],
                         "Expected [], got '%s'" % result)
        
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
        self.assertEqual(result, [],
                         "Expected [], got '%s'" % result)
        
    # --------------------------------------------------------------------------
    def test_running_pid_noproc1(self):
        """
        Preconditions:
         - /tmp/crawler contains a single pid file, proc not running (use
           make_pidfile to set up pidfile)
         - running_pid() called with proc_required=False
        Postconditions:
         - running_pid() returns list containing single tuple with pid,
           context, exitpath
        """
        # set up single pidfile
        util.conditional_rm(self.piddir, tree=True)
        pid = 6700
        context = 'TEST'
        exitpath = "%s/%s.exit" % (self.piddir, util.my_name())
        crawl.make_pidfile(pid, context, exitpath)

        # run the target routine
        result = crawl.running_pid(proc_required=False)
        
        # verify postconditions
        exp = [(6700, context, exitpath)]
        self.assertEqual(result, exp,
                         "Expected '%s', got '%s'" % (exp, result))
        
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
        self.assertEqual(result, exp,
                         "Expected '%s', got '%s'" % (exp, result))
        
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
        self.assertEqual(result, exp,
                         "Expected '%s', got '%s'" % (exp, result))
        
    # --------------------------------------------------------------------------
    def test_crawl_start_x(self):
        """
        TEST: 'crawl start' should fire up a daemon crawler which will exit
        when the exit file is touched. Verify that crawler_pid exists
        while crawler is running and that it is removed when it stops.
        """
        cfgpath = '%s/test_start.cfg' % self.testdir
        logpath = '%s/test_start.log' % self.testdir
        exitpath = "%s/%s.exit" % (self.testdir, util.my_name())
        ctx = "TEST"
        xdict = copy.deepcopy(self.cdict)
        xdict['crawler']['exitpath'] = exitpath
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(self.plugdir, 'plugin_A')
        pre_l = glob.glob(self.pidglob)
        cmd = ('crawl start --log %s --cfg %s --context %s' %
               (logpath, cfgpath, ctx))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'] + ctx, result)
        
        self.assertEqual(crawl.is_running(context=ctx), True,
                         "Expected crawler to still be running but it is not")
        up_l = glob.glob(self.pidglob)
        self.assertEqual(len(pre_l) + 1, len(up_l),
                         "Expected a new pid file in %s" % self.piddir)
        self.assertEqual(os.path.exists(logpath), True)
        self.assertEqual(self.cstr['ldaemon'] in util.contents(logpath), True)

        testhelp.touch(exitpath)

        time.sleep(2)
        self.assertEqual(crawl.is_running(context=ctx), False)
        down_l = glob.glob(self.pidglob)
        self.assertEqual(len(pre_l), len(down_l),
                         "Expected pid file to be removed")
                
    # --------------------------------------------------------------------------
    def test_crawl_start_cfgctx(self):
        """
        TEST: 'crawl start' should fire up a daemon crawler which will exit
        when the exit file is touched. Verify that the crawler is started with
        the context specified in the config file.
        """
        cfgpath = '%s/test_start.cfg' % self.testdir
        logpath = '%s/test_start.log' % self.testdir
        exitpath = "%s/%s.exit" % (self.testdir, util.my_name())
        ctx = 'TEST'
        xdict = copy.deepcopy(self.cdict)
        xdict['crawler']['context'] = ctx
        xdict['crawler']['exitpath'] = exitpath
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(self.plugdir, 'plugin_A')
        pre_l = glob.glob(self.pidglob)
        cmd = ('crawl start --log %s --cfg %s'
               % (logpath, cfgpath))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'] + ctx, result)

        self.assertEqual(crawl.is_running(context=ctx), True,
                         "Expected crawler to still be running but it is not")
        up_l = glob.glob(self.pidglob)
        pidfile = (set(up_l) - set(pre_l)).pop()
        x = util.contents(pidfile).strip().split()
        self.assertEqual(ctx, x[0],
                         "Expected context to be '%s' but it is '%s'" %
                         (ctx, x[0]))
        self.assertEqual(exitpath, x[1],
                         "Expected exitpath to be '%s' but it is '%s'" %
                         (exitpath, x[1]))
        self.assertEqual(os.path.exists(logpath), True)
        self.assertEqual(self.cstr['ldaemon'] in util.contents(logpath), True)

        testhelp.touch(exitpath)

        time.sleep(2)
        self.assertEqual(crawl.is_running(context=ctx), False)
        self.assertEqual(os.path.exists(pidfile), False)
                
    # --------------------------------------------------------------------------
    def test_crawl_start_cmdctx(self):
        """
        TEST: 'crawl start' should fire up a daemon crawler which will exit
        when the exit file is touched. Verify that the crawler is started with
        the context provided on the command line.
        """
        cfgpath = '%s/test_start.cfg' % self.testdir
        logpath = '%s/test_start.log' % self.testdir
        exitpath = "%s/%s.exit" % (self.testdir, util.my_name())
        ctx = 'TEST'
        xdict = copy.deepcopy(self.cdict)
        xdict['crawler']['exitpath'] = exitpath
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(self.plugdir, 'plugin_A')
        pre_l = glob.glob(self.pidglob)
        cmd = ('crawl start --log %s --cfg %s --context %s' % 
               (logpath, cfgpath, ctx))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'] + ctx, result)

        self.assertEqual(crawl.is_running(context=ctx), True,
                         "Expected crawler to still be running but it is not")
        up_l = glob.glob(self.pidglob)
        pidfile = (set(up_l) - set(pre_l)).pop()
        x = util.contents(pidfile).strip().split()
        self.assertEqual(ctx, x[0],
                         "Expected context to be '%s' but it is '%s'" %
                         (ctx, x[0]))
        self.assertEqual(os.path.exists(logpath), True)
        self.assertEqual(self.cstr['ldaemon'] in util.contents(logpath), True)

        testhelp.touch(exitpath)

        time.sleep(2)
        self.assertEqual(crawl.is_running(context=ctx), False)
        self.assertEqual(os.path.exists(pidfile), False)
                
    # --------------------------------------------------------------------------
    def test_crawl_start_defctx(self):
        """
        TEST: 'crawl start' should fire up a daemon crawler which will exit
        when the exit file is touched. Verify that the crawler is started with
        the default context, which should be 'PROD'.
        """
        cfgpath = '%s/test_start.cfg' % self.testdir
        logpath = '%s/test_start.log' % self.testdir
        exitpath = "%s/%s.exit" % (self.testdir, util.my_name())
        ctx = 'PROD'
        xdict = copy.deepcopy(self.cdict)
        xdict['crawler']['context'] = ctx
        xdict['crawler']['exitpath'] = exitpath
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(self.plugdir, 'plugin_A')
        pre_l = glob.glob(self.pidglob)
        cmd = ('crawl start --log %s --cfg %s'
               % (logpath, cfgpath))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)

        self.assertEqual(crawl.is_running(context=ctx), True,
                         "Expected crawler to still be running but it is not")
        up_l = glob.glob(self.pidglob)
        pidfile = (set(up_l) - set(pre_l)).pop()

        x = util.contents(pidfile).strip().split()
        self.assertEqual('PROD', x[0],
                         "Expected context to be 'PROD' but it is '%s'" % x[0])
        self.assertEqual(os.path.exists(logpath), True)
        self.assertEqual(self.cstr['ldaemon'] in util.contents(logpath), True)

        testhelp.touch(exitpath)

        time.sleep(2)
        self.assertEqual(crawl.is_running(context=ctx), False)
        self.assertEqual(os.path.exists(pidfile), False)
                
    # --------------------------------------------------------------------------
    def test_crawl_start_already(self):
        """
        TEST: If the crawler is already running, decline to run a second copy.
        """
        cfgpath = '%s/test_start.cfg' % self.testdir
        logpath = '%s/test_start.log' % self.testdir
        exitpath = "%s/%s.exit" % (self.testdir, util.my_name())
        ctx = 'TEST'
        xdict = copy.deepcopy(self.cdict)
        xdict['crawler']['exitpath'] = exitpath
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(self.plugdir, 'plugin_A')
        cmd = ('crawl start --log %s --cfg %s --context %s' %
               (logpath, cfgpath, ctx))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)

        self.assertEqual(crawl.is_running(context=ctx), True,
                         "Expected crawler to be running but it is not")

        result = pexpect.run(cmd)
        self.assertTrue(self.cstr['pfctx'] in result,
                        "Expected '%s' but didn't see it" % self.cstr['pfctx'])
        
        testhelp.touch(exitpath)

        time.sleep(2)
        self.assertEqual(crawl.is_running(context=ctx), False)
        
    # --------------------------------------------------------------------------
    def test_crawl_start_cfg(self):
        """
        TEST: 'crawl start' should fire up a daemon crawler which will exit
        when the exit file is touched. Verify that the correct config
        file is loaded.
        """
        cfgpath = '%s/test_stcfg.cfg' % self.testdir
        logpath = '%s/test_stcfg.log' % self.testdir
        exitpath = '%s/%s.exit' % (self.testdir, util.my_name())
        ctx = 'TEST'
        xdict = copy.deepcopy(self.cdict)
        xdict['crawler']['exitpath'] = exitpath
        xdict['crawler']['plugins'] = 'plugin_A, other_plugin'
        xdict['other_plugin'] = {'unplanned': 'silver',
                                 'simple': 'check for this'}
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(self.plugdir, 'plugin_A')
        self.write_plugmod(self.plugdir, 'other_plugin')
        pre_l = glob.glob(self.pidglob)
        cmd = ('crawl start --log %s --cfg %s --context %s'
               % (logpath, cfgpath, ctx))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)

        self.assertEqual(crawl.is_running(context=ctx), True,
                         "Expected crawler to be running but it is not")
        up_l = glob.glob(self.pidglob)
        pidfile = (set(up_l) - set(pre_l)).pop()

        self.assertEqual(os.path.exists(logpath), True)
        self.assertEqual('crawl: CONFIG: [other_plugin]' in util.contents(logpath),
                         True,
                         "Expected 'other_plugin' in log file not found")
        self.assertEqual('crawl: CONFIG: unplanned: silver' in
                         util.contents(logpath),
                         True,
                         "Expected 'unplanned: silver' in log file not found")
        self.assertEqual('crawl: CONFIG: simple: check for this' in
                         util.contents(logpath),
                         True,
                         "Expected 'simple: check for this' " +
                         "in log file not found")
        
        testhelp.touch(exitpath)

        time.sleep(2)
        self.assertEqual(crawl.is_running(context=ctx), False)
        self.assertEqual(os.path.exists(pidfile), False)
                
    # --------------------------------------------------------------------------
    def test_crawl_start_fire(self):
        """
        TEST: 'crawl start' should fire up a daemon crawler which will exit
        when the exit file is touched. Verify that at least one plugin
        fires and produces some output.
        """
        cfgpath = '%s/test_fire.cfg' % self.testdir
        logpath = '%s/test_fire.log' % self.testdir
        exitpath = "%s/%s.exit" % (self.testdir, util.my_name())
        ctx = 'TEST'
        xdict = copy.deepcopy(self.cdict)
        xdict['crawler']['exitpath'] = exitpath
        xdict['other'] = {'frequency': '1s', 'fire': 'true'}
        xdict['crawler']['verbose'] = 'true'
        xdict['crawler']['plugins'] = 'other'
        del xdict['plugin_A']
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(self.plugdir, 'other')
        pre_l = glob.glob(self.pidglob)
        cmd = ('crawl start --log %s --cfg %s --context %s'
               % (logpath, cfgpath, ctx))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)

        self.assertEqual(crawl.is_running(context=ctx), True,
                         "Expected crawler to still be running but it isn't")
        up_l = glob.glob(self.pidglob)
        pidfile = (set(up_l) - set(pre_l)).pop()

        self.assertEqual(os.path.exists(logpath), True)
        self.assertEqual(self.cstr['ldaemon'] in util.contents(logpath), True)
        time.sleep(2)
        self.assertEqual('other: firing' in util.contents(logpath), True,
                         "Log file does not indicate plugin was fired")
        self.assertEqual(os.path.exists('%s/fired' % self.testdir), True,
                         "File %s/fired does not exist" % self.testdir)
        self.assertEqual('plugin other fired\n',
                         util.contents('%s/fired' % self.testdir),
                         "Contents of %s/fired is not right" % self.testdir)
        
        testhelp.touch(exitpath)

        time.sleep(2)
        self.assertEqual(crawl.is_running(context=ctx), False)
        self.assertEqual(os.path.exists(pidfile), False)
                
    # --------------------------------------------------------------------------
    def test_crawl_start_noexit(self):
        """
        'crawl start' with no exit path in the config file should throw an
        exception.
        """
        cfgpath = '%s/%s.cfg' % (self.testdir, util.my_name())
        logpath = '%s/%s.log' % (self.testdir, util.my_name())
        ctx = 'TEST'

        self.write_cfg_file(cfgpath, self.cdict)
        self.write_plugmod(self.plugdir, 'plugin_A')

        cmd = ('crawl start --log %s --cfg %s --context %s' %
               (logpath, cfgpath, ctx))
        result = pexpect.run(cmd)

        self.vassert_in("No exit path is specified in the configuration",
                        result)

        self.assertEqual(crawl.is_running(), False,
                         "Crawler should not have started")

    # --------------------------------------------------------------------------
    def test_crawl_start_nonplugin_sections(self):
        """
        TEST: 'crawl start' should fire up a daemon crawler which will exit
        when the exit file is touched. Verify that a config file with
        non-plugin sections works properly. Sections that are not listed with
        the 'plugin' option in the 'crawler' section should not be loaded as
        plugins.
        """
        cfgpath = '%s/test_nonplugin.cfg' % self.testdir
        logpath = '%s/test_nonplugin.log' % self.testdir
        exitpath = "%s/%s.exit" % (self.testdir, util.my_name())
        ctx = 'TEST'
        xdict = copy.deepcopy(self.cdict)
        xdict['alerts'] = {}
        xdict['alerts']['email'] = 'one@somewhere.com, two@elsewhere.org'
        xdict['alerts']['log'] = '!!!ALERT!!! %s'
        xdict['crawler']['exitpath'] = exitpath
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(self.plugdir, 'plugin_A')

        pre_l = glob.glob(self.pidglob)
        cmd = ('crawl start --log %s --cfg %s --context %s' %
               (logpath, cfgpath, ctx))
        result = pexpect.run(cmd)

        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)

        up_l = glob.glob(self.pidglob)
        pidfile = (set(up_l) - set(pre_l)).pop()

        testhelp.touch(exitpath)
        time.sleep(2)
        self.vassert_nin("Traceback", util.contents(logpath))
        self.assertEqual(crawl.is_running(), False,
                         "crawler is still running unexpectedly")
        self.assertEqual(os.path.exists(pidfile), False,
                         "%s is hanging around after it should be gone" %
                         pidfile)

    # --------------------------------------------------------------------------
    def test_crawl_status(self):
        """
        TEST: 'crawl status' should report the crawler status correctly.
        """
        logpath = '%s/%s.log' % (self.testdir, util.my_name())
        cfgpath = '%s/%s.cfg' % (self.testdir, util.my_name())
        exitpath = "%s/%s.exit" % (self.testdir, util.my_name())
        ctx = 'TEST'
        xdict = copy.deepcopy(self.cdict)
        xdict['crawler']['context'] = ctx
        xdict['crawler']['exitpath'] = exitpath
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(self.plugdir, 'plugin_A')
        cmd = 'crawl status'
        result = pexpect.run(cmd)
        self.assertEqual(result.strip(), self.cstr['cdown'])

        pre_l = glob.glob(self.pidglob)
        cmd = ('crawl start --log %s --cfg %s'
               % (logpath, cfgpath))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)

        self.assertEqual(crawl.is_running(context=ctx), True,
                         "Expected crawler to be running but it is not")
        up_l = glob.glob(self.pidglob)
        pidfile = (set(up_l) - set(pre_l)).pop()
        self.assertEqual(os.path.exists(pidfile), True)

        cmd = 'crawl status'
        result = pexpect.run(cmd)
        self.assertEqual(self.cstr['crun'] in result, True)
        self.assertEqual('context=%s' % ctx in result, True)

        cmd = 'crawl stop --log %s --context %s' % (logpath, ctx)
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        time.sleep(1.5)
        
        self.assertEqual(crawl.is_running(context=ctx), False)
        self.assertEqual(os.path.exists(pidfile), False)

        cmd = 'crawl status'
        result = pexpect.run(cmd)
        self.assertEqual(result.strip(), self.cstr['cdown'])

    # --------------------------------------------------------------------------
    def test_crawl_status_multi(self):
        """
        TEST: 'crawl status' should report the crawler status correctly.
        """
        logpath = '%s/%s.log' % (self.testdir, util.my_name())
        cfgpath_a = '%s/%s_a.cfg' % (self.testdir, util.my_name())
        exitpath_a = "%s/%s.exit_a" % (self.testdir, util.my_name())
        ctx_a = 'TEST'
        
        cfgpath_b = "%s/%s_b.cfg" % (self.testdir, util.my_name())
        exitpath_b = "%s/%s.exit_b" % (self.testdir, util.my_name())
        ctx_b = 'DEV'
        
        xdict = copy.deepcopy(self.cdict)
        xdict['crawler']['context'] = ctx_a
        xdict['crawler']['exitpath'] = exitpath_a
        self.write_cfg_file(cfgpath_a, xdict)

        xdict['crawler']['context'] = ctx_b
        xdict['crawler']['exitpath'] = exitpath_b
        self.write_cfg_file(cfgpath_b, xdict)

        self.write_plugmod(self.plugdir, 'plugin_A')
        cmd = 'crawl status'
        result = pexpect.run(cmd)
        self.assertEqual(result.strip(), self.cstr['cdown'])

        pre_l = glob.glob(self.pidglob)
        # start crawler A
        cmd = ('crawl start --log %s --cfg %s'
               % (logpath, cfgpath_a))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)
        a_up_l = glob.glob(self.pidglob)

        # start crawler B
        cmd = ('crawl start --log %s --cfg %s'
               % (logpath, cfgpath_b))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)
        b_up_l = glob.glob(self.pidglob)

        pidfile_a = (set(a_up_l) - set(pre_l)).pop()
        pidfile_b = (set(b_up_l) - set(a_up_l)).pop()
        
        self.assertEqual(crawl.is_running(context=ctx_a), True,
                         "Expected crawler %s to be running but it is not" %
                         ctx_a)
        self.assertEqual(crawl.is_running(context=ctx_b), True,
                         "Expected crawler %s to be running but it is not" %
                         ctx_b)
        self.assertEqual(os.path.exists(pidfile_a), True)
        self.assertEqual(os.path.exists(pidfile_b), True)

        cmd = 'crawl status'
        result = pexpect.run(cmd)
        self.assertEqual(self.cstr['crun'] in result,
                         True)
        self.assertEqual('context=%s' % ctx_a in result, True)
        self.assertEqual('context=%s' % ctx_b in result, True)

        cmd = 'crawl stop --log %s --context %s' % (logpath, ctx_a)
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        time.sleep(1.5)
        
        cmd = 'crawl stop --log %s --context %s' % (logpath, ctx_b)
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        time.sleep(1.5)
        
        self.assertEqual(crawl.is_running(context=ctx_a), False,
                         "Crawler %s should not be running but it is" % ctx_a)
        self.assertEqual(crawl.is_running(context=ctx_b), False,
                         "Crawler %s should not be running but it is" % ctx_b)
        self.assertEqual(os.path.exists(pidfile_a), False)
        self.assertEqual(os.path.exists(pidfile_b), False)

        cmd = 'crawl status'
        result = pexpect.run(cmd)
        self.assertEqual(result.strip(), self.cstr['cdown'])

    # --------------------------------------------------------------------------
    def test_crawl_stop_confirm(self):
        """
        TEST: 'crawl stop' should cause a running daemon to shut down. If no
        context is specified, the user should be asked to confirm the shutdown.
        """
        logpath = '%s/test_start.log' % self.testdir
        cfgpath = '%s/test_start.cfg' % self.testdir
        exitpath = '%s/%s.exit' % (self.testdir, util.my_name())
        ctx = 'TEST'
        xdict = copy.deepcopy(self.cdict)
        xdict['crawler']['context'] = ctx
        xdict['crawler']['exitpath'] = exitpath
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(self.plugdir, 'plugin_A')

        pre_l = glob.glob(self.pidglob)
        cmd = ('crawl start --log %s --cfg %s --context %s' %
               (logpath, cfgpath, ctx))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)

        self.assertEqual(crawl.is_running(context=ctx), True,
                         "Expected the crawler to be running but it is not")
        up_l = glob.glob(self.pidglob)
        pidfile = (set(up_l) - set(pre_l)).pop()

        cmd = 'crawl stop --log %s' % (logpath)
        S = pexpect.spawn(cmd)
        S.expect(self.cstr['prepstop'])
        S.sendline("no")
        S.expect(pexpect.EOF)
        self.vassert_in("No action taken", S.before)
        S.close()

        self.assertEqual(crawl.is_running(context=ctx), True)
        self.assertEqual(os.path.exists(pidfile), True)
        
        cmd = 'crawl stop --log %s' % (logpath)
        S = pexpect.spawn(cmd)
        S.expect(self.cstr['prepstop'])
        S.sendline("yes")
        S.expect(pexpect.EOF)
        self.vassert_in("Stopping the crawler...", S.before)
        S.close()

        time.sleep(2)
        
        self.assertEqual(crawl.is_running(context=ctx), False)
        self.assertEqual(os.path.exists(pidfile), False)

    # --------------------------------------------------------------------------
    def test_crawl_stop_ctx(self):
        """
        TEST: 'crawl stop' should cause a running daemon to shut down. If the
        correct context is specified, the crawler will be shutdown without
        prompting.
        """
        logpath = '%s/test_start.log' % self.testdir
        cfgpath = '%s/test_start.cfg' % self.testdir
        exitpath = '%s/%s.exit' % (self.testdir, util.my_name())
        ctx = 'TEST'
        xdict = copy.deepcopy(self.cdict)
        xdict['crawler']['context'] = ctx
        xdict['crawler']['exitpath'] = exitpath
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(self.plugdir, 'plugin_A')

        pre_l = glob.glob(self.pidglob)
        cmd = ('crawl start --log %s --cfg %s --context %s' %
               (logpath, cfgpath, ctx))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)

        self.assertEqual(crawl.is_running(context=ctx), True,
                         "Expected the crawler to be running but it is not")
        up_l = glob.glob(self.pidglob)
        pidfile = (set(up_l) - set(pre_l)).pop()

        cmd = 'crawl stop --log %s --context %s' % (logpath, ctx)
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_in("Stopping the TEST crawler", result)
        time.sleep(2)
        
        self.assertEqual(crawl.is_running(context=ctx), False)
        self.assertEqual(os.path.exists(pidfile), False)

    # --------------------------------------------------------------------------
    def test_crawl_stop_ctxoth(self):
        """
        TEST: 'crawl stop' with a context other than the running crawler will
        do nothing.
        """
        logpath = '%s/test_start.log' % self.testdir
        cfgpath = '%s/test_start.cfg' % self.testdir
        exitpath = '%s/%s.exit' % (self.testdir, util.my_name())
        ctx = 'TEST'
        xdict = copy.deepcopy(self.cdict)
        xdict['crawler']['context'] = ctx
        xdict['crawler']['exitpath'] = exitpath
        self.write_cfg_file(cfgpath, xdict)
        self.write_plugmod(self.plugdir, 'plugin_A')

        pre_l = glob.glob(self.pidglob)
        cmd = ('crawl start --log %s --cfg %s --context %s' %
               (logpath, cfgpath, ctx))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)

        self.assertEqual(crawl.is_running(context=ctx), True,
                         "Expected the crawler to be running but it is not")

        up_l = glob.glob(self.pidglob)
        pidfile = (set(up_l) - set(pre_l)).pop()

        cmd = 'crawl stop --log %s --context DEV' % (logpath)
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_in("No DEV crawler is running", result)

        self.assertEqual(crawl.is_running(context=ctx), True)

        cmd = 'crawl stop --log %s --context TEST' % (logpath)
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin("No DEV crawler is running", result)

        time.sleep(2)
        
        self.assertEqual(crawl.is_running(context=ctx), False)
        self.assertEqual(os.path.exists(pidfile), False)

    # --------------------------------------------------------------------------
    def test_crawl_stop_noctx(self):
        """
        TEST: 'crawl stop' with multiple crawlers running and no args should
        ask the user to specify a context.
        """
        logpath = '%s/%s.log' % (self.testdir, util.my_name())
        cfgpath_a = '%s/%s_a.cfg' % (self.testdir, util.my_name())
        exitpath_a = '%s/%s_a.exit' % (self.testdir, util.my_name())
        ctx_a = 'TEST'
        
        cfgpath_b = "%s/%s_b.cfg" % (self.testdir, util.my_name())
        exitpath_b = "%s/%s_b.exit" % (self.testdir, util.my_name())
        ctx_b = 'DEV'
        
        xdict = copy.deepcopy(self.cdict)
        xdict['crawler']['context'] = ctx_a
        xdict['crawler']['exitpath'] = exitpath_a
        self.write_cfg_file(cfgpath_a, xdict)

        xdict['crawler']['context'] = ctx_b
        xdict['crawler']['exitpath'] = exitpath_b
        self.write_cfg_file(cfgpath_b, xdict)

        self.write_plugmod(self.plugdir, 'plugin_A')
        cmd = 'crawl status'
        result = pexpect.run(cmd)
        self.assertEqual(result.strip(), self.cstr['cdown'])

        pre_l = glob.glob(self.pidglob)
        # start crawler A
        cmd = ('crawl start --log %s --cfg %s'
               % (logpath, cfgpath_a))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)

        self.assertEqual(crawl.is_running(context=ctx_a), True,
                         "Expected crawler %s to be running but it is not" %
                         ctx_a)
        a_up_l = glob.glob(self.pidglob)
        pidfile_a = (set(a_up_l) - set(pre_l)).pop()
        
        # start crawler B
        cmd = ('crawl start --log %s --cfg %s'
               % (logpath, cfgpath_b))
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_nin(self.cstr['pfctx'], result)

        b_up_l = glob.glob(self.pidglob)
        pidfile_b = (set(b_up_l) - set(a_up_l)).pop()
        
        self.assertEqual(crawl.is_running(context=ctx_b), True,
                         "Expected crawler %s to be running but it is not" %
                         ctx_b)
        self.assertEqual(os.path.exists(pidfile_a), True)
        self.assertEqual(os.path.exists(pidfile_b), True)

        cmd = 'crawl status'
        result = pexpect.run(cmd)
        self.assertEqual(self.cstr['crun'] in result,
                         True)
        self.assertEqual('context=TEST' in result, True)
        self.assertEqual('context=DEV' in result, True)

        cmd = 'crawl stop --log %s' % (logpath)
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_in("Please specify a context", result)
        
        cmd = 'crawl stop --log %s --context %s' % (logpath, ctx_a)
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        time.sleep(1.5)
        
        cmd = 'crawl stop --log %s --context %s' % (logpath, ctx_b)
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        time.sleep(1.5)
        
        self.assertEqual(crawl.is_running(context=ctx_a), False,
                         "Expected crawler %s to be down but it is running" %
                         ctx_a)
        self.assertEqual(crawl.is_running(context=ctx_b), False,
                         "Expected crawler %s to be down but it is running" %
                         ctx_b)

        cmd = 'crawl status'
        result = pexpect.run(cmd)
        self.assertEqual(result.strip(), "The crawler is not running.")

        self.assertEqual(os.path.exists(pidfile_a), False,
                         "%s should have been removed" % pidfile_a)
        self.assertEqual(os.path.exists(pidfile_b), False,
                         "%s should have been removed" % pidfile_b)

    # --------------------------------------------------------------------------
    def test_crawl_stop_none(self):
        """
        TEST: 'crawl stop' when no crawler is running should report that there
        is nothing to do.
        """
        logpath = '%s/test_start.log' % self.testdir
        cfgpath = '%s/test_start.cfg' % self.testdir
        self.cdict['crawler']['context'] = 'TEST'
        self.write_cfg_file(cfgpath, self.cdict)
        self.write_plugmod(self.plugdir, 'plugin_A')

        cmd = 'crawl stop --log %s --context TEST' % (logpath)
        result = pexpect.run(cmd)
        self.vassert_nin("Traceback", result)
        self.vassert_in("No crawlers are running -- nothing to stop", result)
        
        self.assertEqual(crawl.is_running(), False)

    # --------------------------------------------------------------------------
    def test_get_timeval(self):
        """
        TEST: various calls to CrawlConfig.get_time()

        EXP: correct number of seconds returned
        """
        os.environ['CRAWL_LOG'] = '%s/test_get_timeval.log' % self.testdir
        t = CrawlConfig.CrawlConfig()
        t.load_dict(self.cdict)
        result = t.get_time('plugin_A', 'frequency', 1900)
        self.assertEqual(type(result), int,
                         'type of CrawlConfig.get_time result should be %s but is %s (%s)'
                         % ('int', type(result), str(result)))
        self.assertEqual(result, 3600,
                         'CrawlConfig.get_time() got %s wrong: %d'
                         % (t.get('plugin_A', 'frequency'), result))

        t.set('plugin_A', 'frequency', '5')
        result = t.get_time('plugin_A', 'frequency', 1900)
        self.assertEqual(result, 5,
                         'CrawlConfig.get_time() got %s wrong: %d'
                         % (t.get('plugin_A', 'frequency'), result))

        t.set('plugin_A', 'frequency', '5min')
        result = t.get_time('plugin_A', 'frequency', 1900)
        self.assertEqual(result, 300,
                         'CrawlConfig.get_time() got %s wrong: %d'
                         % (t.get('plugin_A', 'frequency'), result))
        
        t.set('plugin_A', 'frequency', '3 days')
        result = t.get_time('plugin_A', 'frequency', 1900)
        self.assertEqual(result, 3 * 24 * 3600,
                         'CrawlConfig.get_time() got %s wrong: %d'
                         % (t.get('plugin_A', 'frequency'), result))
        
        t.set('plugin_A', 'frequency', '2     w')
        result = t.get_time('plugin_A', 'frequency', 1900)
        self.assertEqual(result, 2 * 7 * 24 * 3600,
                         'CrawlConfig.get_time() got %s wrong: %d'
                         % (t.get('plugin_A', 'frequency'), result))
        
        t.set('plugin_A', 'frequency', '4 months')
        result = t.get_time('plugin_A', 'frequency', 1900)
        self.assertEqual(result, 4 * 30 * 24 * 3600,
                         'CrawlConfig.get_time() got %s wrong: %d'
                         % (t.get('plugin_A', 'frequency'), result))
        
        t.set('plugin_A', 'frequency', '8 y')
        result = t.get_time('plugin_A', 'frequency', 1900)
        self.assertEqual(result, 8 * 365 * 24 * 3600,
                         'CrawlConfig.get_time() got %s wrong: %d'
                         % (t.get('plugin_A', 'frequency'), result))
        
        del os.environ['CRAWL_LOG']

    # --------------------------------------------------------------------------
    def test_map_time_unit(self):
        """
        TEST: return value from map_time_unit should reflect the number of
              seconds in the indicated unit or 1 if unit not known

        EXP: expected return values encoded in umap
        """
        os.environ['CRAWL_LOG'] = '%s/test_map_time_unit.log' % self.testdir
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
            self.assertEqual(result, umap[unit])

            unit += '_x'
            result = cfg.map_time_unit(unit)
            self.assertEqual(result, 1)
            
        del os.environ['CRAWL_LOG']

    # --------------------------------------------------------------------------
    def vassert_in(self, expected, actual):
        """
        If expected does not occur in actual, report it as an error.
        """
        if not expected in actual:
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
        f.write("    q = open('%s/fired', 'w')\n" % self.testdir)
        f.write(r"    q.write('plugin %s fired\n')" % plugname)
        f.write("\n")
        f.write("    q.close()\n")
        f.close()
        
    # ------------------------------------------------------------------------
    def tearDown(self):
        """
        Clean up to do after each test
        """
        if crawl.is_running():
            rpl = crawl.running_pid()
            for c in rpl:
                testhelp.touch(c[2])
            time.sleep(1.0)
            
        if crawl.is_running():
            result = pexpect.run("ps -ef")
            for line in result.split("\n"):
                if 'crawl start' in line:
                    pid = line.split()[1]
                    # print("pid = %s <- kill this" % pid)

        util.conditional_rm(self.piddir, tree=True)

# ------------------------------------------------------------------------------
toolframe.ez_launch(test='CrawlTest', logfile=testhelp.testlog(__name__))
