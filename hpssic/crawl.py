#!/usr/bin/env python
"""
Run a bunch of plug-ins which will probe the integrity of HPSS
"""
import atexit
import base64
import CrawlConfig
import CrawlDBI
import CrawlPlugin
import daemon
import getpass
import glob
import optparse
import os
import pdb
import pexpect
import shutil
import sys
import testhelp
import time
from hpssic import toolframe
import traceback as tb
import util

exit_file = 'crawler.exit'


# ------------------------------------------------------------------------------
def crl_cfgdump(argv):
    """cfgdump - load a config file and dump its contents

    usage: crawl cfgdump -c <filename> [--to stdout|log] [--logpath <path>]
    """
    p = optparse.OptionParser()
    p.add_option('-c', '--cfg',
                 action='store', default='', dest='config',
                 help='config file name')
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-t', '--to',
                 action='store', default='', dest='target',
                 help='specify where to send the output')
    p.add_option('-l', '--logpath',
                 action='store', default='', dest='logpath',
                 help='specify where to send the output')
    (o, a) = p.parse_args(argv)

    if o.debug:
        pdb.set_trace()

    if o.target == '':
        o.target = 'stdout'

    cfg = CrawlConfig.get_config(o.config)
    dumpstr = cfg.dump()

    if o.target == 'stdout':
        print dumpstr
    elif o.target == 'log':
        log = CrawlConfig.get_logger(o.logpath, cfg)
        for line in dumpstr.split("\n"):
            log.info(line)


# ------------------------------------------------------------------------------
def crl_cleanup(argv):
    """cleanup - remove any test directories left behind

    usage: crawl cleanup

    Looks for and removes /tmp/hpss-crawl.* recursively.
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-n', '--dryrun',
                 action='store_true', default=False, dest='dryrun',
                 help='see what would happen')
    (o, a) = p.parse_args(argv)

    if o.debug:
        pdb.set_trace()

    testdirs = glob.glob("/tmp/hpss-crawl.*")
    for td in testdirs:
        if o.dryrun:
            print("would do 'shutil.rmtree(%s)'" % td)
        else:
            shutil.rmtree(td)


# ------------------------------------------------------------------------------
def crl_dbdrop(argv):
    """dbdrop - drop a database table

    usage: crawl dbdrop [-f] <table-name>

    Drop database table <table-name>
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-f', '--force',
                 action='store_true', default=False, dest='force',
                 help='proceed without confirmation')
    (o, a) = p.parse_args(argv)

    if o.debug:
        pdb.set_trace()

    cfg = CrawlConfig.get_config()
    tbpfx = cfg.get('dbi', 'tbl_prefix')
    tname = a[0]
    answer = raw_input("About to drop db table %s_%s. Are you sure? > " %
                       (tbpfx, tname))
    if answer[0].lower() != "y":
        sys.exit()

    db = CrawlDBI.DBI()
    db.drop(table=tname)
    if db.table_exists(table=tname):
        print("Attempt to drop table '%s_%s' failed" % (tbpfx, tname))
    else:
        print("Attempt to drop table '%s_%s' was successful" % (tbpfx, tname))
    db.close()


# ------------------------------------------------------------------------------
def crl_fire(argv):
    """fire - run a plugin

    usage: crawl fire --cfg cfgname --logpath logfname --plugin plugname
    """
    p = optparse.OptionParser()
    p.add_option('-c', '--cfg',
                 action='store', default='', dest='config',
                 help='config file name')
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-l', '--logpath',
                 action='store', default='', dest='logpath',
                 help='specify where to send the output')
    p.add_option('-p', '--plugin',
                 action='store', default='', dest='plugname',
                 help='which plugin to fire')
    (o, a) = p.parse_args(argv)

    if o.debug:
        pdb.set_trace()

    cfg = CrawlConfig.get_config(o.config)
    log = CrawlConfig.get_logger(o.logpath, cfg)

    if not cfg.has_section(o.plugname):
        print("No plugin named '%s' is defined")
    else:
        plugdir = cfg.get('crawler', 'plugin-dir')
        sys.path.append(plugdir)
        __import__(o.plugname)
        log.info('firing %s' % o.plugname)
        sys.modules[o.plugname].main(cfg)


# ------------------------------------------------------------------------------
def crl_log(argv):
    """log - write a message to the indicated log file

    usage: crawl log --log <filename> <message>
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-l', '--log',
                 action='store', default=None, dest='logfile',
                 help='specify the log file')
    (o, a) = p.parse_args(argv)

    if o.debug:
        pdb.set_trace()

    if o.logfile is not None:
        log = CrawlConfig.get_logger(o.logfile)
    else:
        cfg = CrawlConfig.get_config()
        log = CrawlConfig.get_logger(cfg=cfg)

    log.info(" ".join(a))


# -----------------------------------------------------------------------------
def crl_pw_encode(argv):
    """pw_encode - accept a password and report its base64 encoding

    usage: crawl pw_encode [-p password]

    If -p is not used, a prompt will be issued to retrieve the password without
    echo.
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-p', '--pwd',
                 action='store', default='', dest='pwd',
                 help='password to encode')
    (o, a) = p.parse_args(argv)

    if o.debug:
        pdb.set_trace()

    if o.pwd != '':
        password = o.pwd
    else:
        password = getpass.getpass("Password? > ")

    print(base64.b64encode(password))


# -----------------------------------------------------------------------------
def crl_pw_decode(argv):
    """pw_decode - accept a base64 hash and decode it

    usage: crawl pw_decode hash

    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(argv)

    if o.debug:
        pdb.set_trace()

    if len(a) < 1:
        print("usage: crawl pw_decode B64STRING")
        sys.exit(1)

    print(base64.b64decode(a[0]))


# -----------------------------------------------------------------------------
def crl_pw_encode(argv):
    """pw_encode - accept a password and report its base64 encoding

    usage: crawl pw_encode [-p password]

    If -p is not used, a prompt will be issued to retrieve the password without
    echo.
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-p', '--pwd',
                 action='store', default='', dest='pwd',
                 help='password to encode')
    (o, a) = p.parse_args(argv)

    if o.debug:
        pdb.set_trace()

    if o.pwd != '':
        password = o.pwd
    else:
        password = getpass.getpass("Password? > ")

    print(base64.b64encode(password))


# ------------------------------------------------------------------------------
def crl_pw_decode(argv):
    """pw_decode - accept a base64 hash and decode it

    usage: crawl pw_decode hash

    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(argv)

    if o.debug:
        pdb.set_trace()

    if len(a) < 1:
        print("usage: crawl pw_decode B64STRING")
        sys.exit(1)

    print(base64.b64decode(a[0]))


# ------------------------------------------------------------------------------
def crl_readme(argv):
    """readme - scroll the package README to stdout
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(argv)

    if o.debug:
        pdb.set_trace()

    path = "README.md"
    if not util.git_repo(__file__):
        path = util.pathjoin(util.dirname(__file__), path)

    print util.contents(path)


# ------------------------------------------------------------------------------
def crl_sample_cfg(argv):
    """sample_cfg - scroll the sample configuration file to stdout
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(argv)

    if o.debug:
        pdb.set_trace()

    path = "crawl.cfg.sample"
    if not util.git_repo(__file__):
        path = util.pathjoin(util.dirname(__file__), path)

    print util.contents(path)


# ------------------------------------------------------------------------------
def crl_start(argv):
    """start - if the crawler is not already running as a daemon, start it

    usage: crawl start

    default config file: crawl.cfg, or
                         $CRAWL_CONF, or
                         -c <filename> on command line
    default log file:    /var/log/crawl.log, or
                         $CRAWL_LOG, or
                         -l <filename> on command line
    """
    p = optparse.OptionParser()
    p.add_option('-c', '--cfg',
                 action='store', default='', dest='config',
                 help='config file name')
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-l', '--log',
                 action='store', default='', dest='logfile',
                 help='specify the log file')
    p.add_option('-C', '--context',
                 action='store', default='', dest='context',
                 help="context of crawler ('TEST' or 'PROD')")
    (o, a) = p.parse_args(argv)

    if o.debug:
        pdb.set_trace()

    cfg = CrawlConfig.get_config(o.config)

    #
    # Initialize the configuration
    #
    if o.context != '':
        cfg.set('crawler', 'context', o.context)
    try:
        exitpath = cfg.get('crawler', 'exitpath')
    except CrawlConfig.NoOptionError, e:
        print("No exit path is specified in the configuration")
        sys.exit(1)

    log = CrawlConfig.get_logger(o.logfile, cfg)
    pfpath = make_pidfile(os.getpid(),
                          cfg.get('crawler', 'context'),
                          exitpath,
                          just_check=True)
    crawler = CrawlDaemon(pfpath,
                          stdout="crawler.stdout",
                          stderr="crawler.stderr",
                          logger=log,
                          workdir='.')
    log.info('crl_start: calling crawler.start()')
    crawler.start()
    pass


# ------------------------------------------------------------------------------
def crl_status(argv):
    """status - report whether the crawler is running or not

    usage: crawl status
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(argv)

    if o.debug:
        pdb.set_trace()

    rpi_l = running_pid()
    if rpi_l == []:
        print("The crawler is not running.")
    else:
        for rpi in rpi_l:
            (pid, context, exitpath) = rpi
            print("The crawler is running as process %d (context=%s)" %
                  (pid, context))
            if os.path.exists(exitpath):
                print("Termination has been requested (%s exists)" %
                      (exitpath))


# ------------------------------------------------------------------------------
def crl_stop(argv):
    """stop - shut down the crawler daemon if it is running

    usage: crawl stop
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-l', '--log',
                 action='store', default='crawler.log', dest='logfile',
                 help='specify the log file')
    p.add_option('-C', '--context',
                 action='store', default='', dest='context',
                 help="context of crawler (PROD/DEV/TEST)")
    (o, a) = p.parse_args(argv)

    if o.debug:
        pdb.set_trace()

    rpid_l = running_pid()
    if rpid_l == []:
        print("No crawlers are running -- nothing to stop.")
        return

    ctx_l = [rpid[1] for rpid in rpid_l]
    if o.context != '' and o.context not in ctx_l:
        print("No %s crawler is running -- nothing to stop." % o.context)
        return

    if o.context == '':
        if 1 == len(rpid_l):
            answer = raw_input("Preparing to stop %s crawler. Proceed? > " %
                               ctx_l[0])
            if answer.strip().lower().startswith('y'):
                print("Stopping the crawler...")
                testhelp.touch(rpid_l[0][2])
            else:
                print("No action taken")
        else:  # more than one entry in rpid_l
            print("More than one crawler is running.")
            print("Please specify a context (e.g., 'crawl stop -C PROD')")
    else:
        idx = ctx_l.index(o.context)
        print("Stopping the %s crawler..." % ctx_l[idx])
        testhelp.touch(rpid_l[idx][2])


# ------------------------------------------------------------------------------
def crl_version(argv):
    """version - report the crawler software version
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(argv)

    if o.debug:
        pdb.set_trace()

    if util.git_repo(__file__):
        path = ".hpssic_version"
    else:
        path = util.pathjoin(util.dirname(__file__), ".hpssic_version")

    hpssic_version = util.contents(path).strip()
    print("HPSS Integrity Crawler version %s" % hpssic_version)


# ------------------------------------------------------------------------------
def get_timeval(cfg, section, option, default):
    """
    Return the number of seconds indicated by the time spec, using default if
    any errors or failures occur
    """
    log = CrawlConfig.get_logger()
    return cfg.gettime(section, option, default, log)


# ------------------------------------------------------------------------------
def is_running(context=None):
    """
    Return True if the crawler is running (per ps(1)) or False otherwise.
    """
    running = False
    if context is None:
        cfg = CrawlConfig.get_config()
        try:
            context = cfg.get('crawler', 'context')
        except CrawlConfig.NoOptionError, e:
            emsg = ("No option 'context' in section 'crawler', file '%s'" %
                    cfg.filename)
            raise StandardError(emsg)

    rpi_l = running_pid()
    for rpi in rpi_l:
        if rpi[1] == context:
            running = True

    return running


# ------------------------------------------------------------------------------
def make_pidfile(pid, context, exitpath, just_check=False):
    """
    Generate a pid file in the pid directory (defined in CrawlDaemon), creating
    the directory if necessary.
    """
    ok = False
    piddir = CrawlDaemon.piddir
    if not os.path.exists(piddir):
        os.mkdir(piddir)
        ok = True

    if not ok:
        pf_l = glob.glob("%s/*" % piddir)
        for pf_n in pf_l:
            data = util.contents(pf_n)
            if 0 == len(data):
                continue
            (ctx, xp) = data.strip().split()
            if ctx == context:
                raise StandardError("The pidfile for context %s exists" %
                                    context)

    pfname = "%s/%d" % (piddir, pid)
    if just_check:
        return pfname

    with open(pfname, 'w') as f:
        f.write("%s %s\n" % (context, exitpath))

    return pfname


# ------------------------------------------------------------------------------
def running_pid(proc_required=True):
    """
    Return a list of pids if the crawler is running (per ps(1)) or [] otherwise
    """
    rval = []
    if proc_required:
        result = pexpect.run("ps -ewwo pid,cmd")
        for line in result.split("\n"):
            if 'crawl start' in line:
                pid = int(line.split()[0])
                pfpath = "%s/%d" % (CrawlDaemon.piddir, pid)
                if os.path.exists(pfpath):
                    (ctx, xpath) = util.contents(pfpath).strip().split()
                    rval.append((pid, ctx, xpath))
    else:
        pid_l = glob.glob("%s/*" % CrawlDaemon.piddir)
        for pid_n in pid_l:
            pid = int(os.path.basename(pid_n))
            (ctx, xpath) = util.contents(pid_n).strip().split()
            rval.append((pid, ctx, xpath))

    return rval


# ------------------------------------------------------------------------------
class CrawlDaemon(daemon.Daemon):
    """
    This class extends this daemon.Daemon to serve this application. Method
    run() gets run in the background and then calls fire() when appropriate to
    invoke a plugin.
    """
    piddir = "/tmp/crawler"

    # --------------------------------------------------------------------------
    def give_up_yet(self, tbstr):
        """
        Monitor exceptions we've seen. If we get more than N exceptions in T
        seconds (where N and T come from the configuration with default values
        of 3 and 7, respectively), shutdown. If we get more than M occurences
        of exactly the same traceback string regardless of timing, shutdown. If
        we more than Z tracebacks of any sort, shutdown.
        """
        if not hasattr(self, 'xseen'):
            self.xseen = {}
            self.ewhen = 0
            self.whenq = []
            self.ecount = 0
            self.xtotal = 0
            self.tlimit = float(self.cfg.get_d('crawler', 'xlim_time', "7.0"))
            self.climit = int(self.cfg.get_d('crawler', 'xlim_count', "3"))
            self.ilimit = int(self.cfg.get_d('crawler', 'xlim_ident', "5"))
            self.zlimit = int(self.cfg.get_d('crawler', 'xlim_total', '10'))

        rval = False

        # tbstr = tb.format_exc()
        now = time.time()
        self.ecount += 1
        self.whenq.append(now)
        if tbstr in self.xseen:
            self.xseen[tbstr] += 1
        else:
            self.xseen[tbstr] = 1
        self.xtotal += 1

        # log the traceback
        for line in tbstr.split('\n'):
            self.dlog("crawl: '%s'" % line)

        # give up because we got enough identical errors
        if self.ilimit <= self.xseen[tbstr]:
            self.dlog("crawl: shutting down because we got " +
                      "%d identical errors" % self.ilimit)
            rval = True

        # give up because we got enough total errors
        if self.zlimit <= self.xtotal:
            self.dlog("crawl: shutting down because we got " +
                      "%d total errors" % self.zlimit)
            rval = True

        # give up if we got enough errors in the time window
        dt = now - self.ewhen
        if self.climit <= self.ecount and dt < self.tlimit:
            self.dlog("crawl: shutting down because we got " +
                      "%d exceptions in %f seconds" %
                      (self.ecount, dt))
            rval = True
        elif self.tlimit <= dt:
            self.ecount = len(self.whenq)
            if 0 < self.ecount:
                self.ewhen = self.whenq.pop()

        # CrawlConfig.log("rval = %s; ecount = %d; dt = %f; whenq = %s" %
        #                 (rval, self.ecount, dt, self.whenq))
        return rval

    # --------------------------------------------------------------------------
    def fire_plugins(self, plugin_d):
        """
        Fire the plugins in plugin_d. Call self.give_up_yet() to track
        exceptions and decide whether or not to bail.
        """
        bail_out = False
        try:
            for p in plugin_d.keys():
                if plugin_d[p].time_to_fire():
                    plugin_d[p].fire()
        except:
            bail_out = self.give_up_yet(tb.format_exc())

        return bail_out

    # --------------------------------------------------------------------------
    def run(self):
        """
        This routine runs in the background as a daemon. Here's where
        we fire off plug-ins as appropriate.
        """
        cfgname = ''
        self.cfg = CrawlConfig.get_config(cfgname)
        self.pidfile = "%s/%d" % (self.piddir, os.getpid())
        exit_file = self.cfg.get('crawler', 'exitpath')
        make_pidfile(os.getpid(),
                     self.cfg.get('crawler', 'context'),
                     exit_file)
        atexit.register(self.delpid)

        keep_going = True
        plugin_d = {}
        while keep_going:
            try:
                pluglstr = self.cfg.get('crawler', 'plugins')
                pluglist = [x.strip() for x in pluglstr.split(',')]
                for s in pluglist:
                    self.dlog('crawl: CONFIG: [%s]' % s)
                    for o in self.cfg.options(s):
                        self.dlog('crawl: CONFIG: %s: %s' %
                                  (o, self.cfg.get(s, o)))
                    if s == 'crawler':
                        continue
                    elif s in plugin_d.keys():
                        CrawlConfig.log("reloading plugin %s" % s)
                        plugin_d[s].reload(self.cfg)
                    else:
                        CrawlConfig.log("initial load of plugin %s" % s)
                        plugin_d[s] = CrawlPlugin.CrawlPlugin(name=s,
                                                              cfg=self.cfg)

                # remove any plugins that are not in the new configuration
                for p in plugin_d.keys():
                    if p not in self.cfg.sections():
                        CrawlConfig.log("unloading obsolete plugin %s" % p)
                        del plugin_d[p]

                heartbeat = self.cfg.get_time('crawler', 'heartbeat', 10)
                # hb_msg = "crawl: heartbeat... "
                while keep_going:
                    #
                    # Fire any plugins that are due
                    #
                    if not self.cfg.quiet_time(time.time()):
                        hb_msg = "crawl: heartbeat..."
                        if self.fire_plugins(plugin_d):
                            keep_going = False
                    else:
                        hb_msg = "crawl: heartbeat... [quiet]"

                    # CrawlConfig.log("issue the heartbeat")
                    #
                    # Issue the heartbeat if it's time
                    #
                    if 0 == (int(time.time()) % heartbeat):
                        # self.dlog('crawl: heartbeat...')
                        self.dlog(hb_msg)

                    # CrawlConfig.log("check for config changes")
                    #
                    # If config file has changed, reload it.
                    # cached config object and breaking out of the inner loop.
                    #
                    if self.cfg.changed():
                        cfgname = self.cfg.get('crawler', 'filename')
                        self.cfg = CrawlConfig.get_config(reset=True)
                        break

                    # CrawlConfig.log("check for exit signal")
                    #
                    # Check for the exit signal
                    #
                    if util.conditional_rm(exit_file):
                        self.dlog('crawl: shutting down')
                        keep_going = False

                    # CrawlConfig.log("sleep")
                    #
                    # We cycle once per second so we can detect if the user
                    # asks us to stop or if the config file changes and needs
                    # to be reloaded
                    #
                    time.sleep(1.0)

            except:
                # if we get an exception, write the traceback to the log file
                tbstr = tb.format_exc()
                for line in tbstr.split('\n'):
                    self.dlog("crawl: '%s'" % line)
                keep_going = False

# ------------------------------------------------------------------------------
toolframe.tf_launch("crl", __name__)
