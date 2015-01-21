"""
Run a bunch of plug-ins which will probe the integrity of HPSS
"""
import atexit
import base64
import copy
import crawl_lib
import CrawlConfig
import CrawlDBI
import CrawlMail
import CrawlPlugin
import daemon
from datetime import datetime as dt
import dbschem
import getpass
import glob
import messages as MSG
import optparse
import os
import pdb
import shutil
import sys
import testhelp
import time
from datetime import timedelta as td
import traceback as tb
import util
import util as U
import version

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
        log = CrawlConfig.log(logpath=o.logpath, cfg=cfg)
        for line in dumpstr.split("\n"):
            CrawlConfig.log(line)


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

    tdirs = glob.glob("/tmp/hpss-crawl.*")
    for td in tdirs:
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

    result = dbschem.drop_table(cfg, tname)
    print(result)


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
    CrawlConfig.log(logpath=o.logpath, cfg=cfg)

    if o.plugname == '':
        print("'-p <plugin-name>' is required")
    elif not cfg.has_section(o.plugname):
        print("No plugin named '%s' found in configuration" % o.plugname)
    else:
        plugdir = cfg.get('crawler', 'plugin-dir')
        sys.path.append(plugdir)
        __import__(o.plugname)
        CrawlConfig.log('firing %s', o.plugname)
        sys.modules[o.plugname].main(cfg)


# ------------------------------------------------------------------------------
def crl_history(argv):
    """history - access to the plugin history

    usage: crawl history [--load|--show|--reset]

    --load {all,cv,mpra,tcc,rpt}
        Load the history table from listed plugin tables, log file

    --show
        Read the history table and report its contents.

    --reset
        Drop the history table.

    --read-log FILENAME
        If --load is specified and includes 'cv', read FILENAME and load cv
        history from it.

    To load just cv data, --load cv --read-log FILENAME
    To load just mpra data, --load mpra
    To load all plugins, --load all (or "") --read-log FILENAME

    If --load contains 'cv' but --read-log is not specified, an error message
    will be issued.

    If --load contains 'all' or is empty and --read-log is not specified, a
    warning will be issued to notify the user that cv data is not being loaded.

    If --load does not contain 'cv' or 'all' and is not empty and --read-log is
    specified, a warning will be issued that the log file is not being read and
    cv data is not being loaded.
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    p.add_option('-n', '--dry-run',
                 action='store_true', default=False, dest='dryrun',
                 help='just report')
    p.add_option('-l', '--load',
                 action='store', default=None, dest='loadlist',
                 help='plugins to load')
    p.add_option('-r', '--read-log',
                 action='store', default=None, dest='filename',
                 help='log file for cv history')
    p.add_option('-R', '--reset',
                 action='store_true', default=False, dest='reset',
                 help='drop the history table')
    p.add_option('-s', '--show',
                 action='store', default='unset', dest='show',
                 help='Report the contents of the history table')
    (o, a) = p.parse_args(argv)

    if o.debug:
        pdb.set_trace()

    # This is saying, if any two of our primary command line options are set,
    # we have a problem since they are all mutually exclusive.
    if o.show == 'unset':
        o.show = None
    if any([all([o.loadlist is not None, o.reset]),
            all([o.loadlist is not None, o.show]),
            all([o.reset, o.show])]):
        raise SystemExit(MSG.history_options)

    if o.dryrun:
        cfg = CrawlConfig.add_config()
        table = cfg.get('dbi-crawler', 'tbl_prefix') + '_history'
        dbname = cfg.get('dbi-crawler', 'dbname')
        hostname = cfg.get('dbi-crawler', 'hostname')

    if o.show:
        # This option is non-destructive, so we ignore --dry-run for it.
        history_show(o.show)
    elif o.reset:
        if o.dryrun:
            print(MSG.history_reset_dryrun_SSS % (table, dbname, hostname))
        else:
            print(dbschem.drop_table(table='history'))
    elif o.loadlist is not None:
        if o.dryrun:
            print(MSG.history_load_dryrun_SSSS %
                  (table, dbname, hostname, o.filename))
        else:
            history_load(o.loadlist, o.filename)


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

    cfg = CrawlConfig.get_config()
    CrawlConfig.log(" ".join(a), logpath=o.logfile, cfg=cfg)


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
    except CrawlConfig.NoOptionError as e:
        print("No exit path is specified in the configuration")
        sys.exit(1)

    vstr = "HPSS Integrity Crawler version %s" % version.__version__
    log = CrawlConfig.log(vstr, logpath=o.logfile, cfg=cfg)
    pfpath = make_pidfile(os.getpid(),
                          cfg.get('crawler', 'context'),
                          exitpath,
                          just_check=True)
    crawler = CrawlDaemon(pfpath,
                          stdout="crawler.stdout",
                          stderr="crawler.stderr",
                          logger=log,
                          workdir='.')
    CrawlConfig.log('crl_start: calling crawler.start()')
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
                util.touch(rpid_l[0][2])
            else:
                print("No action taken")
        else:  # more than one entry in rpid_l
            print("More than one crawler is running.")
            print("Please specify a context (e.g., 'crawl stop -C PROD')")
    else:
        idx = ctx_l.index(o.context)
        print("Stopping the %s crawler..." % ctx_l[idx])
        util.touch(rpid_l[idx][2])


# ------------------------------------------------------------------------------
def crl_syspath(argv):
    """syspath - dump python's sys.path array

    usage: crawl syspath
    """
    p = optparse.OptionParser()
    p.add_option('-d', '--debug',
                 action='store_true', default=False, dest='debug',
                 help='run the debugger')
    (o, a) = p.parse_args(argv)

    if o.debug:
        pdb.set_trace()

    for item in sys.path:
        print("    " + item)


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

    print("HPSS Integrity Crawler version %s" % version.__version__)


# ------------------------------------------------------------------------------
def history_load(loadlist, filename):
    """
    Each plugin's sublib has a load_history() routine that knows how to load
    its data to the history file.

    Unfortunately, we do have to know here something special about plugin 'cv'
    to warn the user when a filename was specified without 'cv' in the load
    list or vice versa and when to pass filename to the plugin's load_history()
    method.
    """
    cfg = CrawlConfig.add_config()
    pluglist = U.csv_list(cfg.get_d('crawler', 'plugins', U.default_plugins()))
    ll = U.csv_list(loadlist)
    if 'all' in ll or ll == []:
        ll = copy.deepcopy(pluglist)

    if filename is None and 'cv' in ll:
        print(MSG.history_cv_not_loaded)
        ll.remove('cv')
    elif filename is not None and 'cv' not in ll:
        print(MSG.history_filename_ignored)

    unk_plugs = [x for x in ll if x not in pluglist]
    if 0 < len(unk_plugs):
        print(MSG.unrecognized_plugin_S % ', '.join(unk_plugs))
        map(ll.remove, unk_plugs)

    if ll == []:
        return

    dbschem.make_table('history')
    for plug in [x for x in ll if x in pluglist]:
        print("loading %s..." % plug)
        if plug == 'cv' and filename is not None:
            args = [filename]
        else:
            args = []
        p = CrawlPlugin.CrawlPlugin(name=plug, cfg=cfg)
        p.load_history(*args)


# ------------------------------------------------------------------------------
def history_show(rptfmt):
    """
    Report the records in the history table in chronological order
    """
    funcname = 'history_show_' + rptfmt
    if funcname in globals():
        func = globals()[funcname]
        func()
    else:
        raise U.HpssicError(history_invalid_format_S % rptfmt)


# ------------------------------------------------------------------------------
def history_period_show(format, rewrite=lambda x: x):
    """
    Report record count by a time period defined by *format*
    """
    fld_list = ['date_format(from_unixtime(runtime), "%s") as period' % format,
                'plugin',
                'count(errors)']
    rows = crawl_lib.retrieve_history(fields=fld_list,
                                      groupby='plugin, period')
    data = dict((d1, dict((p, c) for d2, p, c in rows if d2 == d1))
                for d1, p, c in rows)
    gtotal = 0
    plist = sorted(set([p for d, p, c in rows]))
    hdr = ''.join(["Date       "] +
                  ["%10s" % x for x in sorted(plist)] +
                  ["%15s" % "total"])
    print hdr
    print "-" * 76
    psum = {}
    for p in plist:
        psum[p] = 0
    for d in sorted(data.keys()):
        lsum = 0
        rpt = "%-11s" % rewrite(d)
        for p in sorted(plist):
            if p in data[d]:
                rpt += "%10d" % data[d][p]
                lsum += data[d][p]
                psum[p] += data[d][p]
            else:
                rpt += "%10d" % 0
        rpt += "%15d" % lsum
        print rpt
        gtotal += lsum

    rpt = ''.join(["Total      "] +
                  ["%10d" % psum[x] for x in sorted(plist)] +
                  ["%15d" % gtotal])
    print "-" * 76
    print rpt


# ------------------------------------------------------------------------------
def history_show_bymonth():
    """
    Report record count by plugin by day
    """
    history_period_show("%Y.%m")


# ------------------------------------------------------------------------------
def history_show_byday():
    """
    Report record count by plugin by day
    """
    history_period_show("%Y.%m%d")


# ------------------------------------------------------------------------------
def history_show_byweek():
    """
    Report record count by plugin by day
    """
    history_period_show("%x.%v", rewrite=yw2ymd)


# ------------------------------------------------------------------------------
def history_show_count():
    """
    Report record count by plugin and total
    """
    rows = crawl_lib.retrieve_history(fields=['plugin', 'count(runtime)'],
                                      groupby='plugin')
    total = 0
    for r in rows:
        print("  %10s: %8d" % (r[0], r[1]))
        total += r[1]
    print("  %10s: %8d" % ("Total", total))


# ------------------------------------------------------------------------------
def history_show_raw():
    """
    Display a list of records from table history in chronological order
    """
    fmt = "%-20s %-10s %7s"
    rows = crawl_lib.retrieve_history()
    print(fmt % ("Run Time", "Plugin", "Errors"))
    for row in rows:
        print(fmt % (U.ymdhms(row[1]),
                     row[0],
                     str(row[2])))


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
        except CrawlConfig.NoOptionError as e:
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
def pidcmd():
    """
    Collect a list of running processes and their command lines
    """
    rval = ""
    for proc in glob.glob("/proc/*"):
        bname = util.basename(proc)
        if not bname.isdigit():
            continue
        try:
            cmdline = util.contents(util.pathjoin(proc, "cmdline"))
            if 0 == len(cmdline):
                continue
        except IOError:
            continue
        rval += "%s %s\n" % (bname, cmdline.replace("\x00", " "))
    return rval


# ------------------------------------------------------------------------------
def running_pid(proc_required=True):
    """
    Return a list of pids if the crawler is running (per ps(1)) or [] otherwise
    """
    rval = []
    if proc_required:
        result = pidcmd()
        for line in result.split("\n"):
            if 'crawl start' in line:
                pid = int(line.split()[0])
                pfpath = "%s/%d" % (CrawlDaemon.piddir, pid)
                if os.path.exists(pfpath):
                    (ctx, xpath) = util.contents(pfpath).strip().split()
                else:
                    # crawler is running but the pid file has been lost
                    cfg = CrawlConfig.add_config()
                    ctx = cfg.get('crawler', 'context')
                    xpath = cfg.get('crawler', 'exitpath')
                    make_pidfile(pid, ctx, xpath)
                rval.append((pid, ctx, xpath))
    else:
        pid_l = glob.glob("%s/*" % CrawlDaemon.piddir)
        for pid_n in pid_l:
            pid = int(os.path.basename(pid_n))
            (ctx, xpath) = util.contents(pid_n).strip().split()
            rval.append((pid, ctx, xpath))

    return rval


# ------------------------------------------------------------------------------
def stop_wait(cfg=None):
    """
    Watch for the crawler's exit file to disappear. If it's still there after
    the timeout period, give up and throw an exception.
    """
    if cfg is None:
        cfg = CrawlConfig.get_config()
    context = cfg.get('crawler', 'context')
    exitpath = cfg.get('crawler', 'exitpath')
    timeout = cfg.get_time('crawler', 'stopwait_timeout', 5.0)
    sleep_time = cfg.get_time('crawler', 'sleep_time', 0.25)
    lapse = 0.0

    while is_running(context) and lapse < timeout:
        time.sleep(sleep_time)
        lapse += sleep_time

    if is_running(context) and timeout <= lapse:
        raise util.HpssicError("Stop wait timeout exceeded")


# ------------------------------------------------------------------------------
def yw2ymd(yw):
    """
    Convert a year.week string (*yw*) to ymd of the first day of the week

    Starting from the beginning of the year (jan1), we add the number of
    intervening weeks times 7 to the date of jan1. That yields a date in the
    week of interest. From that date, we subtract enough days to get us back to
    Monday. That's the beginning of the week.
    """
    (y, w) = yw.split('.')
    jan1 = dt(int(y), 1, 1)
    (_, wkj1, _) = jan1.isocalendar()
    end = jan1 + td((int(w)-wkj1)*7)
    mon = end - td(end.isoweekday()-1)
    return mon.strftime("%Y.%m%d")


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
            self.tlimit = self.cfg.get_time('crawler', 'xlim_time', 7.0)
            self.climit = int(self.cfg.get_d('crawler', 'xlim_count', "3"))
            self.ilimit = int(self.cfg.get_d('crawler', 'xlim_ident', "5"))
            self.zlimit = int(self.cfg.get_d('crawler', 'xlim_total', '10'))

        rval = False

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
            reason = ("crawl: shutting down because we got " +
                      "%d identical errors" % self.ilimit)
            self.dlog(reason)
            CrawlMail.send(cfg=self.cfg,
                           to="alerts.email",
                           subj="HPSS Integrity Crawler shutdown",
                           msg="""
                           HPSS Integrity Crawler shutting down.
                           %s
                           """ % reason)
            rval = True

        # give up because we got enough total errors
        if self.zlimit <= self.xtotal:
            reason = ("crawl: shutting down because we got " +
                      "%d total errors" % self.zlimit)
            self.dlog(reason)
            CrawlMail.send(cfg=self.cfg,
                           to="alerts.email",
                           subj="HPSS Integrity Crawler shutdown",
                           msg="""
                           HPSS Integrity Crawler shutting down.
                           %s
                           """ % reason)
            rval = True

        # give up if we got enough errors in the time window
        dt = now - self.ewhen
        if self.climit <= self.ecount and dt < self.tlimit:
            reason = ("crawl: shutting down because we got " +
                      "%d exceptions in %f seconds" %
                      (self.ecount, dt))
            self.dlog(reason)
            CrawlMail.send(cfg=self.cfg,
                           to="alerts.email",
                           subj="HPSS Integrity Crawler shutdown",
                           msg="""
                           HPSS Integrity Crawler shutting down.
                           %s
                           """ % reason)
            rval = True

        elif self.tlimit <= dt:
            self.ecount = len(self.whenq)
            if 0 < self.ecount:
                self.ewhen = self.whenq.pop()

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
                        # self.dlog(hb_msg)
                        CrawlConfig.log(hb_msg)

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
