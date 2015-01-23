"""
A generic daemon class
"""
import errno
import os
import resource
import sys
import time
from signal import SIGTERM

MAXFD = 2048


# -----------------------------------------------------------------------------
class Daemon(object):
    """
    Usage: subclass the Daemon class and override the run() method
    """
    # -------------------------------------------------------------------------
    def __init__(self,
                 pidfile,
                 stdin='/dev/null',
                 stdout='/dev/null',
                 stderr='/dev/null',
                 workdir='.',
                 logger=None):
        """
        Argument pidfile controls where the process id is written.

        The stdin/stdout/stderr arguments are paths to files where the process
        stdin, stdout, and stderr will be written, respectively.

        We'll chdir to the directory indicated by workdir before starting our
        payload.

        If logger is available, we'll use it to report what's going on.
        """
        self.origdir = os.getcwd()
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile
        self.logger = logger
        self.workdir = workdir

    # -------------------------------------------------------------------------
    def __repr__(self):
        return("Daemon<pidfile=%s>" % self.pidfile)

    # -------------------------------------------------------------------------
    def daemonize(self):
        """
        Do the UNIX double-fork magic, see Stevens' "Advanced
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        self.dlog("starting daemonize() -----------------------------------")
        try:
            pid = os.fork()
            if pid > 0:
                # exit first parent
                sys.exit(0)
            self.dlog("fork #1 child: %d" % os.getpid())
        except OSError as e:
            sys.stderr.write("fork #1 failed: %d (%s)\n" %
                             (e.errno, e.strerror))
            sys.exit(1)

        # decouple from parent environment
        if not os.path.exists(self.workdir):
            os.mkdir(self.workdir)
        elif not os.path.isdir(self.workdir):
            raise StandardError("Can't cd to '%s' -- it's not a directory"
                                % self.workdir)
        os.chdir(self.workdir)
        os.setsid()
        os.umask(0)

        # do second fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit from second parent
                sys.exit(0)
            self.dlog("fork #2 child: %d" % os.getpid())
        except OSError as e:
            sys.stderr.write("fork #2 failed: %d (%s)\n" %
                             (e.errno, e.strerror))
            sys.exit(1)

        # redirect standard file descriptors
        self.dlog("flushing stdout, stderr")
        sys.stdout.flush()
        sys.stderr.flush()

        self.dlog("closing all open files")
        dont_close = []
        if self.logger is not None:
            dont_close.append(self.logger.handlers[0].stream.fileno())

        # close all open files
        maxfd = self.get_max_fd()
        for fd in reversed(range(maxfd)):
            if fd not in dont_close:
                self.close_if_open(fd)

        # open the std file descriptors passed in to constructor
        self.dlog("opening stdin")
        si = file(self.stdin, 'r')
        self.dlog("opening stdout")
        so = open(self.stdout, 'a+')
        self.dlog("opening stderr")
        se = file(self.stderr, 'a+', 0)

        sys.stdin = si
        sys.stdout = so
        sys.stderr = se

        self.dlog("leaving daemonize")

    # -------------------------------------------------------------------------
    def close_if_open(self, fd):
        """
        Attempt to close an open file descriptor. This is used to release
        resources in the parent before forking a child.
        """
        try:
            os.close(fd)
        except OSError as exc:
            if exc.errno == errno.EBADF:
                # file was not open
                pass
            else:
                sys.stdout.write("Failed to close file descriptor %(fd)d"
                                 + " (%(exc)s)" % vars())
                raise exc

    # -------------------------------------------------------------------------
    def dlog(self, message):
        """
        Conditional logging
        """
        if self.logger:
            self.logger.info(message)

    # -------------------------------------------------------------------------
    def delpid(self):
        """
        The problem with just removing the pid file on the way out is that
        doing so leaves a tiny little window when the crawler is still in the
        process table and the pid file does not exist. If running_pid() gets
        called by another process during that window, it will regenerate the
        missing pid file. By leaving the pid file named <pid>.DEFUNCT, we make
        it clear that the pid file is not just missing but is no longer valid
        and should not be regenerated.

        was: Remove the pid file on the way out.
        """
        if os.path.exists(self.pidfile):
            os.rename(self.pidfile, self.pidfile + '.DEFUNCT')

    # -------------------------------------------------------------------------
    def get_max_fd(self):
        """
        Find out what the system's highest per process file descriptor can be
        """
        limits = resource.getrlimit(resource.RLIMIT_NOFILE)
        result = limits[1]
        if result == resource.RLIM_INFINITY:
            result = MAXFD
        return result

    # -------------------------------------------------------------------------
    def start(self):
        """
        Start the daemon
        """
        # Check for a pidfile to see if the daemon is already running
        try:
            self.dlog("checking for pidfile %s" % self.pidfile)
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if pid:
            message = "pidfile %s already exists. Daemon already running?"
            self.dlog(message)
            self.dlog("giving up")
            sys.stderr.write(message % self.pidfile)
            sys.stderr.write("\n")
            sys.exit(1)

        # Start the daemon
        self.dlog("daemon.start: calling daemon.daemonize")
        self.daemonize()
        self.dlog("daemon.start: calling daemon.run")
        self.run()

    # -------------------------------------------------------------------------
    def stop(self):
        """
        Stop the daemon
        """
        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return  # not an error in a restart

        # Try killing the daemon process
        try:
            while 1:
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
        except OSError as err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print str(err)
                sys.exit(1)

    # -------------------------------------------------------------------------
    def restart(self):
        """
        Restart the daemon
        """
        self.stop()
        self.start()

    # -------------------------------------------------------------------------
    def run(self):
        """
        You should override this method when you subclass Daemon. It
        will be called after the process has been daemonized by
        start() or restart().
        """
