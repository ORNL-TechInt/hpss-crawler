#!/usr/bin/env python

import atexit
import errno
import os
import resource
import sys
import time
from signal import SIGTERM

MAXFD = 2048

# ---------------------------------------------------------------------------
class Daemon:
    """
    A generic daemon class.
    
    Usage: subclass the Daemon class and override the run() method
    """
    # -----------------------------------------------------------------------
    def __init__(self, pidfile, stdin='/dev/null',
                 stdout='/dev/null', stderr='/dev/null', logger=None):
        self.origdir = os.getcwd()
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile
        self.logger = logger
        
    # -----------------------------------------------------------------------
    def daemonize(self):
        """
        do the UNIX double-fork magic, see Stevens' "Advanced
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
        except OSError, e:
            sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)
        
        # decouple from parent environment
        workdir = "/tmp/esg"
        if not os.path.exists(workdir):
            os.mkdir(workdir)
        elif not os.path.isdir(workdir):
            raise StandardError("Can't cd to '%s' -- it's not a directory" % workdir)
        os.chdir(workdir)
        os.setsid()
        os.umask(0)
        
        # do second fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit from second parent
                sys.exit(0)
            self.dlog("fork #2 child: %d" % os.getpid())
        except OSError, e:
            sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)
            
        # redirect standard file descriptors
        self.dlog("flushing stdout, stderr")
        sys.stdout.flush()
        sys.stderr.flush()

        self.dlog("closing all open files");
        dont_close = [
            self.logger.handlers[0].stream.fileno()
        ]
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

        # write pidfile
        self.dlog("atexit register, write pid file")
        atexit.register(self.delpid)
        pid = str(os.getpid())
        file(self.pidfile,'w+').write("%s\n" % pid)

        self.dlog("leaving daemonize")
        
    # -----------------------------------------------------------------------
    def close_if_open(self, fd):
        try:
            os.close(fd)
        except OSError, exc:
            if exc.errno == errno.EBADF:
                # file was not open
                pass
            else:
                sys.stdout.write("Failed to close file descriptor %(fd)d"
                                 + " (%(exc)s)" % vars())
                raise exc
            
    # -----------------------------------------------------------------------
    def dlog(self, message):
        if self.logger:
            self.logger.info(message)
            
    # -----------------------------------------------------------------------
    def delpid(self):
        os.remove(self.pidfile)

    # -----------------------------------------------------------------------
    def get_max_fd(self):
        limits = resource.getrlimit(resource.RLIMIT_NOFILE)
        result = limits[1]
        if result == resource.RLIM_INFINITY:
            result = MAXFD
        return result
    
    # -----------------------------------------------------------------------
    def start(self):
        """
        Start the daemon
        """
        # Check for a pidfile to see if the daemon is already running
        try:
            self.dlog("checking for pidfile %s" % self.pidfile)
            pf = file(self.pidfile,'r')
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
            
    # -----------------------------------------------------------------------
    def stop(self):
        """
        Stop the daemon
        """
        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
            
        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return # not an error in a restart
        
        # Try killing the daemon process       
        try:
            while 1:
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print str(err)
                sys.exit(1)
                
    # -----------------------------------------------------------------------
    def restart(self):
        """
        Restart the daemon
        """
        self.stop()
        self.start()
 
    # -----------------------------------------------------------------------
    def run(self):
        """
        You should override this method when you subclass Daemon. It
        will be called after the process has been daemonized by
        start() or restart().
        """

    # -----------------------------------------------------------------------
    def trace(self, msg):
        """
        """
        f = open("/tmp/esg/pubd/pubd.trace", "a")
        now = time.strftime("%Y.%m%d %H:%M:%S")
        f.write("%(now)s %(msg)s\n" % vars())
        f.close()
        
