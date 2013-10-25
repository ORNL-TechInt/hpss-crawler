#!/usr/bin/env python
"""
toolframe - creating a script with commandline-callable subfunctions

To use this, a script must

To build a tool-style program (i.e., one with multiple entry points
that can be called individually from the command line with a list of
options and arguments):

    * provide a prefix() routine that returns the prefix that will
      indicate command line callable subfunctions

    * provide a collection of callable subfunctions with names like
      '%s_callme' % prefix()

    * Call toolframe.tf_launch() (no args) outside any function

    * Additional features:
    
       * When run as 'program.py -L', symlink program -> program.py is
         created so the program can be easily called from the command
         line without typing the .py suffix.

       * When run as 'program.py', any unittest cases that are defined
         will be run.

       * When run as 'program subfunc <options> <arguments>',
         subfunction prefix_subfunc will be called.

Non-tool-style programs can call toolfram.ez_launch(main) to get the
following:

    * When run as 'program.py -L', a symlink program -> program.py
      will be created so the program can be easily called from the
      command line.

    * When run as 'program.py', any unittest test cases defined will
      be run.

    * When run as 'program <options> <args>', routine main (the one
      passed to ez_launch) will be called with sys.argv as an
      argument.

History
   2011.0209   inception
   2011.0304   figured out to pass routine main to ez_launch
"""
import os
import pdb
import re
import sys
import testhelp
import traceback as tb
import unittest

# -----------------------------------------------------------------------------
def tf_main(args, prefix=None):
    mainmod = sys.modules['__main__']
    if prefix == None:
        prefix = mainmod.prefix()
    if len(args) < 2:
        tf_help([], prefix=prefix)
    elif args[1] == "help":
        tf_help(args[2:], prefix=prefix)
    else:
        try:
            method = getattr(mainmod, "%s_%s" % (prefix, args[1]))
            method(args[2:])
        except IndexError, e:
            if len(a) < 2:
                print(str(e))
                tf_help([], prefix=prefix)
            else:
                tb.print_exc()
        except NameError, e:
            if args[1] not in dir(mainmod):
                print(str(e))
                print("unrecognized subfunction: %s" % args[1])
                tf_help([], prefix=prefix)
            else:
                tb.print_exc()
        except Exception, e:
            tb.print_exc()

# ----------------------------------------------------------------------------
def tf_help(A, prefix=None):
    """help - show this list

    usage: <program> help [function-name]

    If a function name is given, show the functions __doc__ member.
    Otherwise, show a list of functions based on the first line of
    each __doc__ member.
    """
    d = dir(sys.modules['__main__'])
    if prefix == None:
        prefix = sys.modules['__main__'].prefix()
    if 0 < len(A):
        if '%s_%s' % (prefix, A[0]) in d:
            dname = "sys.modules['__main__'].%s_%s.__doc__" % (prefix, A[0])
            x = eval(dname)
            print x
        elif A[0] == 'help':
            x = tf_help.__doc__
            print x
        return

    d.append('tf_help')
    for o in d:
        if o.startswith(prefix + '_'):
            f = o.replace(prefix + '_', '')
            x = eval("sys.modules['__main__'].%s.__doc__" % o)
            docsum = x.split('\n')[0]
            print "   %s" % (docsum)
        elif o == 'tf_help':
            f = 'help'
            docsum = tf_help.__doc__.split('\n')[0]
            print "   %s" % (docsum)
            

# -----------------------------------------------------------------------------
def tf_launch(prefix,
              modname,
              setup_tests=None,
              cleanup_tests=None,
              testclass='',
              logfile=''):
    if '__main__' != modname:
        return
    if len(sys.argv) == 1 and sys.argv[0] == '':
        return
    sname = sys.argv[0]
    pname = re.sub('.py$', '', sname)
    if sname.endswith('.py') and not os.path.exists(pname):
        print("creating symlink: %s -> %s" % (pname, sname))
        os.symlink(sname, pname)
    elif sys._getframe(1).f_code.co_name in ['?', '<module>']:
        if sname.endswith('.py'):
            if '-d' in sys.argv:
                sys.argv.remove('-d')
                pdb.set_trace()
            if None != setup_tests:
                setup_tests()
            keep = testhelp.main(sys.argv, testclass, logfile=logfile)
            if None != cleanup_tests and not keep:
                cleanup_tests()
        else:
            tf_main(sys.argv, prefix=prefix)

# -----------------------------------------------------------------------------
def ez_launch(main = None,
              setup=None,
              cleanup=None,
              test=None,
              logfile=''):
    # pdb.set_trace()
    if len(sys.argv) == 1 and sys.argv[0] == '':
        return
    sname = sys.argv[0]
    pname = re.sub('.py$', '', sname)
    if sname.endswith('.py') and not os.path.exists(pname) and '-L' in sys.argv:
        print("creating symlink: %s -> %s" % (pname, sname))
        os.symlink(sname, pname)
    elif sys._getframe(1).f_code.co_name in ['?', '<module>']:
        if sname.endswith('.py'):
            if '-d' in sys.argv:
                sys.argv.remove('-d')
                pdb.set_trace()
            if test == None:
                unittest.main()
            else:
                if setup != None:
                    setup()
                keep = testhelp.main(sys.argv,test, logfile=logfile)
                if not keep and cleanup != None:
                    cleanup()
        elif main != None:
            main(sys.argv)
