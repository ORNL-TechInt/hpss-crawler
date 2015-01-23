import inspect
from hpssic import messages as MSG
import os
import pdb
import pexpect
import pytest
import re
import sys
from hpssic import testhelp as th
import unittest
from hpssic import util as U


# -----------------------------------------------------------------------------
class Test_ABLE(th.HelpedTestCase):
    # -------------------------------------------------------------------------
    @pytest.mark.slow
    def test_000_pep8(self):
        """
        Apply pep8 to all .py files in the system and return the result
        """
        full_result = ""
        for r, d, f in os.walk('hpssic'):
            pylist = [os.path.abspath(os.path.join(r, fn))
                      for fn in f
                      if fn.endswith('.py') and not fn.startswith(".#")]
            inputs = " ".join(pylist)
            if any([r == "./test",
                    ".git" in r,
                    ".attic" in r,
                    "" == inputs]):
                continue
            result = pexpect.run("pep8 %s" % inputs)
            full_result += result.replace(MSG.cov_no_data, "")
        self.expected("", full_result)

    # -------------------------------------------------------------------------
    def test_100_duplicates(self):
        """
        Scan all .py files for duplicate function names
        """
        self.dbgfunc()
        dupl = {}
        for r, d, f in os.walk('hpssic'):
            for fname in f:
                path = os.path.join(r, fname)
                if "CrawlDBI" in path:
                    continue
                if path.endswith(".py") and not fname.startswith(".#"):
                    result = check_for_duplicates(path)
                    if result != '':
                        dupl[path] = result
        if dupl != {}:
            rpt = ''
            for key in dupl:
                rpt += "Duplicates in %s:" % key + dupl[key] + "\n"
            self.fail(rpt)


# -----------------------------------------------------------------------------
class ScriptBase(th.HelpedTestCase):
    # -------------------------------------------------------------------------
    def script_which_module(self, modname):
        """
        ScriptBase: Make sure we're loading the right tcc module
        """
        try:
            mod = sys.modules[modname]
        except KeyError:
            sep = impname = ''
            for comp in modname.split('.'):
                impname += sep + comp
                sep = '.'
                __import__(impname)
            mod = sys.modules[modname]

        tdir = improot(__file__, __name__)
        mdir = improot(mod.__file__, modname)
        self.expected(tdir, mdir)

    # -------------------------------------------------------------------------
    def script_which_command(self, cmdname):
        """
        ScriptBase: Make sure the tcc command exists and is executable
        """
        cmd = pexpect.which(cmdname)
        if cmd is None:
            cmd = "bin/" + cmdname
        self.assertTrue(os.access(cmd, os.X_OK))

    # -------------------------------------------------------------------------
    def script_help(self, cmdname, helplist):
        """
        ScriptBase: Make sure 'tcc help' generates something reasonable
        """
        cmd = pexpect.which(cmdname)
        if cmd is None:
            cmd = "bin/" + cmdname
        result = pexpect.run("%s help" % cmd)
        self.assertFalse("Traceback" in result,
                         "'Traceback' not expected in %s" %
                         U.line_quote(result))
        for item in helplist:
            self.assertTrue(item in result,
                            "Expected '%s' in '%s'" % (item, result))


# -----------------------------------------------------------------------------
class Test_CRAWL(ScriptBase):
    # -------------------------------------------------------------------------
    def test_crawl_syspath(self):
        """
        Test the command 'crawl syspath'
        """
        result = pexpect.run("crawl syspath")
        assert 'Traceback' not in result

    # -------------------------------------------------------------------------
    def test_crawl_which_lib(self):
        """
        Test_CRAWL:
        """
        super(Test_CRAWL, self).script_which_module("hpssic.crawl_lib")

    # -------------------------------------------------------------------------
    def test_crawl_which_module(self):
        """
        Test_CRAWL:
        """
        super(Test_CRAWL, self).script_which_module("hpssic.crawl")

    # -------------------------------------------------------------------------
    def test_crawl_which_command(self):
        """
        Test_CRAWL:
        """
        super(Test_CRAWL, self).script_which_command("crawl")

    # -------------------------------------------------------------------------
    def test_crawl_help(self):
        """
        Test_CRAWL:
        """
        super(Test_CRAWL, self).script_help("crawl",
                                            ["cfgdump - ",
                                             "cleanup - ",
                                             "dbdrop - ",
                                             "fire - ",
                                             "log - ",
                                             "pw_decode - ",
                                             "pw_encode - ",
                                             "start - ",
                                             "status - ",
                                             "stop - ",
                                             ])


# -----------------------------------------------------------------------------
class Test_CV(ScriptBase):
    # -------------------------------------------------------------------------
    def test_cv_help(self):
        """
        Test_CV:
        """
        super(Test_CV, self).script_help("cv",
                                         ["fail_reset - ",
                                          "nulltest - ",
                                          "report - ",
                                          "show_next - ",
                                          "simplug - ",
                                          "test_check - ",
                                          ])

    # -------------------------------------------------------------------------
    def test_cv_which_command(self):
        """
        Test_CV:
        """
        super(Test_CV, self).script_which_command("cv")

    # -------------------------------------------------------------------------
    def test_cv_which_module(self):
        """
        Test_CV:
        """
        super(Test_CV, self).script_which_module("hpssic.cv")

    # -------------------------------------------------------------------------
    def test_cv_which_plugin(self):
        """
        Test_CV:
        """
        super(Test_CV, self).script_which_module("hpssic.plugins.cv_plugin")


# -----------------------------------------------------------------------------
class Test_MPRA(ScriptBase):
    # -------------------------------------------------------------------------
    def test_mpra_help(self):
        """
        Test_MPRA:
        """
        super(Test_MPRA, self).script_help("mpra",
                                           ["age - ",
                                            "date_age - ",
                                            "epoch - ",
                                            "history - ",
                                            "migr_recs - ",
                                            "purge_recs - ",
                                            "reset - ",
                                            "simplug - ",
                                            "times - ",
                                            "xplocks - ",
                                            "ymd",
                                            ])

    # -------------------------------------------------------------------------
    def test_mpra_which_command(self):
        """
        Test_MPRA:
        """
        super(Test_MPRA, self).script_which_command("mpra")

    # -------------------------------------------------------------------------
    def test_mpra_which_lib(self):
        """
        Test_MPRA:
        """
        super(Test_MPRA, self).script_which_module("hpssic.mpra_lib")

    # -------------------------------------------------------------------------
    def test_mpra_which_module(self):
        """
        Test_MPRA:
        """
        super(Test_MPRA, self).script_which_module("hpssic.mpra")

    # -------------------------------------------------------------------------
    def test_mpra_which_plugin(self):
        """
        Test_MPRA:
        """
        super(Test_MPRA, self).script_which_module(
            "hpssic.plugins.mpra_plugin")


# -----------------------------------------------------------------------------
class Test_RPT(ScriptBase):
    # -------------------------------------------------------------------------
    def test_rpt_help(self):
        """
        Test_RPT:
        """
        super(Test_RPT, self).script_help("rpt",
                                          ["report - ",
                                           "simplug - ",
                                           "testmail - ",
                                           ])

    # -------------------------------------------------------------------------
    def test_rpt_which_command(self):
        """
        Test_RPT:
        """
        super(Test_RPT, self).script_which_command("rpt")

    # -------------------------------------------------------------------------
    def test_rpt_which_lib(self):
        """
        Test_RPT:
        """
        super(Test_RPT, self).script_which_module("hpssic.rpt_lib")

    # -------------------------------------------------------------------------
    def test_rpt_which_module(self):
        """
        Test_RPT:
        """
        super(Test_RPT, self).script_which_module("hpssic.rpt")

    # -------------------------------------------------------------------------
    def test_rpt_which_plugin(self):
        """
        Test_RPT:
        """
        super(Test_RPT, self).script_which_module("hpssic.plugins.rpt_plugin")


# -----------------------------------------------------------------------------
class Test_TCC(ScriptBase):
    # -------------------------------------------------------------------------
    def test_tcc_help(self):
        """
        Test_TCC:
        """
        super(Test_TCC, self).script_help("tcc",
                                          ["bfid - ",
                                           "bfpath - ",
                                           "copies_by_cos - ",
                                           "copies_by_file - ",
                                           "report - ",
                                           "selbf - ",
                                           "simplug - ",
                                           "tables - ",
                                           "zreport - ",
                                           ])

    # -------------------------------------------------------------------------
    def test_tcc_which_command(self):
        """
        Test_TCC:
        """
        super(Test_TCC, self).script_which_command("tcc")

    # -------------------------------------------------------------------------
    def test_tcc_which_lib(self):
        """
        Test_TCC:
        """
        super(Test_TCC, self).script_which_module("hpssic.tcc_lib")

    # -------------------------------------------------------------------------
    def test_tcc_which_module(self):
        """
        Test_TCC:
        """
        super(Test_TCC, self).script_which_module("hpssic.tcc")

    # -------------------------------------------------------------------------
    def test_tcc_which_plugin(self):
        """
        Test_TCC:
        """
        super(Test_TCC, self).script_which_module("hpssic.plugins.tcc_plugin")


# -----------------------------------------------------------------------------
@U.memoize
def rgx_def(obarg):
    """
    Return a compiled regex for finding function definitions
    """
    return re.compile("^\s*def\s+(\w+)\s*\(")


# -----------------------------------------------------------------------------
@U.memoize
def rgx_class(obarg):
    """
    Return a compiled regex for finding class definitions
    """
    return re.compile("^\s*class\s+(\w+)\s*\(")


# -----------------------------------------------------------------------------
def check_for_duplicates(path):
    """
    Scan *path* for duplicate function names
    """
    rx_def = rgx_def(0)
    rx_cls = rgx_class(0)

    flist = []
    rval = ''
    cur_class = ''
    with open(path, 'r') as f:
        for l in f.readlines():
            q = rx_cls.match(l)
            if q:
                cur_class = q.groups()[0] + '.'

            q = rx_def.match(l)
            if q:
                cur_def = q.groups()[0]
                flist.append(cur_class + cur_def)

    if len(flist) != len(set(flist)):
        flist.sort()
        last = ''
        for foof in flist:
            if last == foof and foof != '__init__':
                rval += "\n   %s" % foof
            last = foof
    return rval


# -----------------------------------------------------------------------------
def improot(path, modpath):
    """
    Navigate upward in *path* as many levels as there are in *modpath*
    """
    rval = path
    for x in modpath.split('.'):
        rval = os.path.dirname(rval)
    return rval


# -----------------------------------------------------------------------------
def test_nodoc():
    """
    Report routines missing a doc string
    """
    pytest.dbgfunc()

    # get our bearings -- where is hpssic?
    hpssic_dir = U.dirname(sys.modules['hpssic'].__file__)

    excludes = ['setup.py', '__init__.py']

    # up a level from there, do we have a '.git' directory? That is, are we in
    # a git repository? If so, we want to talk the whole repo for .py files
    hpssic_par = U.dirname(hpssic_dir)
    if not os.path.isdir(U.pathjoin(hpssic_par, ".git")):
        # otherwise, we just work from hpssic down
        wroot = hpssic_dir
    else:
        wroot = hpssic_par

    # collect all the .py files in pylist
    pylist = []
    for r, dlist, flist in os.walk(wroot):
        if '.git' in dlist:
            dlist.remove('.git')
        pylist.extend([U.pathjoin(r, x)
                       for x in flist
                       if x.endswith('.py') and x not in excludes])

    # make a list of the modules implied in pylist in mdict. Each module name
    # is a key. The associated value is False until the module is checked.
    mlist = ['hpssic']
    for path in pylist:
        # Throw away the hpssic parent string, '.py' at the end, and split on
        # '/' to get a list of the module components
        mp = path.replace(hpssic_par + '/', '').replace('.py', '').split('/')

        if 1 < len(mp):
            fromlist = ['hpssic']
        else:
            fromlist = []
        mname = '.'.join(mp)
        if mname.startswith('hpssic'):
            mlist.append(mname)
        if mname not in sys.modules and mname.startswith('hpssic'):
            try:
                __import__(mname, fromlist=fromlist)

            except ImportError:
                pytest.fail('Failure trying to import %s' % mname)

    result = ''
    for m in mlist:
        result += nodoc_check(sys.modules[m], pylist, 0, 't')

    # result = nodoc_check(hpssic, 0, 't')
    if result != '':
        pytest.fail(result)


# -----------------------------------------------------------------------------
def nodoc_check(mod, pylist, depth, why):
    """
    Walk the tree of modules and classes looking for routines with no doc
    string and report them
    """
    # -------------------------------------------------------------------------
    def filepath_reject(obj, pylist):
        """
        Reject the object based on its filepath
        """
        if hasattr(obj, '__file__'):
            fpath = obj.__file__.replace('.pyc', '.py')
            rval = fpath not in pylist
        elif hasattr(obj, '__func__'):
            fpath = obj.__func__.func_code.co_filename.replace('.pyc', '.py')
            rval = fpath not in pylist
        else:
            rval = False

        return rval

    # -------------------------------------------------------------------------
    def name_accept(name, already):
        """
        Whether to accept the object based on its name
        """
        rval = True
        if all([name in dir(unittest.TestCase),
                name not in dir(th.HelpedTestCase)]):
            rval = False
        elif name in already:
            rval = False
        elif all([name.startswith('__'),
                  name != '__init__']):
            rval = False

        return rval

    # -------------------------------------------------------------------------
    global count
    try:
        already = nodoc_check._already
    except AttributeError:
        count = 0
        nodoc_check._already = ['AssertionError',
                                'base64',
                                'bdb',
                                'contextlib',
                                'datetime',
                                'decimal',
                                'difflib',
                                'dis',
                                'email',
                                'errno',
                                'fcntl',
                                'getopt',
                                'getpass',
                                'glob',
                                'heapq',
                                'inspect',
                                'InterpolationMissingOptionError',
                                'linecache',
                                'logging',
                                'MySQLdb',
                                'NoOptionError',
                                'NoSectionError',
                                'optparse',
                                'os',
                                'pdb',
                                'pexpect',
                                'pickle',
                                'pprint',
                                'pwd',
                                'pytest',
                                're',
                                'shlex',
                                'shutil',
                                'smtplib',
                                'socket',
                                'sqlite3',
                                'ssl',
                                'stat',
                                'StringIO',
                                'sys',
                                'tempfile',
                                'text',
                                'timedelta',
                                'times',
                                'tokenize',
                                'traceback',
                                'unittest',
                                'urllib',
                                'warnings',
                                'weakref',
                                ]
        already = nodoc_check._already

    rval = ''

    if any([mod.__name__ in already,
            U.safeattr(mod, '__module__') == '__builtin__',
            filepath_reject(mod, pylist),
            ]):
        return rval

    # print("nodoc_check(%s = %s)" % (mod.__name__, str(mod)))
    for name, item in inspect.getmembers(mod, inspect.isroutine):
        if all([not inspect.isbuiltin(item),
                not filepath_reject(item, pylist),
                name_accept(item.__name__, already)
                ]):
            if item.__doc__ is None:
                try:
                    filename = U.basename(mod.__file__)
                except AttributeError:
                    tmod = sys.modules[mod.__module__]
                    filename = U.basename(tmod.__file__)
                rval += "\n%3d. %s(%s): %s" % (count,
                                               filename,
                                               why,
                                               item.__name__)
                try:
                    count += 1
                except NameError:
                    count = 1
            already.append(":".join([mod.__name__, name]))

    for name, item in inspect.getmembers(mod, inspect.isclass):
        if all([item.__name__ not in already,
                depth < 5]):
            rval += nodoc_check(item, pylist, depth+1, 'c')
            already.append(item.__name__)

    for name, item in inspect.getmembers(mod, inspect.ismodule):
        if all([not inspect.isbuiltin(item),
                item.__name__ not in already,
                not name.startswith('@'),
                not name.startswith('_'),
                depth < 5]):
            rval += nodoc_check(item, pylist, depth+1, 'm')
            already.append(item.__name__)

    return rval
