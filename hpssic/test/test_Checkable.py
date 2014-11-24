#!/usr/bin/env python
"""
Tests for Checkable.py
"""
from hpssic.Checkable import Checkable
import copy
from hpssic import CrawlConfig
from hpssic import CrawlDBI
from hpssic import dbschem
from hpssic import Dimension
import os
import pdb
import pytest
import random
import stat
import sys
from hpssic import testhelp
import time
from hpssic import toolframe
import traceback as tb
from hpssic import util
from hpssic import util as U


# -----------------------------------------------------------------------------
def setUpModule():
    """
    Create the test directory in preparation to run the tests.
    """
    testhelp.module_test_setup(CheckableTest.testdir)


# -----------------------------------------------------------------------------
def tearDownModule():
    """
    Clean up the test directory after a test run.
    """
    testhelp.module_test_teardown(CheckableTest.testdir)


# -----------------------------------------------------------------------------
class CheckableTest(testhelp.HelpedTestCase):
    testdir = testhelp.testdata(__name__)
    testdb = '%s/test.db' % testdir
    methods = ['__init__', 'ex_nihilo', 'get_list', 'check', 'persist']
    testpath = '/home/tpb/TODO'
    testcfg = {'dbi-crawler': {'dbtype': 'sqlite',
                               'dbname': testdb,
                               'tbl_prefix': 'test'},
               'crawler': {'logpath': '%s/test.log' % (testdir)},
               'cv': {'fire': 'no'}
               }

    testdata = [('/', 'd', '', 0),
                ('/abc', 'd', '', 17),
                ('/xyz', 'f', '', 92),
                ('/abc/foo', 'f', '', 5),
                ('/abc/bar', 'f', '', time.time())]

    # -------------------------------------------------------------------------
    def populate_ttypes(self):
        """
        Create and populate table <pfx>_tape_types
        """
        dbschem.make_table("tape_types")
        db = CrawlDBI.DBI(dbtype='crawler')
        db.insert(table="tape_types",
                  fields=['type', 'subtype', 'name'],
                  data=[(16777235, 0, 'STK T10000/T10000A(500GB)'),
                        (16777235, 1, 'STK T10000/T10000B(1000GB)'),
                        (16777235, 2, 'STK T10000/T10000C(5000GB)'),
                        (16777235, 3, 'STK T10000/T10000D(8000GB)')])
        db.close()

    # -------------------------------------------------------------------------
    @pytest.mark.skipif(not pytest.config.getvalue("all"),
                        reason="slow -- use --all to run this one")
    @pytest.mark.skipif('jenkins' in os.getcwd())
    def test_check_dir(self):
        """
        Calling .check() on a directory should give us back a list of Checkable
        objects representing the entries in the directory
        """
        self.dbgfunc()
        util.conditional_rm(self.testdb)
        cfg = CrawlConfig.get_config('hpssic_sqlite_test.cfg', reset=True)
        cfg.set('crawler', 'logpath',
                U.pathjoin(self.testdir, util.my_name()) + ".log")
        cfg.set('dbi-crawler', 'dbname', U.pathjoin(self.testdir, 'test.db'))
        cfg.set('cv', 'fire', 'no')
        self.populate_ttypes()

        Checkable.ex_nihilo()
        testdir = '/home/tpb/hic_test'
        self.db_add_one(path=testdir, type='d')
        x = Checkable.get_list()

        self.expected(2, len(x))
        dirlist = x[1].check()
        if type(dirlist) == str and dirlist == "unavailable":
            return

        c = Checkable(path=testdir + '/crawler.tar', type='f')
        self.assertTrue(c in dirlist,
                        "expected to find %s in %s" % (c, dirlist))
        c = Checkable(path=testdir + '/crawler.tar.idx', type='f')
        self.assertTrue(c in dirlist,
                        "expected to find %s in %s" % (c, dirlist))
        c = Checkable(path=testdir + '/subdir1', type='d')
        self.assertTrue(c in dirlist,
                        "expected to find %s in %s" % (c, dirlist))
        c = Checkable(path=testdir + '/subdir2', type='d')
        self.assertTrue(c in dirlist,
                        "expected to find %s in %s" % (c, dirlist))

        for c in dirlist:
            if c.path == "%s/crawler.tar" % testdir:
                self.expected(0, c.checksum)
            elif c.path == "%s/crawler.tar.idx" % testdir:
                self.expected(0, c.checksum)
            elif c.path == "%s/subdir1" % testdir:
                self.expected(0, c.checksum)
            elif c.path == "%s/subdir2" % testdir:
                self.expected(0, c.checksum)

    # -------------------------------------------------------------------------
    @pytest.mark.skipif(not pytest.config.getvalue("all"),
                        reason="slow -- use --all to run this one")
    @pytest.mark.skipif('jenkins' in os.getcwd())
    def test_check_file(self):
        """
        Calling .check() on a file should execute the check actions for that
        file and update the item's last_check value.
        """
        if 'jenkins' in os.getcwd():
            raise SkipTest('HPSS not available on jenkins')
        util.conditional_rm(self.testdb)
        Dimension.get_dim('ignore', reset=True)
        CrawlConfig.add_config(close=True, dct=self.cfg_dict())
        Checkable.ex_nihilo()
        testdir = '/home/tpb/hic_test'
        self.db_add_one(path=testdir, type='d')
        self.db_add_one(path=testdir + '/crawler.tar', type='f')
        self.db_add_one(path=testdir + '/crawler.tar.idx', type='f')

        x = Checkable.get_list()
        checked = []
        for item in [z for z in x if z.type == 'f']:
            self.expected(0, item.last_check)
            result = item.check()
            if type(result) == str and result == "unavailable":
                return

        x = Checkable.get_list()
        for item in [z for z in x if z.type == 'f']:
            self.assertNotEqual(0, item.last_check,
                                "Expected last_check to be updated but " +
                                "it was not")

    # -------------------------------------------------------------------------
    def test_ctor(self):
        """
        Verify that the constructor gives us an object with the right methods
        and default attributes.
        """
        x = Checkable()
        for method in self.methods:
            self.assertEqual(method in dir(x), True,
                             "Checkable object is missing %s method" % method)
        self.expected('---', x.path)
        self.expected('-', x.type)
        self.expected(0, x.checksum)
        self.expected('', x.cos)
        self.expected(0, x.last_check)
        self.expected(0, x.fails)
        self.expected(None, x.rowid)
        self.expected(0.1, x.probability)

    # -------------------------------------------------------------------------
    def test_ctor_args(self):
        """
        Verify that the constructor accepts and sets rowid, path, type,
        cos, and last_check
        """
        x = Checkable(rowid=3, path='/one/two/three', type='f', cos='6002',
                      last_check=72, probability=0.01)
        for method in self.methods:
            self.assertEqual(method in dir(x), True,
                             "Checkable object is missing %s method" % method)
        self.expected(3, x.rowid)
        self.expected('/one/two/three', x.path)
        self.expected('f', x.type)
        self.expected('6002', x.cos)
        self.expected(72, x.last_check)
        self.expected(0.01, x.probability)
        self.expected(0, x.fails)

    # -------------------------------------------------------------------------
    def test_ctor_bad_args(self):
        """
        Verify that the constructor rejects invalid arguments
        """
        try:
            x = Checkable(path_x='/one/two/three', type='f', last_check=72)
            self.fail("Expected an exception but didn't get one.")
        except StandardError as e:
            self.assertEqual('Attribute path_x is invalid for Checkable'
                             in str(e), True,
                             "Got the wrong StandardError: %s" %
                             util.line_quote(tb.format_exc()))

    # -------------------------------------------------------------------------
    def test_eq(self):
        """
        Two (Checkable) objects should be equal iff both are instances of class
        Checkable and their path and type attributes are equal.
        """
        now = time.time()
        a = Checkable(rowid=92,
                      path='/foo/bar',
                      type='f',
                      cos='9283',
                      cart=None,
                      last_check=now)
        b = Checkable(rowid=97,
                      path='/foo/bar',
                      type='f',
                      cos='23743',
                      cart='',
                      last_check=now + 23)
        c = Checkable(rowid=43,
                      path='/foo/bar',
                      type='d',
                      cos='2843',
                      cart='X0352700',
                      last_check=now - 32)
        d = Checkable(rowid=18,
                      path='/foo/fiddle',
                      type='f',
                      cos='9222',
                      cart='',
                      last_check=now + 10132)
        e = lambda: None
        setattr(e, 'path', '/foo/bar')
        setattr(e, 'type', 'f')
        f = Checkable(rowid=49,
                      path='/foo/fiddle',
                      type='d',
                      cos='739',
                      cart=None,
                      last_check=now-19)
        self.assertEqual(a, b,
                         "'%s' and '%s' should be equal" % (a, b))
        self.assertNotEqual(a, c,
                            "'%s' and '%s' should not be equal" % (a, c))
        self.assertNotEqual(a, d,
                            "'%s' and '%s' should not be equal" % (a, d))
        self.assertNotEqual(a, e,
                            "'%s' and '%s' should not be equal" % (a, e))
        self.assertNotEqual(c, d,
                            "'%s' and '%s' should not be equal" % (c, d))
        self.assertNotEqual(c, e,
                            "'%s' and '%s' should not be equal" % (c, e))
        self.assertNotEqual(d, e,
                            "'%s' and '%s' should not be equal" % (d, e))
        self.assertNotEqual(e, f,
                            "'%s' and '%s' should not be equal" % (e, f))

    # -------------------------------------------------------------------------
    def test_ex_nihilo_drspec(self):
        """
        If the database file does not already exist, calling ex_nihilo() should
        create it. For this test, we specify and verify a dataroot value.
        """
        # make sure the .db file does not exist
        util.conditional_rm(self.testdb)
        testhelp.db_config(self.testdir, util.my_name())

        # this call should create it
        Checkable.ex_nihilo(dataroot="/home/somebody")

        # check that it exists
        self.assertEqual(os.path.exists(self.testdb), True,
                         "File '%s' should be created by ex_nihilo()" %
                         (self.testdb))

        # assuming it does, look inside and make sure the checkables table got
        # initialized correctly
        db = CrawlDBI.DBI(dbtype='crawler')

        # there should be one row
        rows = db.select(table='checkables', fields=['rowid',
                                                     'path',
                                                     'type',
                                                     'cos',
                                                     'cart',
                                                     'checksum',
                                                     'last_check',
                                                     'fails',
                                                     ])
        self.expected(1, len(rows))

        # the one row should reference the root directory
        [(max_id,)] = db.select(table='checkables', fields=['max(rowid)'])
        self.expected(1, max_id)                       # id
        self.expected('/home/somebody', rows[0][1])    # path
        self.expected('d', rows[0][2])                 # type
        self.expected('', rows[0][3])                  # cos
        self.expected(None, rows[0][4])                # cart
        self.expected(0, rows[0][5])                   # checksum
        self.expected(0, rows[0][6])                   # last_check
        self.expected(0, rows[0][7])                   # fails
        self.dbgfunc()

    # -------------------------------------------------------------------------
    @pytest.mark.skipif(not pytest.config.getvalue("all"),
                        reason="slow -- use --all to run this one")
    def test_ex_nihilo_exist(self):
        """
        If the database file and the checkables table already exists, calling
        ex_nihilo() should do nothing.

        The current behavior is that if an existing (non database) file is
        named as the database, no attempt will be made to create the tables.
        This is desirable. We don't want to create the tables unless we created
        the file. Otherwise, the user might overwrite a file inadvertently by
        trying to treat it as a database.

        Note: the use of unacceptable database files is tested in CrawlDBI.py.
        """
        # make sure the .db file does not exist
        util.conditional_rm(self.testdb)
        CrawlConfig.add_config(close=True, dct=self.cfg_dict())

        # create a dummy .db file and set its mtime back by 500 seconds
        util.touch(self.testdb)
        s = os.stat(self.testdb)
        newtime = s[stat.ST_MTIME] - 500
        os.utime(self.testdb, (s[stat.ST_ATIME], newtime))

        # create a dummy 'checkables' table in the test database
        Checkable.ex_nihilo()
        pre = os.stat(self.testdb)

        # call the test target routine
        time.sleep(1.0)
        Checkable.ex_nihilo()

        # verify that the file's mtime is unchanged and its size is unchanged
        post = os.stat(self.testdb)
        self.expected(util.ymdhms(pre[stat.ST_MTIME]),
                      util.ymdhms(post[stat.ST_MTIME]))
        self.expected(pre[stat.ST_SIZE], post[stat.ST_SIZE])

    # -------------------------------------------------------------------------
    def test_ex_nihilo_notable(self):
        """
        If the database file does already exist and the checkables table does
        not, calling ex_nihilo() should create the table.

        The current behavior is that if an existing (non database) file is
        named as the database, no attempt will be made to create the tables.
        This is desirable. We don't want to create the tables unless we created
        the file. Otherwise, the user might overwrite a file inadvertently by
        trying to treat it as a database.

        Note: the use of unacceptable database files is tested in CrawlDBI.py.
        """
        # make sure the .db file does not exist
        util.conditional_rm(self.testdb)
        CrawlConfig.add_config(close=True, dct=self.cfg_dict())

        # create a dummy .db file and set its mtime back by 500 seconds
        util.touch(self.testdb)
        s = os.stat(self.testdb)
        newtime = s[stat.ST_MTIME] - 500
        os.utime(self.testdb, (s[stat.ST_ATIME], newtime))

        # call the test target routine
        Checkable.ex_nihilo()

        # verify that the file exists and the table does also
        self.assertTrue(os.path.exists(self.testdb),
                        "Expected %s to exist" % self.testdb)
        db = CrawlDBI.DBI(dbtype='crawler')
        self.assertTrue(db.table_exists(table='checkables'),
                        "Expected table 'checkables' to exist in db")

    # -------------------------------------------------------------------------
    def test_ex_nihilo_scratch(self):
        """
        If the database file does not already exist, calling ex_nihilo() should
        create it.
        """
        # make sure the .db file does not exist
        util.conditional_rm(self.testdb)
        testhelp.db_config(self.testdir, util.my_name())

        # this call should create it
        Checkable.ex_nihilo()

        # check that it exists
        self.assertEqual(os.path.exists(self.testdb), True,
                         "File '%s' should be created by ex_nihilo()" %
                         (self.testdb))

        # assuming it does, look inside and make sure the checkables table got
        # initialized correctly
        db = CrawlDBI.DBI(dbtype='crawler')

        # there should be one row
        rows = db.select(table='checkables', fields=['rowid',
                                                     'path',
                                                     'type',
                                                     'cos',
                                                     'cart',
                                                     'checksum',
                                                     'last_check',
                                                     'fails',
                                                     ])
        self.expected(1, len(rows))

        # the one row should reference the root directory
        [(max_id, )] = db.select(table='checkables', fields=['max(rowid)'])
        self.expected(1, max_id)           # id
        self.expected('/', rows[0][1])     # path
        self.expected('d', rows[0][2])     # type
        self.expected('', rows[0][3])      # cos
        self.expected(None, rows[0][4])    # cart
        self.expected(0, rows[0][5])       # checksum
        self.expected(0, rows[0][6])       # last_check
        self.expected(0, rows[0][7])       # fails

    # -------------------------------------------------------------------------
    def test_ex_nihilo_rootlist(self):
        """
        Method ex_nihilo() must be able to take a list in its dataroot argument
        """
        # make sure the .db file does not exist
        util.conditional_rm(self.testdb)
        testhelp.db_config(self.testdir, util.my_name())

        # this call should create it
        Checkable.ex_nihilo(dataroot=['abc', 'def'])

        # check that it exists
        self.assertEqual(os.path.exists(self.testdb), True,
                         "File '%s' should be created by ex_nihilo()" %
                         (self.testdb))

        # assuming it does, look inside and make sure the checkables table got
        # initialized correctly
        db = CrawlDBI.DBI(dbtype='crawler')

        # there should be two rows, one for each item in the dataroot list
        rows = db.select(table='checkables', fields=['rowid',
                                                     'path',
                                                     'type',
                                                     'cos',
                                                     'cart',
                                                     'checksum',
                                                     'last_check',
                                                     'fails',
                                                     ])
        self.expected(2, len(rows))

        self.expected('abc', rows[0][1])
        self.expected('d', rows[0][2])

        self.expected('def', rows[1][1])
        self.expected('d', rows[1][2])

    # -------------------------------------------------------------------------
    def test_fdparse_ldr(self):
        """
        Parse an ls -l line from hsi where we're looking at a directory with a
        recent date (no year). fdparse() should return type='d', path=<file
        name>.
        """
        n = Checkable(path='xyx', type='d')
        line = ('drwx------    2 tpb       ccsstaff         ' +
                '512 Oct 17 13:54 subdir1')
        r = n.fdparse(line)
        self.expected('d', r.type)
        self.expected('subdir1', r.path)
        self.expected('', r.cos)
        self.expected(None, r.cart)
        self.assertTrue(isinstance(r, Checkable),
                        "Expected Checkable(), got %s" % r)

    # -------------------------------------------------------------------------
    def test_fdparse_ldy(self):
        """
        Parse an ls -l line from hsi where we're looking at a directory with a
        year in the date. fdparse() should return type='d', path=<file name>.
        """
        n = Checkable(path='xyx', type='d')
        line = ('drwxr-xr-x    2 tpb       ccsstaff         ' +
                '512 Dec 17  2004 incase')
        r = n.fdparse(line)
        self.expected('d', r.type)
        self.expected('incase', r.path)
        self.expected('', r.cos)
        self.assertTrue(isinstance(r, Checkable),
                        "Expected Checkable(), got %s" % r)

    # -------------------------------------------------------------------------
    def test_fdparse_lfr(self):
        """
        Parse an ls -l line from hsi where we're looking at a file with a
        recent date (no year). fdparse() should return type='f', path=<file
        name>.
        """
        n = Checkable(path='xyx', type='d')
        line = ('-rw-------    1 tpb       ccsstaff     ' +
                '1720832 Oct 17 13:56 crawler.tar')
        r = n.fdparse(line)
        self.expected('f', r.type)
        self.expected('crawler.tar', r.path)
        self.expected('', r.cos)
        self.assertTrue(isinstance(r, Checkable),
                        "Expected Checkable, got %s" % r)

    # -------------------------------------------------------------------------
    def test_fdparse_lfy(self):
        """
        Parse an ls -X line from hsi where we're looking at a file with a year
        in the date. fdparse() should return type='f', path=<file name>.
        """
        n = Checkable(path='xyx', type='d')
        line = ('-rw-------    1 tpb       ccsstaff        4896' +
                ' Dec 30  2011 pytest.tar.idx')
        r = n.fdparse(line)
        self.expected('f', r.type)
        self.expected('pytest.tar.idx', r.path)
        self.expected('', r.cos)
        self.assertTrue(isinstance(r, Checkable),
                        "Expected Checkable, got %s" % r)

    # -------------------------------------------------------------------------
    def test_fdparse_nomatch(self):
        """
        Parse an ls -X line from hsi that does not describe a file or directory
        in the date. fdparse() should return None.
        """
        n = Checkable(path='xyx', type='d')
        line = '/home/tpb/cli_test:'
        z = n.fdparse(line)
        self.expected(None, z)

    # -------------------------------------------------------------------------
    def test_fdparse_Pd(self):
        """
        Parse an ls -P line from hsi where we're looking at a directory. The -P
        format doesn't provide date or cos for directories.

        fdparse() should return type='d', path=<file path>
        """
        self.dbgfunc()
        n = Checkable(path='xyx', type='d')
        line = "DIRECTORY\t       /home/tpb/apache"
        r = n.fdparse(line)
        self.expected('d', r.type)
        self.expected('/home/tpb/apache', r.path)
        self.expected('', r.cos)
        self.expected(None, r.cart)
        self.assertTrue(isinstance(r, Checkable),
                        "Expected Checkable(), got %s" % r)

    # -------------------------------------------------------------------------
    def test_fdparse_Pf(self):
        """
        Parse an ls -P line from hsi where we're looking at a file. fdparse()
        should return type='f', path=<file path>, cos.
        """
        n = Checkable(path='xyx', type='d')
        line = ("FILE\t /home/tpb/LoadL_admin\t 88787\t   88787\t   " +
                "3962+411820\t X0352700\t 5081\t 0\t 1\t       " +
                "03/14/2003\t  07:12:43\t 03/19/2012\t 13:09:50")
        r = n.fdparse(line)
        self.expected('f', r.type)
        self.expected('/home/tpb/LoadL_admin', r.path)
        self.expected('5081', r.cos)
        self.expected('X0352700', r.cart)
        self.assertTrue(isinstance(r, Checkable),
                        "Expected Checkable(), got %s" % r)

    # -------------------------------------------------------------------------
    def test_fdparse_Pf0(self):
        """
        Parse an ls -P line from hsi where we're looking at a zero length file
        . fdparse() should return type='f', path=<file path>, cos.
        """
        n = Checkable(path='xyx', type='d')
        line = ("FILE\t/log/2007/05/15/logfile01_200705150306\t 0\t 0\t   " +
                "0\t        \t              6001\t    0\t       1\t  " +
                "05/15/2007\t   03:06:39\t  02/11/2009\t  11:06:31")
        r = n.fdparse(line)
        self.expected('f', r.type)
        self.expected('/log/2007/05/15/logfile01_200705150306', r.path)
        self.expected('6001', r.cos)
        self.expected('', r.cart)
        self.assertTrue(isinstance(r, Checkable),
                        "Expected Checkable(), got %s" % r)

    # -------------------------------------------------------------------------
    def test_get_list_nosuch(self):
        """
        Calling .get_list() before .ex_nihilo() should cause an exception
        """
        util.conditional_rm(self.testdb)
        CrawlConfig.add_config(close=True, dct=self.cfg_dict())

        try:
            Checkable.get_list()
            self.fail("Expected an exception but didn't get one.")
        except CrawlDBI.DBIerror as e:
            self.assertEqual("no such table: test_checkables" in str(e), True,
                             "Got the wrong DBIerror: %s" %
                             util.line_quote(tb.format_exc()))

    # -------------------------------------------------------------------------
    def test_get_list_known(self):
        """
        Calling .get_list() should give us back a list of Checkable objects
        representing what is in the table
        """
        # make sure the .db file does not exist
        util.conditional_rm(self.testdb)
        CrawlConfig.add_config(close=True, dct=self.cfg_dict())

        # create some test data (path, type, cos, last_check)
        tdcopy = copy.deepcopy(self.testdata)

        # testdata has to be sorted by last_check since that's the way get_list
        # will order the list it returns
        tdcopy.sort(key=lambda x: x[3])

        # create the .db file
        Checkable.ex_nihilo()

        # put the test data into the database
        db = CrawlDBI.DBI(dbtype='crawler')
        db.insert(table='checkables',
                  fields=['path', 'type', 'cos', 'last_check'],
                  data=tdcopy[1:])
        db.close()

        # run the target routine
        x = Checkable.get_list()

        # we should have gotten back the same number of records as went into
        # the database
        self.expected(len(tdcopy), len(x))

        # verify that the data from the database matches the testdata that was
        # inserted
        for idx, item in enumerate(x):
            self.expected(tdcopy[idx][0], item.path)
            self.expected(tdcopy[idx][1], item.type)
            self.expected(tdcopy[idx][2], item.cos)
            self.expected(tdcopy[idx][3], item.last_check)

    # -------------------------------------------------------------------------
    def test_get_list_newroot(self):
        """
        Calling .get_list() when a new item has been added to dataroot in the
        config should add the new dataroot before giving us back a list of
        Checkable objects representing what is in the table

        This test succeeding depends upon the select in get_list() ordering the
        objects returned by pathname within last_check. Both '/' and 'newroot'
        are going to have a last_check time of 0. The testdata list assumes '/'
        comes first. As long as the select fulfills that assumption, we're
        fine.
        """
        # make sure the .db file does not exist
        util.conditional_rm(self.testdb)
        CrawlConfig.add_config(close=True, dct=self.cfg_dict())

        # create some test data (path, type, cos, last_check)
        tdcopy = copy.deepcopy(self.testdata)

        nrpath = '/newroot'
        nrtup = (nrpath, 'd', '', 0)

        # create the .db file
        Checkable.ex_nihilo()

        # put the test data into the database (but not newroot)
        db = CrawlDBI.DBI(dbtype='crawler')
        db.insert(table='checkables',
                  fields=['path', 'type', 'cos', 'last_check'],
                  data=tdcopy[1:])
        db.close()

        # run the target routine
        x = Checkable.get_list(rootlist=['/abc', nrpath])

        # we should have gotten back the same number of records as went into
        # the database plus 1 for the new root
        tdcopy.append(nrtup)

        # testdata has to be sorted by last_check since that's the way get_list
        # will order the list it returns
        tdcopy.sort(key=lambda x: x[3])

        self.expected(len(tdcopy), len(x))

        # verify that the data from the database matches the testdata that was
        # inserted
        for idx, item in enumerate(x):
            self.expected(tdcopy[idx][0], item.path)
            self.expected(tdcopy[idx][1], item.type)
            self.expected(tdcopy[idx][2], item.cos)
            self.expected(tdcopy[idx][3], item.last_check)

    # -------------------------------------------------------------------------
    def test_get_list_priority(self):
        """
        Calling .get_list() when a priority file is in place should give back
        the list with the content of the priority file(s) at the top.
        """
        # set up data, filenames, etc
        pri_pending = U.pathjoin(self.testdir, 'pending')
        self.dbgfunc()
        pri_glob = U.pathjoin(pri_pending, '*')
        pri_complete = U.pathjoin(self.testdir, 'completed')
        pri_d = [{'ppath': U.pathjoin(pri_pending, 'test1'),
                  'cpath': U.pathjoin(pri_complete, 'test1'),
                  'data': ['/this/should/come/first',
                           '/this/should/come/second']
                  },
                 {'ppath': U.pathjoin(pri_pending, 'Test2'),
                  'cpath': U.pathjoin(pri_complete, 'Test2'),
                  'data': ['/this/should/come/third',
                           '/this/should/come/fourth']
                  }
                 ]
        explist = pri_d[0]['data'] + pri_d[1]['data']
        explist.append(self.testdata[0])
        explist.append(self.testdata[3])
        explist.append(self.testdata[1])

        # write out the config file with info about the priority files
        cfg = copy.deepcopy(self.testcfg)
        cfg['cv']['priority'] = pri_glob
        cfg['cv']['completed'] = pri_complete
        testhelp.db_config(self.testdir, U.my_name(), cfg_d=cfg)

        # initialize the database from scratch with some known testdata
        U.conditional_rm(self.testdb)
        Checkable.ex_nihilo()
        db = CrawlDBI.DBI(dbtype='crawler')
        db.insert(table='checkables',
                  fields=['path', 'type', 'cos', 'last_check', ],
                  data=self.testdata[1:])
        db.close()

        # write out some priority files
        os.mkdir(pri_pending)
        os.mkdir(pri_complete)
        for z in pri_d:
            testhelp.write_file(z['ppath'], content=z['data'])

        # run get_list
        x = Checkable.get_list()

        # verify that the priority file contents are at the top of the list
        for exp in explist:
            if type(exp) == tuple:
                self.expected(Checkable(path=exp[0], type=exp[1]), U.pop0(x))
            else:
                self.expected(Checkable(path=exp, type='f'), U.pop0(x))

        for z in pri_d:
            self.assertFalse(os.path.exists(z['ppath']),
                             "%s should have been moved to %s"
                             % (z['ppath'], z['cpath']))
            self.assertTrue(os.path.exists(z['cpath']),
                            "%s should have been moved from %s"
                            % (z['cpath'], z['ppath']))

    # -------------------------------------------------------------------------
    def test_persist_last_check(self):
        """
        Verify that last_check gets stored by persist().
        """
        CrawlConfig.add_config(close=True, dct=self.cfg_dict())
        testpath = 'Checkable.py'
        Checkable.ex_nihilo(dataroot=testpath)

        when = time.time()

        x = Checkable.get_list()
        x[0].set('last_check', when)
        x[0].persist()

        y = Checkable.get_list()
        self.expected(when, y[0].last_check)

    # -------------------------------------------------------------------------
    def test_persist_dir_duplicate(self):
        """
        Send in a new directory with path matching a duplicate in database
        (rowid == None, last_check == 0, type == 'd'). Exception should be
        thrown.
        """
        util.conditional_rm(self.testdb)
        self.dbgfunc()
        CrawlConfig.add_config(close=True, dct=self.cfg_dict())
        Checkable.ex_nihilo()
        self.db_duplicates()
        x = Checkable.get_list()
        self.expected(3, len(x))

        foo = Checkable(path='/abc/def', type='d')
        self.assertRaisesMsg(StandardError,
                             "There appears to be more than one",
                             foo.load)

        x = Checkable.get_list()
        self.expected(3, len(x))
        self.assertEqual(foo in x, True,
                         "Object foo not found in database")
        root = Checkable(path='/', type='d')
        self.assertEqual(root in x, True,
                         "Object root not found in database")

    # -------------------------------------------------------------------------
    def test_persist_dir_new(self):
        """
        Send in a new directory (rowid == None, last_check == 0, type == 'd',
        path does not match). New record should be added.
        """
        util.conditional_rm(self.testdb)
        CrawlConfig.add_config(close=True, dct=self.cfg_dict())
        Checkable.ex_nihilo()
        x = Checkable.get_list()
        self.expected(1, len(x))

        foo = Checkable(path='/abc/def', type='d')
        foo.persist()

        x = Checkable.get_list()
        self.expected(2, len(x))
        self.assertEqual(foo in x, True,
                         "Object foo not found in database")
        root = Checkable(path='/', type='d')
        self.assertEqual(root in x, True,
                         "Object root not found in database")

    # -------------------------------------------------------------------------
    def test_persist_dir_exist_dd(self):
        """
        Send in a new directory with matching path (rowid == None, last_check
        == 0, type == 'd'). Existing path should not be updated.
        """
        util.conditional_rm(self.testdb)
        CrawlConfig.add_config(close=True, dct=self.cfg_dict())
        Checkable.ex_nihilo()

        now = time.time()
        self.db_add_one(path=self.testpath, type='d', last_check=now)

        x = Checkable.get_list()
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected(now, x[1].last_check)
        self.expected('', x[1].cos)

        x[1].last_check = 0
        x[1].rowid = None
        x[1].persist()

        x = Checkable.get_list()
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected('d', x[1].type)
        self.expected(now, x[1].last_check)
        self.expected('', x[1].cos)

    # -------------------------------------------------------------------------
    def test_persist_dir_exist_fd(self):
        """
        Send in a new directory with matching path (rowid == None, last_check
        == 0, type == 'f'), changing type (f -> d). Existing path should be
        updated.
        """
        util.conditional_rm(self.testdb)
        CrawlConfig.add_config(close=True, dct=self.cfg_dict())
        Checkable.ex_nihilo()

        now = time.time()
        self.db_add_one(path=self.testpath, type='f',
                        cos='1234', last_check=now)

        x = Checkable.get_list()
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected(now, x[1].last_check)
        self.expected('1234', x[1].cos)

        x[1].set('last_check', 0)
        x[1].set('cos', '')
        x[1].set('rowid', None)
        x[1].set('type', 'd')
        x[1].persist()

        x = Checkable.get_list()
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected('d', x[1].type)
        self.expected(0, x[1].last_check)
        self.expected('', x[1].cos)

    # -------------------------------------------------------------------------
    def test_persist_dir_invalid(self):
        """
        Send in an invalid directory (rowid == None, last_check != 0, type ==
        'd'). Exception should be thrown.
        """
        util.conditional_rm(self.testdb)
        CrawlConfig.add_config(close=True, dct=self.cfg_dict())
        Checkable.ex_nihilo()
        t1 = time.time()
        self.db_add_one(path='/home', type='d', last_check=t1)

        x = Checkable.get_list()

        self.expected(2, len(x))
        c = Checkable(path='/', type='d')
        self.assertTrue(c in x, "expected to find '%s' in '%s'" % (c, x))
        c = Checkable(path='/home', type='d')
        self.assertTrue(c in x, "expected to find '%s' in '%s'" % (c, x))

        x[0].rowid = None
        x[0].last_check = t0 = time.time()

        try:
            x[0].persist()
            self.fail("Expected an exception but didn't get one.")
        except StandardError as e:
            self.assertEqual("has rowid == None, last_check != 0.0" in str(e),
                             True,
                             "Got the wrong StandardError: %s" %
                             util.line_quote(tb.format_exc()))

        x = Checkable.get_list()
        self.expected(2, len(x))
        c = Checkable(path='/', type='d')
        self.assertTrue(c in x, "expected to find '%s' in '%s'" % (c, x))
        c = Checkable(path='/home', type='d')
        self.assertTrue(c in x, "expected to find '%s' in '%s'" % (c, x))
        self.expected(util.ymdhms(t1), util.ymdhms(x[1].last_check))

    # -------------------------------------------------------------------------
    def test_persist_dir_update(self):
        """
        Send in an existing directory with a new last_check time (rowid !=
        None, path exists, type == 'd', last_check changed). Last check time
        should be updated.
        """
        util.conditional_rm(self.testdb)
        CrawlConfig.add_config(close=True, dct=self.cfg_dict())
        Checkable.ex_nihilo()

        x = Checkable.get_list()
        self.expected(1, len(x))
        self.expected(0, x[0].last_check)

        now = time.time()
        x[0].set('last_check', now)
        x[0].persist()

        x = Checkable.get_list()
        self.expected(1, len(x))
        self.expected('/', x[0].path)
        self.expected('d', x[0].type)
        self.expected(now, x[0].last_check)

    # -------------------------------------------------------------------------
    def test_persist_file_duplicate(self):
        """
        Send in a new file with path matching a duplicate in database (rowid ==
        None, last_check == 0, type == 'f'). Exception should be thrown.
        """
        util.conditional_rm(self.testdb)
        CrawlConfig.add_config(close=True, dct=self.cfg_dict())
        Checkable.ex_nihilo()
        self.db_add_one(path=self.testpath, type='f')
        self.db_add_one(path=self.testpath, type='f')

        x = Checkable.get_list()
        self.expected(3, len(x))
        self.assertEqual(x[1], x[2],
                         "There should be a duplicate entry in the database.")

        foo = Checkable(path=self.testpath, type='f')
        self.assertRaisesMsg(StandardError,
                             "There appears to be more than one",
                             foo.load)

        x = Checkable.get_list()
        self.expected(3, len(x))
        self.assertEqual(foo in x, True,
                         "Object foo not found in database")
        root = Checkable(path='/', type='d')
        self.assertEqual(root in x, True,
                         "Object root not found in database")
        self.assertEqual(x[1], x[2],
                         "There should be a duplicate entry in the database.")

    # -------------------------------------------------------------------------
    def test_persist_file_new(self):
        """
        Send in a new file (rowid == None, last_check == 0, path does not
        match, type == 'f'). New record should be added.
        """
        util.conditional_rm(self.testdb)
        CrawlConfig.add_config(close=True, dct=self.cfg_dict())
        Checkable.ex_nihilo()

        x = Checkable.get_list()
        self.expected(1, len(x))
        self.expected("/", x[0].path)
        self.expected(0, x[0].last_check)

        foo = Checkable(path=self.testpath, type='f')
        foo.persist()

        x = Checkable.get_list()
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected(0, x[1].last_check)

    # -------------------------------------------------------------------------
    def test_persist_file_exist_df(self):
        """
        Send in a new file with matching path (rowid == None, last_check
        == 0, type == 'f'). Existing path should be updated.
        """
        util.conditional_rm(self.testdb)
        CrawlConfig.add_config(close=True, dct=self.cfg_dict())
        Checkable.ex_nihilo()
        now = time.time()
        self.db_add_one(last_check=now, type='d')
        x = Checkable.get_list()
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected(now, x[1].last_check)

        x[1].set('last_check', 0)
        x[1].set('cos', '1234')
        x[1].set('checksum', 1)
        x[1].set('rowid', None)
        x[1].set('type', 'f')
        x[1].persist()

        x = Checkable.get_list()
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected('f', x[1].type)
        self.expected(1, x[1].checksum)
        self.expected(0, x[1].last_check)
        self.expected('1234', x[1].cos)

    # -------------------------------------------------------------------------
    def test_persist_file_exist_ff(self):
        """
        Send in a new file with matching path (rowid == None, last_check
        == 0, type == 'f'). Existing path should not be updated.

        !@! last_check should not be persisted when it's already set and type
        doesn't change
        """
        util.conditional_rm(self.testdb)
        CrawlConfig.add_config(close=True, dct=self.cfg_dict())
        Checkable.ex_nihilo()
        now = time.time()
        self.db_add_one(last_check=now, type='f', cos='1111')
        x = Checkable.get_list()
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected(now, x[1].last_check)

        x[1].set('last_check', 0)
        x[1].set('cos', '2222')
        x[1].set('rowid', None)
        x[1].persist()

        x = Checkable.get_list()
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected('f', x[1].type)
        self.expected(0, x[1].last_check)
        self.expected('2222', x[1].cos)

    # -------------------------------------------------------------------------
    def test_persist_file_ok(self):
        """
        Send in a (formerly invalid) file (rowid != None, last_check == 0,
        type == 'f') No exception should be thrown.
        """
        util.conditional_rm(self.testdb)
        CrawlConfig.add_config(close=True, dct=self.cfg_dict())
        Checkable.ex_nihilo()
        self.db_add_one()
        x = Checkable.get_list()
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected(0, x[1].last_check)
        x[1].persist()

        x = Checkable.get_list()
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected(0, x[1].last_check)

    # -------------------------------------------------------------------------
    def test_persist_file_x(self):
        """
        Send in an existing file with a new last_check time (rowid != None,
        path exists, type == 'f', last_check changed). Last check time should
        be updated.
        """
        util.conditional_rm(self.testdb)
        CrawlConfig.add_config(close=True, dct=self.cfg_dict())
        Checkable.ex_nihilo()
        self.db_add_one()
        x = Checkable.get_list()
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected(0, x[1].last_check)

        now = time.time()
        x[1].set('last_check', now)
        x[1].persist()

        x = Checkable.get_list()
        self.expected(2, len(x))
        self.expected(util.ymdhms(now), util.ymdhms(x[1].last_check))

    # -------------------------------------------------------------------------
    def test_repr(self):
        """
        Verify the output of the __repr__() method
        """
        now = time.time()
        exp = ("Checkable(rowid=17, " +
               "path='/abc/def', " +
               "type='d', " +
               "cos='9999', " +
               "cart=None, " +
               "checksum=0, " +
               "last_check=%f)" % now)

        x = eval(exp)
        self.expected(exp, x.__repr__())

    # -------------------------------------------------------------------------
    def db_add_one(self,
                   path=testpath,
                   type='f',
                   cos='',
                   last_check=0):
        """
        Add one record to the database. All arguments except self are optional.
        """
        db = CrawlDBI.DBI(dbtype='crawler')
        db.insert(table='checkables',
                  fields=['path', 'type', 'cos', 'last_check'],
                  data=[(path, type, cos, last_check)])
        db.close()

    # -------------------------------------------------------------------------
    def db_duplicates(self):
        """
        Store a duplicate entry in the file table.
        """
        db = CrawlDBI.DBI(dbtype='crawler')
        db.insert(table='checkables',
                  fields=['path', 'type', 'cos', 'last_check'],
                  data=[('/abc/def', 'd', '', 0)])
        db.insert(table='checkables',
                  fields=['path', 'type', 'cos', 'last_check'],
                  data=[('/abc/def', 'd', '', 0)])
        db.close()


# -----------------------------------------------------------------------------
def fuzztime(days, cfg=None):
    """
    Get a random time between 0:00am and current time on the day indicated
    """
    base = int(U.day_offset(days))        # 0:00am on the day desired
    today = int(U.daybase(time.time()))   # 0:00am today
    offs = int(time.time()) - today       # secs since 0:00
    top = base + offs - 10   # 10 sec before current tod on target day
    rval = random.randrange(base, top)
    CrawlConfig.log("last_check = %s (%d); top = %s (%d)",
                    U.ymdhms(rval),
                    rval,
                    U.ymdhms(top),
                    top)
    return rval


# -----------------------------------------------------------------------------
class test_get_list(testhelp.HelpedTestCase):
    # test directory and database name
    testdir = testhelp.testdata(__name__)
    testdb = U.pathjoin(testdir, 'test.db')

    # these fields don't change
    rtype = 'f'
    cos = '6002'
    cart = 'X0352700'
    ttypes = 'STK T10000/T10000A(500GB)'
    fails = reported = 0

    fld_list = ['path',
                'type',
                'cos',
                'cart',
                'ttypes',
                'checksum',
                'last_check',
                'fails',
                'reported',
                ]

    cfg = {'crawler':
           {
               'exitpath': '/tmp/foobar',
               'logpath':  U.pathjoin(testdir, 'hpssic-getlist-test.log'),
           },

           'cv':
           {
               'recheck_fraction': '0.3',
               'recheck_age': '1d',
               'operations': '10',
           },

           'dbi-crawler':
           {
               'dbtype': 'sqlite',
               'dbname': testdb,
               'tbl_prefix': 'test',
           }
          }

    # changing fields: path, checksum, last_check
    testdata = [("/home/tpb/hic_test/test_001", 0, 0),
                ("/home/tpb/hic_test/test_002", 0, 0),
                ("/home/tpb/hic_test/test_003", 0, 0),
                ("/home/tpb/hic_test/test_004", 0, 0),
                ("/home/tpb/hic_test/test_005", 0, 0),
                ("/home/tpb/hic_test/test_006", 0, 0),
                ("/home/tpb/hic_test/test_007", 0, 0),
                ("/home/tpb/hic_test/test_008", 0, 0),
                ("/home/tpb/hic_test/test_009", 0, 0),
                ("/home/tpb/hic_test/test_010", 0, 0),
                ("/home/tpb/hic_test/test_011", 0, 0),
                ("/home/tpb/hic_test/test_012", 0, 0),

                ("/home/tpb/hic_test/test_100", 1, fuzztime(-1, cfg=cfg)),
                ("/home/tpb/hic_test/test_101", 1, fuzztime(-1)),
                ("/home/tpb/hic_test/test_102", 1, fuzztime(-1)),
                ("/home/tpb/hic_test/test_103", 1, fuzztime(-1)),
                ("/home/tpb/hic_test/test_104", 1, fuzztime(-2)),
                ("/home/tpb/hic_test/test_105", 1, fuzztime(-2)),
                ("/home/tpb/hic_test/test_106", 1, fuzztime(-2)),
                ("/home/tpb/hic_test/test_107", 1, fuzztime(-3)),
                ("/home/tpb/hic_test/test_108", 1, fuzztime(-3)),
                ("/home/tpb/hic_test/test_109", 1, fuzztime(-4)),
                ("/home/tpb/hic_test/test_110", 1, fuzztime(-5)),
                ]

    # -------------------------------------------------------------------------
    @classmethod
    def setUpClass(cls):
        """
        Set up the default config for all the tests in this class
        """
        cfg = CrawlConfig.add_config(close=True, dct=cls.cfg)

    # -------------------------------------------------------------------------
    def setUp(self):
        """
        The default config should be pointing us to an sqlite database.

        Drop the checkables table if it exists, then populate it with our test
        data.
        """
        super(test_get_list, self).setUp()
        cfg = CrawlConfig.add_config()

        db = CrawlDBI.DBI(dbtype='crawler')
        self.assertTrue(isinstance(db._dbobj, CrawlDBI.DBIsqlite),
                        "Expected an SQLITE db object, got %s" %
                        type(db._dbobj))

        if db.table_exists(table="checkables"):
            dbschem.drop_table(table='checkables', cfg=cfg)
        dbschem.make_table('checkables', cfg=cfg)

        fulldata = [(x[0],
                     self.rtype,
                     self.cos,
                     self.cart,
                     self.ttypes,
                     x[1],
                     x[2],
                     self.fails,
                     self.reported)
                    for x in self.testdata]
        db.insert(table='checkables',
                  fields=self.fld_list,
                  data=fulldata)
        db.close()

    # -------------------------------------------------------------------------
    def tearDown(self):
        """
        Drop the checkables table to clear the way for the next test.
        """
        if not testhelp.keepfiles():
            dbschem.drop_table(table='checkables')

    # -------------------------------------------------------------------------
    def check_result(self,
                     list,
                     len_expected=0,
                     rechecks_expected=0,
                     zeros_expected=0):
        """
        What get_list() returns is:
         - rechecks forced by out of date last_checks, if any
         - records with last_check == 0
         - records with last_check <> 0, in monotonic order
        After running get_list(), if there are no rechecks forced by out of
        date last checks, all last_checks and checksums in the list will be 0.
        """
        # validate the length of the list
        self.expected(len_expected, len(list))

        # validate the correct number of rechecks at the front
        for idx in range(rechecks_expected):
            item = U.pop0(list)
            self.expected(1, item.checksum)
            self.unexpected(0, item.last_check)
            item.last_check = int(time.time())
            item.persist(dirty=True)

        # validate the correct number of zero records in the middle
        for idx in range(zeros_expected):
            item = U.pop0(list)
            self.expected(0, item.checksum)
            self.expected(0, item.last_check)
            item.last_check = int(time.time())
            item.persist(dirty=True)

        # for the rest, last_check should rise monotonically
        prev = 0
        for item in list:
            self.assertTrue(prev <= item.last_check,
                            "Expected %d to be less than %d" %
                            (prev, item.last_check))
            self.unexpected(0, item.last_check)
            self.expected(1, item.checksum)
            prev = int(item.last_check)

            item.last_check = int(time.time())
            item.persist(dirty=True)

    # -------------------------------------------------------------------------
    def test_recheck_0_0__1d(self):
        """
        recheck_fraction = 0.0
        recheck_age = 1d
        ==> all 10 from last_check = 0 (lc=0)
        ==> 2 from lc = 0, 8 ordered by last_check (oblc)
        """
        self.dbgfunc()
        xcfg = copy.deepcopy(self.cfg)
        xcfg['cv']['recheck_fraction'] = '0.0'

        cfg = CrawlConfig.add_config(close=True, dct=xcfg)
        ops = int(cfg.get('cv', 'operations'))

        z = Checkable.get_list(ops)
        self.check_result(z,
                          len_expected=ops,
                          rechecks_expected=0,
                          zeros_expected=10)

        z = Checkable.get_list(ops)
        self.check_result(z,
                          ops,
                          rechecks_expected=0,
                          zeros_expected=2)

    # -------------------------------------------------------------------------
    def test_recheck_0_3__5d(self):
        """
        recheck_fraction = 0.3
        recheck_age = 5d
        ==> 1 recheck, 9 lc=0
        ==> 3 lc=0, 7 oblc
        """
        self.dbgfunc()
        cfg = CrawlConfig.add_config(close=True, dct=self.cfg)
        cfg.set('cv', 'recheck_age', '5d')
        ops = int(cfg.get('cv', 'operations'))

        z = Checkable.get_list(ops)
        self.check_result(z,
                          len_expected=ops,
                          rechecks_expected=1,
                          zeros_expected=9)

        z = Checkable.get_list(ops)
        self.check_result(z,
                          len_expected=ops,
                          rechecks_expected=0,
                          zeros_expected=3)

    # -------------------------------------------------------------------------
    def test_recheck_0_3__4d(self):
        """
        recheck_fraction = 0.3
        recheck_age = 4d
        ==> 2 rechecks, 8 lc=0
        ==> 4 lc=0, 6 oblc
        """
        self.dbgfunc()
        cfg = CrawlConfig.add_config(close=True, dct=self.cfg)
        cfg.set('cv', 'recheck_age', '4d')
        ops = int(cfg.get('cv', 'operations'))

        z = Checkable.get_list(ops)
        self.check_result(z,
                          len_expected=ops,
                          rechecks_expected=2,
                          zeros_expected=8)

        z = Checkable.get_list(ops)
        self.check_result(z,
                          len_expected=ops,
                          rechecks_expected=0,
                          zeros_expected=4)

    # -------------------------------------------------------------------------
    def test_recheck_0_3__3d(self):
        """
        recheck_fraction = 0.3
        recheck_age = 3d
        ==> 3 rechecks, 7 lc=0
        ==> 1 recheck, 5 lc=0, 4 oblc
        """
        self.dbgfunc()
        cfg = CrawlConfig.add_config(close=True, dct=self.cfg)
        cfg.set('cv', 'recheck_age', '3d')
        ops = int(cfg.get('cv', 'operations'))

        z = Checkable.get_list(ops)
        self.check_result(z,
                          len_expected=ops,
                          rechecks_expected=3,
                          zeros_expected=7)

        z = Checkable.get_list(ops)
        self.check_result(z,
                          len_expected=ops,
                          rechecks_expected=1,
                          zeros_expected=5)

    # -------------------------------------------------------------------------
    def test_recheck_0_3__2d(self):
        """
        recheck_fraction = 0.3
        recheck_age = 2d
        ==> 3 rechecks, 7 lc=0
        ==> 3 rechecks, 5 lc=0, 2 oblc
        """
        self.dbgfunc()
        cfg = CrawlConfig.add_config(close=True, dct=self.cfg)
        cfg.set('cv', 'recheck_age', '2d')
        ops = int(cfg.get('cv', 'operations'))

        z = Checkable.get_list(ops)
        self.check_result(z,
                          len_expected=ops,
                          rechecks_expected=3,
                          zeros_expected=7)

        z = Checkable.get_list(ops)
        self.check_result(z,
                          len_expected=ops,
                          rechecks_expected=3,
                          zeros_expected=5)

    # -------------------------------------------------------------------------
    def test_recheck_0_3__1d(self):
        """
        recheck_fraction = 0.3
        recheck_age = 1d
        ==> 3 rechecks, 7 lc=0
        ==> 3 rechecks, 5 lc=0, 1 oblc
        """
        self.dbgfunc()
        cfg = CrawlConfig.add_config(close=True, dct=self.cfg)
        cfg.set('cv', 'recheck_age', '1d')
        ops = int(cfg.get('cv', 'operations'))

        z = Checkable.get_list(ops)
        self.check_result(z,
                          len_expected=ops,
                          rechecks_expected=3,
                          zeros_expected=7)

        z = Checkable.get_list(ops)
        self.check_result(z,
                          len_expected=ops,
                          rechecks_expected=3,
                          zeros_expected=5)

    # -------------------------------------------------------------------------
    def test_recheck_0_4__1d(self):
        """
        recheck_fraction = 0.4
        recheck_age = 1d
        ==> 4 rechecks, 6 lc=0
        ==> 4 rechecks, 6 lc=0
        """
        self.dbgfunc()
        cfg = CrawlConfig.add_config(close=True, dct=self.cfg)
        cfg.set('cv', 'recheck_fraction', '0.4')
        cfg.set('cv', 'recheck_age', '1d')
        ops = int(cfg.get('cv', 'operations'))

        z = Checkable.get_list(ops)
        self.check_result(z,
                          len_expected=ops,
                          rechecks_expected=4,
                          zeros_expected=6)

        z = Checkable.get_list(ops)
        self.check_result(z,
                          len_expected=ops,
                          rechecks_expected=4,
                          zeros_expected=6)

    # -------------------------------------------------------------------------
    def test_recheck_0_5__1d(self):
        """
        recheck_fraction = 0.5
        recheck_age = 1d
        ==> 5 rechecks, 5 lc=0
        ==> 5 rechecks, 5 lc=0
        """
        self.dbgfunc()
        cfg = CrawlConfig.add_config(close=True, dct=self.cfg)
        cfg.set('cv', 'recheck_fraction', '0.5')
        cfg.set('cv', 'recheck_age', '1d')
        ops = int(cfg.get('cv', 'operations'))

        z = Checkable.get_list(ops)
        self.check_result(z,
                          len_expected=ops,
                          rechecks_expected=5,
                          zeros_expected=5)

        z = Checkable.get_list(ops)
        self.check_result(z,
                          len_expected=ops,
                          rechecks_expected=5,
                          zeros_expected=5)

    # -------------------------------------------------------------------------
    def test_recheck_1_0__1d(self):
        """
        recheck_fraction = 1.0
        recheck_age = 1d
        ==> 10 rechecks
        ==> 1 recheck, 9 lc=0
        """
        self.dbgfunc()
        cfg = CrawlConfig.add_config(close=True, dct=self.cfg)
        cfg.set('cv', 'recheck_fraction', '1.0')
        cfg.set('cv', 'recheck_age', '1d')
        ops = int(cfg.get('cv', 'operations'))

        z = Checkable.get_list(ops)
        self.check_result(z,
                          len_expected=ops,
                          rechecks_expected=10,
                          zeros_expected=0)

        z = Checkable.get_list(ops)
        self.check_result(z,
                          len_expected=ops,
                          rechecks_expected=1,
                          zeros_expected=9)
