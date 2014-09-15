#!/usr/bin/env python
"""
Tests for Checkable.py
"""
from hpssic.Checkable import Checkable
from hpssic import CrawlDBI
from hpssic import Dimension
import os
import pdb
import pytest
import stat
import sys
from hpssic import testhelp
import time
from hpssic import toolframe
import traceback as tb
from hpssic import util


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

    # -------------------------------------------------------------------------
    @attr(slow=True, heavy=True)
    @pytest.mark.skipif('jenkins' in os.getcwd())
    def test_check_dir(self):
        """
        Calling .check() on a directory should give us back a list of Checkable
        objects representing the entries in the directory
        """
        util.conditional_rm(self.testdb)
        testhelp.db_config(self.testdir, util.my_name())
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
    @attr(slow=True, heavy=True)
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
        testhelp.db_config(self.testdir, util.my_name())
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
        except StandardError, e:
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

    # -------------------------------------------------------------------------
    @attr(slow=True)
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
        testhelp.db_config(self.testdir, util.my_name())

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
        testhelp.db_config(self.testdir, util.my_name())

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
        # pdb.set_trace()
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
        testhelp.db_config(self.testdir, util.my_name())

        try:
            Checkable.get_list()
            self.fail("Expected an exception but didn't get one.")
        except CrawlDBI.DBIerror, e:
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
        testhelp.db_config(self.testdir, util.my_name())

        # create some test data (path, type, cos, last_check)
        testdata = [('/', 'd', '', 0),
                    ('/abc', 'd', '', 17),
                    ('/xyz', 'f', '', 92),
                    ('/abc/foo', 'f', '', 5),
                    ('/abc/bar', 'f', '', time.time())]

        # testdata has to be sorted by last_check since that's the way get_list
        # will order the list it returns
        testdata.sort(key=lambda x: x[3])

        # create the .db file
        Checkable.ex_nihilo()

        # put the test data into the database
        db = CrawlDBI.DBI(dbtype='crawler')
        db.insert(table='checkables',
                  fields=['path', 'type', 'cos', 'last_check'],
                  data=testdata[1:])
        db.close()

        # run the target routine
        x = Checkable.get_list()

        # we should have gotten back the same number of records as went into
        # the database
        self.expected(len(testdata), len(x))

        # verify that the data from the database matches the testdata that was
        # inserted
        for idx, item in enumerate(x):
            self.expected(testdata[idx][0], item.path)
            self.expected(testdata[idx][1], item.type)
            self.expected(testdata[idx][2], item.cos)
            self.expected(testdata[idx][3], item.last_check)

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
        testhelp.db_config(self.testdir, util.my_name())

        # create some test data (path, type, cos, last_check)
        testdata = [('/', 'd', '', 0),
                    ('/abc', 'd', '', 17),
                    ('/xyz', 'f', '', 92),
                    ('/abc/foo', 'f', '', 5),
                    ('/abc/bar', 'f', '', time.time())]

        nrpath = '/newroot'
        nrtup = (nrpath, 'd', '', 0)

        # create the .db file
        Checkable.ex_nihilo()

        # put the test data into the database (but not newroot)
        db = CrawlDBI.DBI(dbtype='crawler')
        db.insert(table='checkables',
                  fields=['path', 'type', 'cos', 'last_check'],
                  data=testdata[1:])
        db.close()

        # run the target routine
        x = Checkable.get_list(rootlist=['/abc', nrpath])

        # we should have gotten back the same number of records as went into
        # the database plus 1 for the new root
        testdata.append(nrtup)

        # testdata has to be sorted by last_check since that's the way get_list
        # will order the list it returns
        testdata.sort(key=lambda x: x[3])

        self.expected(len(testdata), len(x))

        # verify that the data from the database matches the testdata that was
        # inserted
        for idx, item in enumerate(x):
            self.expected(testdata[idx][0], item.path)
            self.expected(testdata[idx][1], item.type)
            self.expected(testdata[idx][2], item.cos)
            self.expected(testdata[idx][3], item.last_check)

    # -------------------------------------------------------------------------
    def test_persist_last_check(self):
        """
        Verify that last_check gets stored by persist().
        """
        util.conditional_rm(self.testdb)
        testhelp.db_config(self.testdir, util.my_name())
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
        testhelp.db_config(self.testdir, util.my_name())
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
        testhelp.db_config(self.testdir, util.my_name())
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
        testhelp.db_config(self.testdir, util.my_name())
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
        testhelp.db_config(self.testdir, util.my_name())
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
        testhelp.db_config(self.testdir, util.my_name())
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
        except StandardError, e:
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
        testhelp.db_config(self.testdir, util.my_name())
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
        testhelp.db_config(self.testdir, util.my_name())
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
        testhelp.db_config(self.testdir, util.my_name())
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
        testhelp.db_config(self.testdir, util.my_name())
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
        testhelp.db_config(self.testdir, util.my_name())
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
        testhelp.db_config(self.testdir, util.my_name())
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
        testhelp.db_config(self.testdir, util.my_name())
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
