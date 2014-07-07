#!/usr/bin/env python
"""
Database interface classes

We have interface classes for mysql, sqlite, and db2.

The db2 interface only supports read operations, nothing that will change the
database. Also the db2 interface doesn't use table prefixes.
"""
from nose.plugins.attrib import attr
import base64
from hpssic import CrawlConfig
from hpssic import CrawlDBI
import os
import pdb
import socket
import sys
from hpssic import testhelp
from hpssic import toolframe
import traceback as tb
from hpssic import util
import warnings


# -----------------------------------------------------------------------------
def make_tcfg(dbtype):
    tcfg = CrawlConfig.CrawlConfig()
    tcfg.add_section('dbi')
    tcfg.set('dbi', 'dbtype', dbtype)
    tcfg.set('dbi', 'dbname', DBITest.testdb)
    tcfg.set('dbi', 'tbl_prefix', 'test')
    xcfg = CrawlConfig.get_config(reset=True)
    if dbtype == 'mysql':
        tcfg.set('dbi', 'dbname', 'hpssic')
        tcfg.set('dbi', 'host', 'db-hpssic.ccs.ornl.gov')
        tcfg.set('dbi', 'username', 'hpssic_user')
        tcfg.set('dbi', 'password', xcfg.get('dbi', 'password'))
    elif dbtype == 'db2':
        tcfg.add_section('db2')
        tcfg.set('dbi', 'dbname', 'hcfg')
        tcfg.set('dbi', 'tbl_prefix', 'hpss')
        for optname in ['db_cfg_name', 'db_sub_name',
                        'hostname', 'port',
                        'username', 'password']:
            tcfg.set('db2', optname, xcfg.get('db2', optname))

    return tcfg

# -----------------------------------------------------------------------------
def setUpModule():
    """
    Set up for testing
    """
    # pdb.set_trace()
    testhelp.module_test_setup(DBITest.testdir)

# -----------------------------------------------------------------------------
def tearDownModule():
    """
    Clean up after testing
    """
    testhelp.module_test_teardown(DBITest.testdir)
    if not testhelp.keepfiles():
        tcfg = make_tcfg('mysql')
        tcfg.set('dbi', 'tbl_prefix', '')
        db = CrawlDBI.DBI(cfg=tcfg)
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore",
                                    "Can't read dir of .*")
            tlist = db.select(table="information_schema.tables",
                              fields=['table_name'],
                              where='table_name like "test_%"')
        for (tname,) in tlist:
            db.drop(table=tname)

# -----------------------------------------------------------------------------
class DBITestRoot(testhelp.HelpedTestCase):
    # -------------------------------------------------------------------------
    def setup_select(self, table_name):
        self.reset_db(table_name)
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        db.create(table=table_name, fields=self.fdef)
        db.insert(table=table_name, fields=self.fnames, data=self.testdata)
        return db
    
    # -------------------------------------------------------------------------
    def exc_raises(self, funcname, kwargs, exp):
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        try:
            if funcname == 'create':
                db.create(**kwargs)
            elif funcname == 'delete':
                db.delete(**kwargs)
            elif funcname == 'drop':
                db.drop(**kwargs)
            elif funcname == 'insert':
                db.insert(**kwargs)
            elif funcname == 'select':
                rows = db.select(**kwargs)
            elif funcname == 'update':
                db.update(**kwargs)
            else:
                raise StandardError("unsupported function name '%s'" %
                                    funcname)
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue(exp in str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))

    
# -----------------------------------------------------------------------------
class DBITest(DBITestRoot):
    """
    Tests for the DBI class
    """
    testdir = testhelp.testdata(__name__)
    cfgfile = '%s/dbitest.cfg' % testdir
    testdb = '%s/test.db' % testdir
    
    # -------------------------------------------------------------------------
    def test_ctor_nodbname(self):
        """
        CrawlDBI ctor should not accept a dbname argument. It has to take its
        dbname from the config.
        """
        try:
            a = CrawlDBI.DBI(dbname='foobar')
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            exp = "dbname may only be specified if dbtype = 'db2'"
            self.assertTrue(exp in str(e),
                            "Got the wrong DBIerror: %s" +  # <-- !@!looks wrong
                            util.line_quote(str(e)))

    # -------------------------------------------------------------------------
    def test_ctor_sqlite(self):
        """
        With a config object specifying sqlite as the database type, DBI should
        instantiate itself with an internal DBIsqlite object.
        """
        a = CrawlDBI.DBI(cfg=make_tcfg('sqlite'))
        self.assertTrue(hasattr(a, '_dbobj'),
                        "Expected to find a _dbobj attribute on %s" % a)
        self.assertTrue(isinstance(a._dbobj, CrawlDBI.DBIsqlite),
                        "Expected %s to be a DBIsqlite object" % a._dbobj)
        
    # -------------------------------------------------------------------------
    def test_repr(self):
        """
        With a config object specifying sqlite as the database type, calling
        __repr__ on a DBI object should produce a representation that looks
        like a DBIsqlite object.
        """
        a = CrawlDBI.DBI(cfg=make_tcfg('sqlite'))
        b = CrawlDBI.DBIsqlite(dbname=self.testdb, tbl_prefix='test')
        self.expected(str(b), str(a))

# -----------------------------------------------------------------------------
class DBI_in_Base(object):
    """
    Basic tests for the DBI<dbname> classes that do not change the database.

    Arranging the tests this way allows us to avoid having to write a complete,
    independent set of tests for each database type.

    Class DBIsqliteTest, for example, which inherits the test methods from this
    one, will set the necessary parameters to select the sqlite database type
    so that when it is processed by the test running code, the tests will be
    run on an sqlite database. Similarly, DBImysqlTest will set the necessary
    parameters to select that database type, then run the inherited tests on a
    mysql database.
    """

    testdir = DBITest.testdir
    testdb = '%s/test.db' % testdir
    fdef = ['name text', 'size int', 'weight double']
    fnames = [x.split()[0] for x in fdef]
    # tests below depend on testdata fulfilling the following conditions:
    #  * only one record with size = 92
    #  * only one record with name = 'zippo'
    testdata = [('frodo', 17, 108.5),
                ('zippo', 92, 12341.23),
                ('zumpy', 45, 9.3242),
                ('frodo', 23, 212.5),
                ('zumpy', 55, 90.6758)]
    
    # -------------------------------------------------------------------------
    def test_close(self):
        """
        Calling close() should free up the db resources and make the database
        handle unusable.
        """
        a = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        a.close()
        try:
            a.table_exists(table='dimension')
            self.fail("Expected exception on closed database not thrown")
        except CrawlDBI.DBIerror, e:
            exp = "Cannot operate on a closed database."
            self.assertTrue(exp in str(e),
                            "Expected '%s', got '%s'" % (exp, str(e)))
    
    # -------------------------------------------------------------------------
    def test_ctor_attrs(self):
        """
        Verify that a new object has the right attributes with the right
        default values
        """
        a = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        dirl = [q for q in dir(a) if not q.startswith('_')]
        xattr = ['close', 'create', 'dbname', 'delete', 'drop', 'insert',
                 'select', 'table_exists', 'update',
                 'cursor']

        for attr in dirl:
            if attr not in xattr:
                self.fail("Unexpected attribute %s on object %s" % (attr, a))
        for attr in xattr:
            if attr not in dirl:
                self.fail("Expected attribute %s not found on object %s" %
                          (attr, a))

    # -------------------------------------------------------------------------
    def test_ctor_bad_attrs(self):
        """
        Attempt to create an object is invalid attribute should get an
        exception
        """
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "Attribute 'badattr' is not valid",
                             CrawlDBI.DBI,
                             cfg=make_tcfg(self.dbtype),
                             badattr='frooble')
            
    # -------------------------------------------------------------------------
    def test_ctor_dbn_none(self):
        """
        Attempt to create an object with no dbname should get an exception
        """
        try:
            tcfg = make_tcfg(self.dbtype)
            tcfg.remove_option('dbi', 'dbname')
            a = CrawlDBI.DBI(cfg=tcfg)
            self.fail("Expected an exception but didn't get one")
        except CrawlDBI.DBIerror, e:
            exp = "No option 'dbname' in section: 'dbi'"
            self.assertTrue(exp in str(e),
                            "Got the wrong DBIerror: " +
                            '"""\n%s\n"""' % str(e))

    # -------------------------------------------------------------------------
    def test_select_f(self):
        """
        Calling select() specifying fields should get only the fields requested
        """
        tname=util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        rows = db.select(table=tname, fields=['size', 'weight'])
        self.assertEqual(len(rows[0]), 2,
                         "Expected two fields in each row, got %d" % len(rows[0]))
        for tup in self.testdata:
            self.assertTrue((tup[1], tup[2]) in rows,
                            "Expected %s in %s but it's not there" %
                            (str((tup[1], tup[2], )),
                             util.line_quote(str(rows))))
    

    # -------------------------------------------------------------------------
    def test_select_gb_f(self):
        """
        Select with a group by clause on a field that is present in the table.
        """
        tname=util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        rows = db.select(table=tname, fields=['sum(size)'], groupby='name')
        self.expected(3, len(rows))
        self.expected(True, ((40,)) in rows)
        self.expected(True, ((92,)) in rows)
        self.expected(True, ((100,)) in rows)
        
    # -------------------------------------------------------------------------
    def test_select_gb_ns(self):
        """
        Select with a group by clause that is not a string -- should get an
        exception.
        """
        tname=util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        try:
            rows = db.select(table=tname, fields=['sum(size)'],
                             groupby=['fiddle'])
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On select(), groupby clause must be a string"
                            in str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        
    # -------------------------------------------------------------------------
    def test_select_gb_u(self):
        """
        Select with a group by clause on a field that is unknown should get an
        exception.
        """
        tname=util.my_name().replace('test_', '')
        db = self.setup_select(tname)
        ns_field = 'fiddle'

        try:
            rows = db.select(table=tname, fields=['sum(size)'],
                             groupby=ns_field)
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            sqlite_msg = 'no such column: %s' % ns_field
            mysql_msg = "Unknown column '%s' in 'group statement'" % ns_field
            self.assertTrue(sqlite_msg in str(e) or mysql_msg in str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        
    # -------------------------------------------------------------------------
    def test_select_nq_mtd(self):
        """
        Calling select() with where with no '?' and an empty data list is fine.
        The data returned should match the where clause.
        """
        tname=util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        rows = db.select(table=tname, fields=[],
                         where='size = 92', data=())
        self.expected(1, len(rows))
        self.expected([self.testdata[1]], list(rows))

    # -------------------------------------------------------------------------
    def test_select_q_mtd(self):
        """
        Calling select() with a where clause with a '?' and an empty data list
        should get an exception
        """
        tname=util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        try:
            rows = db.select(table=tname, fields=[],
                             where='name = ?', data=())
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("Incorrect number of bindings supplied" in str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except TypeError, e:
            self.assertTrue("not enough arguments for format string" in str(e),
                            "Got the wrong TypeError: %s" %
                            util.line_quote(str(e)))

    # -------------------------------------------------------------------------
    def test_select_nq_ld(self):
        """
        Calling select() with where clause with no '?' and data in the list
        should get an exception -- the data would be ignored
        """
        tname=util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        try:
            rows = db.select(table=tname, fields=[],
                             where="name = 'zippo'", data=('frodo',))
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("Data would be ignored" in str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))

    # -------------------------------------------------------------------------
    def test_select_q_ld(self):
        """
        Calling select() with a where clause containing '?' and data in the
        data list should return the data matching the where clause
        """
        tname=util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        rows = db.select(table=tname, fields=[],
                         where='name = ?', data=('zippo',))
        self.expected(1, len(rows))
        self.expected([self.testdata[1],], list(rows))

    # -------------------------------------------------------------------------
    # Adding tests for limit in select. Conditions to be tested:

    # -------------------------------------------------------------------------
    #   - no limit argument (this is already tested)
    #     > should retrieve all the data
    # -------------------------------------------------------------------------
    #   - limit not an int
    #     > should throw exception
    def test_select_l_nint(self):
        tname = util.my_name().replace("test_", "")
        self.setup_select(tname)
        self.exc_raises('select',
                            {'table': tname,
                             'fields': [],
                             'limit': 'this is a string'},
                            "On select(), limit must be an int")
        
    # -------------------------------------------------------------------------
    #   - limit is an int
    #     > should retrieve the specified number of records
    def test_select_l_int(self):
        tname=util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        rlim = 3
        rows = db.select(table=tname, fields=[], limit=rlim)
        self.assertEqual(len(rows[0]), 3,
                         "Expected three fields in each row, got %d" %
                         len(rows[0]))
        self.expected(rlim, len(rows))
        for tup in self.testdata[0:int(rlim)]:
            self.assertTrue(tup in rows,
                            "Expected %s in %s but it's not there" %
                            (str(tup), util.line_quote(rows)))
        
    # -------------------------------------------------------------------------
    #   - limit is a float
    #     > should convert to an int and use it
    def test_select_l_float(self):
        tname=util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        rlim = 2.7
        rows = db.select(table=tname, fields=[], limit=rlim)
        self.assertEqual(len(rows[0]), 3,
                         "Expected three fields in each row, got %d" %
                         len(rows[0]))
        self.expected(int(rlim), len(rows))
        for tup in self.testdata[0:int(rlim)]:
            self.assertTrue(tup in rows,
                            "Expected %s in %s but it's not there" %
                            (str(tup), util.line_quote(rows)))

    # -------------------------------------------------------------------------
    def test_select_mtf(self):
        """
        Calling select() with an empty field list should get all the data -- an
        empty field list indicates the wildcard option
        """
        tname=util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        rows = db.select(table=tname, fields=[])
        self.assertEqual(len(rows[0]), 3,
                         "Expected three fields in each row, got %d" %
                         len(rows[0]))
        for tup in self.testdata:
            self.assertTrue(tup in rows,
                            "Expected %s in %s but it's not there" %
                            (str(tup), util.line_quote(rows)))
    
    # -------------------------------------------------------------------------
    def test_select_mto(self):
        """
        Calling select() with an empty orderby should get the data in the order
        inserted
        """
        tname=util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        rows = db.select(table=tname, fields=[], orderby='')
        self.assertEqual(len(rows[0]), 3,
                         "Expected three fields in each row, got %d" %
                         len(rows[0]))
        self.assertEqual(list(self.testdata), list(rows),
                         "Expected %s and %s to match" %
                         (list(self.testdata), list(rows)))
    
    # -------------------------------------------------------------------------
    def test_select_mtt(self):
        """
        Calling select() with an empty table name should get an exception
        """
        tname=util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        try:
            rows = db.select(table='', fields=[])
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On select(), table name must not be empty" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))

    # -------------------------------------------------------------------------
    def test_select_mtw(self):
        """
        Calling select() with an empty where arg should get all the data
        """
        tname=util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        rows = db.select(table=tname, fields=[], where='')
        self.assertEqual(len(rows[0]), 3,
                         "Expected three fields in each row, got %d" %
                         len(rows[0]))
        self.assertEqual(list(self.testdata), list(rows),
                         "Expected %s and %s to match" %
                         (list(self.testdata), list(rows)))
    
    # -------------------------------------------------------------------------
    def test_select_nld(self):
        """
        Calling select() with a non-tuple as the data argument should
        get an exception
        """
        tname=util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        try:
            rows = db.select(table=tname, fields=[],
                             where='name = ?', data='zippo')
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On select(), data must be a tuple" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
    
    # -------------------------------------------------------------------------
    def test_select_nlf(self):
        """
        Calling select() with a non-list as the fields argument should
        get an exception
        """
        tname=util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        try:
            rows = db.select(table=tname, fields=17)
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On select(), fields must be a list" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
    
    # -------------------------------------------------------------------------
    def test_select_nso(self):
        """
        Calling select() with a non-string orderby argument should
        get an exception
        """
        tname=util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        try:
            rows = db.select(table=tname, fields=self.fnames, orderby=22)
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On select(), orderby clause must be a string" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
    
    # -------------------------------------------------------------------------
    def test_select_nst(self):
        """
        Calling select() with a non-string table argument should
        get an exception
        """
        tname=util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        try:
            rows = db.select(table={}, fields=self.fnames)
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On select(), table name must be a string" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
    
    # -------------------------------------------------------------------------
    def test_select_nsw(self):
        """
        Calling select() with a non-string where argument should
        get an exception
        """
        tname=util.my_name().replace('test_', '')
        db = self.setup_select(tname)

        try:
            rows = db.select(table=tname, fields=self.fnames, where=22)
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On select(), where clause must be a string" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
    
    # -------------------------------------------------------------------------
    def test_select_o(self):
        """
        Calling select() specifying orderby should get the rows in the
        order requested
        """
        tname=util.my_name().replace('test_', '')
        db = self.setup_select(tname)
        exp = [self.testdata[2], self.testdata[4], self.testdata[0],
               self.testdata[3], self.testdata[1]]

        rows = db.select(table=tname, fields=[], orderby='weight')
        self.assertEqual(len(rows[0]), 3,
                         "Expected three fields in each row, got %d" %
                         len(rows[0]))
        self.assertEqual(list(exp), list(rows),
                         "Expected %s to match %s" % (str(exp), str(rows)))
    
    # -------------------------------------------------------------------------
    def test_select_w(self):
        """
        Calling select() specifying where should get only the rows requested
        """
        tname=util.my_name().replace('test_', '')
        db = self.setup_select(tname)
        exp = [self.testdata[0], self.testdata[2], self.testdata[3]]

        rows = db.select(table=tname, fields=[], where="size < 50")
        self.assertEqual(len(rows[0]), 3,
                         "Expected three fields in each row, got %d" %
                         len(rows[0]))
        self.assertEqual(list(exp), list(rows),
                         "Expected %s to match %s" % (str(exp), str(rows)))
    
    # -------------------------------------------------------------------------
    def test_table_exists_yes(self):
        """
        If table foo exists, db.table_exists(table='foo') should return True
        """
        tname = util.my_name().replace('test_', '')
        self.reset_db(tname)
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        db.create(table=tname, fields=self.fdef)
        self.expected(True, db.table_exists(table=tname))

    # -------------------------------------------------------------------------
    def test_table_exists_no(self):
        """
        If table foo does not exist, db.table_exists(table='foo') should return
        False
        """
        tname = util.my_name().replace('test_', '')
        self.reset_db(tname)
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        self.expected(False, db.table_exists(table=tname))
        
# -----------------------------------------------------------------------------
class DBI_out_Base(object):
    """
    Basic tests for the DBI<dbname> classes -- methods that change the
    database.

    Arranging the tests this way allows us to avoid having to write a complete,
    independent set of tests for each database type.

    The mySql and sqlite test classes will inherit this one in addition to
    DBI_in_Base.
    """
    testdir = DBITest.testdir
    testdb = '%s/test.db' % testdir
    fdef = ['name text', 'size int', 'weight double']
    fnames = [x.split()[0] for x in fdef]
    # tests below depend on testdata fulfilling the following conditions:
    #  * only one record with size = 92
    #  * only one record with name = 'zippo'
    testdata = [('frodo', 17, 108.5),
                ('zippo', 92, 12341.23),
                ('zumpy', 45, 9.3242),
                ('frodo', 23, 212.5),
                ('zumpy', 55, 90.6758)]
    
    # -------------------------------------------------------------------------
    def test_create_mtf(self):
        """
        Calling create() with an empty field list should get an exception
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))

        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On create(), fields must not be empty",
                             db.create,
                             table='nogood',
                             fields=[])

    # -------------------------------------------------------------------------
    def test_create_mtt(self):
        """
        Calling create() with an empty table name should get an exception
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On create(), table name must not be empty",
                             db.create,
                             table='',
                             fields=['abc text'])

    # -------------------------------------------------------------------------
    def test_create_nlf(self):
        """
        Calling create() with a non-list as the fields argument should
        get an exception
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On create(), fields must be a list",
                             db.create,
                             table='create_nlf',
                             fields='notdict')

    # -------------------------------------------------------------------------
    def test_create_yes(self):
        """
        Calling create() with correct arguments should create the table
        """
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        if db.table_exists(table='create_yes'):
            db.drop(table='create_yes')
        db.create(table='create_yes', fields=['one text',
                                              'two int'])
        self.assertTrue(db.table_exists(table='create_yes'))

    # -------------------------------------------------------------------------
    def test_delete_nq_nd(self):
        """
        A delete with no '?' in the where clause and no data tuple is
        okay. The records deleted should match the where clause.
        """
        (db, td) = self.delete_setup()
        db.delete(table=td['tabname'], where="name='sam'")
        rows = db.select(table=td['tabname'])
        db.close()

        for r in td['rows'][0:1] + td['rows'][2:]:
            self.assertTrue(r in rows,
                            "Expected %s in %s" % (r, rows))
        self.assertFalse(td['rows'][1] in rows,
                         "%s should have been deleted" % (td['rows'][1],))
        
    # -------------------------------------------------------------------------
    def test_delete_q_nd(self):
        """
        A delete with a '?' in the where clause and no data tuple should
        get an exception.
        """
        (db, td) = self.delete_setup()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "Criteria are not fully specified",
                             db.delete,
                             table=td['tabname'],
                             where='name=?')
        
        rows = db.select(table=td['tabname'])
        db.close()

        # no data should have been deleted
        for r in td['rows']:
            self.assertTrue(r in rows,
                            "Expected %s in %s" % (r, rows))
        
    # -------------------------------------------------------------------------
    def test_delete_nq_td(self):
        """
        A delete with no '?' in the where clause and a non-empty data list
        should get an exception -- the data would be ignored.
        """
        (db, td) = self.delete_setup()

        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "Data would be ignored",
                             db.delete,
                             table=td['tabname'],
                             where='name=foo',
                             data=('meg',))
        
        rows = db.select(table=td['tabname'])
        db.close()

        # no data should have been deleted
        for r in td['rows']:
            self.assertTrue(r in rows,
                            "Expected %s in %s" % (r, rows))
        

    # -------------------------------------------------------------------------
    def test_delete_q_td(self):
        """
        A delete with a '?' in the where clause and a non-empty data list
        should delete the data matching the where clause.
        """
        (db, td) = self.delete_setup()
        db.delete(table=td['tabname'], where='name=?', data=('gertrude',))
        rows = db.select(table=td['tabname'])
        db.close()

        for r in td['rows'][0:-1]:
            self.assertTrue(r in rows,
                            "Expected %s in %s" % (r, rows))
        self.assertFalse(td['rows'][-1] in rows,
                         "%s should have been deleted" % (td['rows'][1],))
        

    # -------------------------------------------------------------------------
    def test_delete_mtt(self):
        """
        A delete with an empty table name should throw an exception.
        """
        (db, td) = self.delete_setup()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On delete(), table name must not be empty",
                             db.delete,
                             table='',
                             where='name=?',
                             data=('meg',))
        
        rows = db.select(table=td['tabname'])
        db.close()

        # no data should have been deleted
        for r in td['rows']:
            self.assertTrue(r in rows,
                            "Expected %s in %s" % (r, rows))

    # -------------------------------------------------------------------------
    def test_delete_mtw(self):
        """
        A delete with an empty where clause should delete all the data.
        """
        (db, td) = self.delete_setup()
        db.delete(table=td['tabname'])
        rows = db.select(table=td['tabname'])
        db.close()

        self.expected(0, len(rows))

    # -------------------------------------------------------------------------
    def test_delete_ntd(self):
        """
        A delete with a non-tuple data value should throw an exception
        """
        (db, td) = self.delete_setup()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On delete(), data must be a tuple",
                             db.delete,
                             table=td['tabname'],
                             where='name=?',
                             data='meg')
        
        rows = db.select(table=td['tabname'])
        db.close()

        # no data should have been deleted
        for r in td['rows']:
            self.assertTrue(r in rows,
                            "Expected %s in %s" % (r, rows))

    # -------------------------------------------------------------------------
    def test_delete_nst(self):
        """
        A delete with a non-string table name should throw an exception
        """
        (db, td) = self.delete_setup()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On delete(), table name must be a string",
                             db.delete,
                             table=32,
                             where='name=?',
                             data='meg')
        
        rows = db.select(table=td['tabname'])
        db.close()

        # no data should have been deleted
        for r in td['rows']:
            self.assertTrue(r in rows,
                            "Expected %s in %s" % (r, rows))

    # -------------------------------------------------------------------------
    def test_delete_nsw(self):
        """
        A delete with a non-string where argument should throw an exception
        """
        (db, td) = self.delete_setup()
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On delete(), where clause must be a string",
                             db.delete,
                             table=td['tabname'],
                             where=[])
                          
        rows = db.select(table=td['tabname'])
        db.close()

        # no data should have been deleted
        for r in td['rows']:
            self.assertTrue(r in rows,
                            "Expected %s in %s" % (r, rows))

    # -------------------------------------------------------------------------
    def test_delete_w(self):
        """
        A delete with a valid where argument should delete the data matching
        the where
        """
        (db, td) = self.delete_setup()
        db.delete(table=td['tabname'], where="name like 's%'")
        rows = db.select(table=td['tabname'], fields=['id', 'name', 'age'])
        db.close()

        for r in td['rows'][0:1] + td['rows'][3:]:
            self.assertTrue(r in rows,
                            "Expected %s in %s" % (r, rows))
        for r in td['rows'][1:3]:
            self.assertFalse(r in rows,
                             "%s should have been deleted" % (r,))
        
    # -------------------------------------------------------------------------
    def test_insert_fnox(self):
        """
        Calling insert on fields not in the table should get an exception
        """
        self.reset_db('fnox')
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        db.create(table='fnox', fields=['one text', 'two text'])
        try:
            db.insert(table='fnox',
                      fields=['one', 'two', 'three'],
                      data=[('abc', 'def', 99),
                            ('aardvark', 'buffalo', 78)])
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            sqlite_msg = "table test_fnox has no column named three"
            mysql_msg = "Unknown column 'three' in 'field list'"
            self.assertTrue(sqlite_msg in str(e) or mysql_msg in str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
    
    # -------------------------------------------------------------------------
    def test_insert_mtd(self):
        """
        Calling insert with an empty data list should get an exception
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On insert(), data list must not be empty",
                             db.insert,
                             table='mtd',
                             fields=['one', 'two'],
                             data=[])
    
    # -------------------------------------------------------------------------
    def test_insert_mtf(self):
        """
        Calling insert with an empty field list should get an exception
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On insert(), fields list must not be empty",
                             db.insert,
                             table='mtd',
                             fields=[],
                             data=[(1, 2)])
    
    # -------------------------------------------------------------------------
    def test_insert_mtt(self):
        """
        Calling insert with an empty table name should get an exception
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On insert(), table name must not be empty",
                             db.insert,
                             table='',
                             fields=['one', 'two'],
                             data=[(1, 2)])
    
    # -------------------------------------------------------------------------
    def test_insert_nst(self):
        """
        Calling insert with a non-string table name should get an exception
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On insert(), table name must be a string",
                             db.insert,
                             table=32,
                             fields=['one', 'two'],
                             data=[(1, 2)])

    # -------------------------------------------------------------------------
    def test_insert_nlf(self):
        """
        Calling insert with a non-list fields arg should get an exception
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On insert(), fields must be a list",
                             db.insert,
                             table='nlf',
                             fields='froo',
                             data=[(1, 2)])

    # -------------------------------------------------------------------------
    def test_insert_nld(self):
        """
        Calling insert with a non-list data arg should get an exception
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On insert(), data must be a list",
                             db.insert,
                             table='nlf',
                             fields=['froo', 'pizzazz'],
                             data={})
    
    # -------------------------------------------------------------------------
    def test_insert_tnox(self):
        """
        Calling insert on a non-existent table should get an exception
        """
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             ["no such table: test_tnox",
                              "Table 'hpssic.test_tnox' doesn't exist"],
                             db.insert,
                             table='tnox',
                             fields=['one', 'two'],
                             data=[('abc', 'def'),
                                   ('aardvark', 'buffalo')])
    
    # -------------------------------------------------------------------------
    def test_insert_yes(self):
        """
        Calling insert with good arguments should put the data in the table
        """
        tname=util.my_name().replace('test_', '')
        self.reset_db(tname)
        fdef = ['id int primary key', 'name text', 'size int']
        fnames = [x.split()[0] for x in fdef]
        testdata = [(1, 'sinbad', 54),
                    (2, 'zorro', 98)]

        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        db.create(table=tname, fields=fdef)
        db.insert(table=tname, fields=fnames, data=testdata)
                  
        dbc = db.cursor()
        dbc.execute("""
        select * from test_insert_yes
        """)
        rows = dbc.fetchall()
        for tup in testdata:
            self.assertTrue(tup in rows,
                            "Expected data %s not found in table" % str(tup))

    # -------------------------------------------------------------------------
    def test_update_f(self):
        """
        Calling update() specifying fields should update the fields requested
        """
        tname = util.my_name().replace('test_', '')
        udata = [('frodo', 23, 199.7),
                 ('zippo', 14, 201.3),
                 ('zumpy', 47, 202.1)]
        
        self.reset_db(tname)
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        db.create(table=tname, fields=self.fdef)
        db.insert(table=tname, fields=self.fnames, data=self.testdata)
        db.update(table=tname,
                  fields=['size'],
                  data=[(x[1], x[0]) for x in udata],
                  where='name = ?')
        r = db.select(table=tname, fields=[])

        for idx, tup in enumerate(udata):
            exp = (udata[idx][0], udata[idx][1], self.testdata[idx][2])
            self.assertTrue(exp in r,
                            "Expected %s in %s but didn't find it" %
                            (str(exp), util.line_quote(r)))
    
    # -------------------------------------------------------------------------
    def test_update_qp(self):
        """
        Calling update() specifying fields should update the fields requested.
        However, placeholders should not be quoted.
        """
        tname = util.my_name().replace('test_', '')
        udata = [('frodo', 23, 199.7),
                 ('zippo', 14, 201.3),
                 ('zumpy', 47, 202.1)]
        
        self.reset_db(tname)
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        db.create(table=tname, fields=self.fdef)
        db.insert(table=tname, fields=self.fnames, data=self.testdata)
        try:
            db.update(table=tname,
                      fields=['size'],
                      data=[(x[1], x[0]) for x in udata],
                      where='name = "?"')
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("Parameter placeholders should not be quoted"
                            in str(e),
                            "Expected message not found in exception")
        
    # -------------------------------------------------------------------------
    def test_update_mtd(self):
        """
        Calling update() with an empty data list should get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On update(), data must not be empty",
                             db.update,
                             table=tname,
                             fields=self.fnames,
                             data=[])

    # -------------------------------------------------------------------------
    def test_update_mtf(self):
        """
        Calling update() with an empty field list should get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On update(), fields must not be empty",
                             db.update,
                             table=tname,
                             fields=[],
                             data=self.testdata)
    
    # -------------------------------------------------------------------------
    def test_update_mtt(self):
        """
        Calling update() with an empty table name should get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On update(), table name must not be empty",
                             db.update,
                             table='',
                             fields=self.fnames,
                             data=self.testdata)
    
    # -------------------------------------------------------------------------
    def test_update_mtw(self):
        """
        Calling update() with an empty where arg should update all the rows
        """
        tname = util.my_name().replace('test_', '')
        udata = [('frodo', 23, 199.7),
                 ('zippo', 14, 201.3),
                 ('zumpy', 47, 202.1)]
        
        self.reset_db(tname)
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        db.create(table=tname, fields=self.fdef)
        db.insert(table=tname, fields=self.fnames, data=self.testdata)
        db.update(table=tname,
                  fields=['size'],
                  data=[(43,)],
                  where='')
        r = db.select(table=tname, fields=[])

        for idx, tup in enumerate(udata):
            exp = (udata[idx][0], 43, self.testdata[idx][2])
            self.assertTrue(exp in r,
                            "Expected %s in %s but didn't find it" %
                            (str(exp), util.line_quote(r)))
    
    # -------------------------------------------------------------------------
    def test_update_nlf(self):
        """
        Calling update() with a non-list as the fields argument should
        get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On update(), fields must be a list",
                             db.update,
                             table=tname,
                             fields=17,
                             data=self.testdata)
    
    # -------------------------------------------------------------------------
    def test_update_nld(self):
        """
        Calling update() with a non-list data argument should
        get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On update(), data must be a list of tuples",
                             db.update,
                             table=tname,
                             fields=self.fnames,
                             data='notalist')
    
    # -------------------------------------------------------------------------
    def test_update_nst(self):
        """
        Calling update() with a non-string table argument should
        get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On update(), table name must be a string",
                             db.update,
                             table=38,
                             fields=self.fnames,
                             data=self.testdata)
    
    # -------------------------------------------------------------------------
    def test_update_nsw(self):
        """
        Calling update() with a non-string where argument should
        get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "On update(), where clause must be a string",
                             db.update,
                             table=tname,
                             fields=self.fnames,
                             data=self.testdata,
                             where=[])
    
    # -------------------------------------------------------------------------
    def test_update_w(self):
        """
        Calling update() specifying where should update only the rows requested
        """
        tname = util.my_name().replace('test_', '')
        udata = [('frodo', 23, 199.7),
                 ('zippo', 14, 201.3),
                 ('zumpy', 47, 202.1)]
        
        self.reset_db(tname)
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        db.create(table=tname, fields=self.fdef)
        db.insert(table=tname, fields=self.fnames, data=self.testdata)
        db.update(table=tname,
                  fields=['size', 'weight'],
                  data=[(udata[2][1:])],
                  where="name = 'zumpy'")
        r = db.select(table=tname, fields=[])

        explist = self.testdata[0:2] + udata[2:]
        for exp in explist:
            self.assertTrue(exp in r,
                            "Expected %s in %s but didn't find it" %
                            (str(exp), util.line_quote(r)))
        
    # -------------------------------------------------------------------------
    def delete_setup(self):
        flist = ['id integer primary key', 'name text', 'age int']
        testdata = {'tabname': 'test_table',
                    'flist': flist,
                    'ifields': [x.split()[0] for x in flist],
                    'rows': [(1, 'bob', 32),
                             (2, 'sam', 17),
                             (3, 'sally', 25),
                             (4, 'meg', 19),
                             (5, 'gertrude', 95)]}
        self.reset_db(testdata['tabname'])
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        db.create(table=testdata['tabname'],
                  fields=testdata['flist'])
        db.insert(table=testdata['tabname'],
                  fields=testdata['ifields'],
                  data=testdata['rows'])
        return (db, testdata)
    
# -----------------------------------------------------------------------------
class DBImysqlTest(DBI_in_Base, DBI_out_Base, DBITestRoot):
    dbtype = 'mysql'
    pass

    # -------------------------------------------------------------------------
    def test_ctor_mysql_dbnreq(self):
        """
        The DBImysql ctor requires 'dbname' and 'tbl_prefix' as keyword
        arguments
        """
        try:
            a = CrawlDBI.DBImysql(tbl_prefix='xyzzy')
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            exp = "A database name is required"
            self.assertTrue(exp in str(e),
                            "Got the wrong DBIerror: %s" +
                            util.line_quote(str(e)))
            
    # -------------------------------------------------------------------------
    def reset_db(self, name=''):
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        db.drop(table=name)

# -----------------------------------------------------------------------------
class DBIsqliteTest(DBI_in_Base, DBI_out_Base, DBITestRoot):
    dbtype = 'sqlite'

    # -------------------------------------------------------------------------
    def test_ctor_dbn_db(self):
        """
        File dbname exists and is a database file -- we will use it.
        """
        # first, we create a database file from scratch
        util.conditional_rm(self.testdb)
        tabname = util.my_name()
        dba = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        dba.create(table=tabname, fields=['field1 text'])
        dba.close()
        self.assertTrue(os.path.exists(self.testdb),
                        "Expected %s to exists but it does not" % self.testdb)
        s = os.stat(self.testdb)
        self.assertNotEqual(0, s.st_size,
                            "Expected %s to contain some data" % self.testdb)

        # now, when we try to access it, it should be there
        dbb = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        self.assertTrue(dbb.table_exists(table=tabname))
        dbb.close()
        self.assertTrue(os.path.exists(self.testdb),
                        "Expected %s to exists but it does not" % self.testdb)
        s = os.stat(self.testdb)
        self.assertNotEqual(0, s.st_size,
                            "Expected %s to contain some data" % self.testdb)

    # -------------------------------------------------------------------------
    def test_ctor_dbn_dir(self):
        """
        File dbname exists and is a directory -- we throw an exception.
        """
        util.conditional_rm(self.testdb)
        os.mkdir(self.testdb, 0777)
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "unable to open database file",
                             CrawlDBI.DBI,
                             cfg=make_tcfg(self.dbtype))
    
    # -------------------------------------------------------------------------
    def test_ctor_dbn_empty(self):
        """
        File dbname exists and is empty -- we will use it as a database.
        """
        util.conditional_rm(self.testdb)
        testhelp.touch(self.testdb)
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        db.create(table='testtab', fields=['gru text'])
        db.close()
        self.assertTrue(os.path.exists(self.testdb),
                        "Expected %s to exists but it does not" % self.testdb)
        s = os.stat(self.testdb)
        self.assertNotEqual(0, s.st_size,
                            "Expected %s to contain some data" % self.testdb)
    
    # -------------------------------------------------------------------------
    def test_ctor_dbn_fifo(self):
        """
        File dbname exists and is a fifo -- we throw an exception
        """
        util.conditional_rm(self.testdb)
        os.mkfifo(self.testdb)
        self.assertRaisesMsg(CrawlDBI.DBIerror,
                             "disk I/O error",
                             CrawlDBI.DBI,
                             cfg=make_tcfg(self.dbtype))
    
    # -------------------------------------------------------------------------
    def test_ctor_dbn_nosuch(self):
        """
        File dbname does not exist -- initializing a db connection to it should
        create it.
        """
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        db.close()
        self.assertTrue(os.path.exists(self.testdb),
                        "Expected %s to exists but it does not" % self.testdb)
    
    # -------------------------------------------------------------------------
    def test_ctor_dbn_sock(self):
        """
        File dbname is a socket -- we throw an exception
        """
        util.conditional_rm(self.testdb)
        sockname = '%s/%s' % (self.testdir, util.my_name())
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(sockname)
        tcfg = make_tcfg(self.dbtype)
        tcfg.set('dbi', 'dbname', sockname)
        try:
            db = CrawlDBI.DBI(cfg=tcfg)
            self.fail("Expected exception was not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("unable to open database file" in str(e),
                            "Unexpected DBIerror thrown: %s" %
                            util.line_quote(tb.format_exc()))
    
    # -------------------------------------------------------------------------
    def test_ctor_dbn_sym_dir(self):
        """
        File dbname exists is a symlink. We should react to what the symlink
        points at. If it's a directory, we throw an exception.
        """
        # the symlink points at a directory
        util.conditional_rm(self.testdb + '_xyz')
        os.mkdir(self.testdb + '_xyz', 0777)
        os.symlink(os.path.basename(self.testdb + '_xyz'), self.testdb)
        try:
            db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
            self.fail("Expected exception was not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("unable to open database file" in str(e),
                            "Unexpected DBIerror thrown: %s" %
                            util.line_quote(tb.format_exc()))

    # -------------------------------------------------------------------------
    def test_ctor_dbn_sym_empty(self):
        """
        File dbname exists and is a symlink pointing at an empty file. We use
        it.
        """
        # the symlink points at a directory
        util.conditional_rm(self.testdb)
        util.conditional_rm(self.testdb + '_xyz')
        testhelp.touch(self.testdb + '_xyz')
        os.symlink(os.path.basename(self.testdb + '_xyz'), self.testdb)
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        db.create(table='testtab', fields=['froob text'])
        db.close

        self.assertTrue(os.path.exists(self.testdb + '_xyz'),
                        "Expected %s to exist" %
                        self.testdb + '_xyz')
        s = os.stat(self.testdb)
        self.assertNotEqual(0, s.st_size,
                            "Expected %s to contain some data" % self.testdb)

    # -------------------------------------------------------------------------
    def test_ctor_dbn_sym_nosuch(self):
        """
        File dbname exists and is a symlink pointing at a non-existent file. We
        create it.
        """
        # the symlink points at a non-existent file
        util.conditional_rm(self.testdb)
        util.conditional_rm(self.testdb + '_xyz')
        os.symlink(os.path.basename(self.testdb + '_xyz'), self.testdb)
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        db.create(table='testtab', fields=['froob text'])
        db.close

        self.assertTrue(os.path.exists(self.testdb + '_xyz'),
                        "Expected %s to exist" %
                        self.testdb + '_xyz')
        s = os.stat(self.testdb)
        self.assertNotEqual(0, s.st_size,
                            "Expected %s to contain some data" % self.testdb)
    
    # -------------------------------------------------------------------------
    def test_ctor_dbn_text(self):
        """
        File dbname exists and contains text. We should throw an exception
        """
        util.conditional_rm(self.testdb)
        f = open(self.testdb, 'w')
        f.write('This is a text file, not a database file\n')
        f.close()

        try:
            db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
            self.fail("Expected exception was not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("file is encrypted or is not a database" in str(e),
                            "Unexpected DBIerror thrown: %s" %
                            util.line_quote(tb.format_exc()))
    
    # -------------------------------------------------------------------------
    def test_ctor_sqlite_dbnreq(self):
        """
        The DBIsqlite ctor requires 'dbname' and 'tbl_prefix' as keyword
        arguments
        """
        try:
            a = CrawlDBI.DBIsqlite(tbl_prefix='xyzzy')
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            exp = "A database name is required"
            self.assertTrue(exp in str(e),
                            "Got the wrong DBIerror: %s" +
                            util.line_quote(str(e)))
            
    # -------------------------------------------------------------------------
    def test_ctor_sqlite_tbpreq(self):
        """
        The DBIsqlite ctor requires 'tbl_prefix' as keyword
        arguments
        """
        try:
            a = CrawlDBI.DBIsqlite(dbname='foobar')
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            exp = "A table prefix is required"
            self.assertTrue(exp in str(e),
                            "Got the wrong DBIerror: %s" +
                            util.line_quote(str(e)))
            
    # -------------------------------------------------------------------------
    def test_ctor_sqlite_other(self):
        """
        The DBIsqlite ctor takes only 'dbname' and 'tbl_prefix' as keyword
        arguments
        """
        try:
            a = CrawlDBI.DBIsqlite(dbname='foobar', tbl_prefix='xyzzy',
                                   something='fribble')
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            exp = "Attribute 'something' is not valid"
            self.assertTrue(exp in str(e),
                            "Got the wrong DBIerror: %s" +
                            util.line_quote(str(e)))
            
    # -------------------------------------------------------------------------
    def reset_db(self, name=''):
        util.conditional_rm(self.testdb)
        
# -----------------------------------------------------------------------------
@attr(slow=True)
class DBIdb2Test(DBI_in_Base, DBITestRoot):
    dbtype = 'db2'
    
    # -------------------------------------------------------------------------
    def test_select_f(self):
        """
        Calling select() specifying fields should get only the fields requested
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        rows = db.select(table='hpss.server',
                         fields=['desc_name', 'flags'])
        self.assertEqual(len(rows[0].keys()), 2,
                         "Expected two fields in each row, got %d" %
                         len(rows[0].keys()))
        for exp in ['FLAGS', 'DESC_NAME']:
            self.assertTrue(exp in rows[0].keys(),
                            "Expected key '%s' in each row, not found" %
                            exp)

    # -------------------------------------------------------------------------
    def test_select_gb_f(self):
        """
        Select with a group by clause on a field that is present in the table.
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        rows = db.select(table='hpss.logpolicy',
                         fields=['max(desc_name) as mdn', 'log_record_type_mask'],
                         groupby='log_record_type_mask')
        self.assertEqual(len(rows[0].keys()), 2,
                         "Expected two fields in each row, got %d" %
                         len(rows[0].keys()))
        for exp in ['MDN', 'LOG_RECORD_TYPE_MASK']:
            self.assertTrue(exp in rows[0].keys(),
                            "Expected key '%s' in each row, not found" %
                            exp)

    # -------------------------------------------------------------------------
    def test_select_gb_ns(self):
        """
        Select with a group by clause that is not a string -- should get an
        exception.
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        try:
            rows = db.select(table='hpss.logpolicy',
                             fields=['max(desc_name) as mdn',
                                     'log_record_type_mask'],
                             groupby=17)
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            exp = "On select(), groupby clause must be a string"
            self.assertTrue(exp in str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))

    # -------------------------------------------------------------------------
    def test_select_gb_u(self):
        """
        Select with a group by clause on a field that is unknown should get an
        exception.
        """
        self.exc_raises('select',
                            {'table': 'hpss.logpolicy',
                             'fields': ['max(desc_name) as mdn',
                                        'log_record_type_mask'],
                             'groupby': 'unknown_field'},
                            "\"UNKNOWN_FIELD\" is not valid in the " +
                            "context where it")
                             
        # db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        # try:
        #     rows = db.select(table='hpss.logpolicy',
        #                      fields=['max(desc_name) as mdn',
        #                              'log_record_type_mask'],
        #                      groupby='unknown_field')
        #     self.fail("Expected exception not thrown")
        # except CrawlDBI.DBIerror, e:
        #     exp = "\"UNKNOWN_FIELD\" is not valid in the context where it"
        #     self.assertTrue(exp in str(e),
        #                     "Got the wrong DBIerror: %s" %
        #                     util.line_quote(str(e)))

    # -------------------------------------------------------------------------
    #   - limit not an int
    #     > should throw exception
    def test_select_l_nint(self):
        self.exc_raises('select',
                            {'table': 'authzacl',
                             'limit': 'not an int'},
                            "On select(), limit must be an int")
        
    # -------------------------------------------------------------------------
    #   - limit is an int
    #     > should retrieve the specified number of records
    def test_select_l_int(self):
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        rlim = 3
        rows = db.select(table='hpss.server',
                         limit=rlim)
        self.expected(rlim, len(rows))
        # self.fail('under construction')
        
    # -------------------------------------------------------------------------
    #   - limit is a float
    #     > should convert to an int and use it
    def test_select_l_float(self):
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        rlim = 4.5
        rows = db.select(table='hpss.server',
                         limit=rlim)
        self.expected(int(rlim), len(rows))
        # self.fail('under construction')

    # -------------------------------------------------------------------------
    def test_select_mtf(self):
        """
        Calling select() with an empty field list should get all the data -- an
        empty field list indicates the wildcard option
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        rows = db.select(table='hpss.gatekeeper',
                         fields=[])
        self.expected(3, len(rows[0].keys()))
        for exp in ['GKID',
                    'DEFAULT_WAIT_TIME',
                    'SITE_POLICY_PATHNAME']:
            self.assertTrue(exp in rows[0].keys(),
                            "Expected key '%s' in each row, not found" %
                            exp)

    # -------------------------------------------------------------------------
    def test_select_nf(self):
        """
        Calling select() with no field list should get all the data -- fields
        should default to the empty list, indicating the wildcard option
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        mtf_rows = db.select(table='hpss.logclient', fields=[])
        nf_rows = db.select(table='hpss.logclient')
        
        self.expected(len(mtf_rows[0].keys()), len(nf_rows[0].keys()))
        self.expected(mtf_rows[0].keys(), nf_rows[0].keys())
        

    # -------------------------------------------------------------------------
    def test_select_mto(self):
        """
        Calling select() with an empty orderby should get the data in the
        same order as using no orderby at all.
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        ordered_rows = db.select(table='hpss.logclient', orderby='')
        unordered_rows = db.select(table='hpss.logclient')
        okl = [CrawlDBI.DBIdb2.hexstr(x['LOGC_SERVER_ID']) for x in ordered_rows]
        ukl = [CrawlDBI.DBIdb2.hexstr(x['LOGC_SERVER_ID']) for x in unordered_rows]
        self.expected(ukl, okl)

    # -------------------------------------------------------------------------
    def test_select_mtt(self):
        """
        Calling select() with an empty table name should get an exception
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        try:
            rows = db.select(table='',
                             fields=['max(desc_name) as mdn',
                                     'log_record_type_mask'],
                             groupby='unknown_field')
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            exp = "On select(), table name must not be empty"
            self.assertTrue(exp in str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))

    # -------------------------------------------------------------------------
    def test_select_mtw(self):
        """
        Calling select() with an empty where arg should get the same data as no
        where arg at all
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        w_rows = db.select(table='hpss.logpolicy', where='')
        x_rows = db.select(table='hpss.logpolicy')
        self.expected(len(x_rows), len(w_rows))
        for exp, actual in zip(x_rows, w_rows):
            self.expected(actual, exp)

    # -------------------------------------------------------------------------
    def test_select_nld(self):
        """
        Calling select() with a non-tuple as the data argument should
        get an exception
        """
        self.exc_raises('select',
                            {'table': 'hpss.logpolicy',
                             'fields': [],
                             'where': 'desc_name = ?',
                             'data': 'prudhoe'},
                            "On select(), data must be a tuple")

    # -------------------------------------------------------------------------
    def test_select_nlf(self):
        """
        Calling select() with a non-list as the fields argument should
        get an exception
        """
        self.exc_raises('select',
                            {'table': 'hpss.logpolicy',
                             'fields': 92,
                             'where': 'desc_name = ?',
                             'data': ('prudhoe',)},
                            "On select(), fields must be a list")

    # -------------------------------------------------------------------------
    def test_select_nq_ld(self):
        """
        Calling select() with where clause with no '?' and data in the list
        should get an exception -- the data would be ignored
        """
        self.exc_raises('select',
                            {'table': 'hpss.logpolicy',
                             'fields': [],
                             'where': 'desc_name = ""',
                             'data': ('prudhoe',)},
                            "Data would be ignored")

    # -------------------------------------------------------------------------
    def test_select_nq_mtd(self):
        """
        Calling select() with where with no '?' and an empty data list is fine.
        The data returned should match the where clause.
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        crit = 'Server'
        rows = db.select(table='hpss.server',
                         fields=['desc_name', 'rpc_prog_num', 'server_type'],
                         where="desc_name like '%%%s%%'" % crit,
                         data=())
        for x in rows:
            self.assertTrue(crit in x['DESC_NAME'],
                            "Expected '%s' in '%s' but it's missing" %
                            (crit, x['DESC_NAME']))

    # # -------------------------------------------------------------------------
    # def test_select_nq_mtd(self):
    #     """
    #     Calling select() with where with no '?' and an empty data list is fine.
    #     The data returned should match the where clause.
    #     """
    #     self.fail('under construction')

    # -------------------------------------------------------------------------
    def test_select_nso(self):
        """
        Calling select() with a non-string orderby argument should
        get an exception
        """
        self.exc_raises('select',
                            {'table': "hpss.logpolicy",
                             'orderby': 22},
                            "On select(), orderby clause must be a string")
                            
    # -------------------------------------------------------------------------
    def test_select_nst(self):
        """
        Calling select() with a non-string table argument should
        get an exception
        """
        self.exc_raises('select',
                            {'table': [],
                             'orderby': 22},
                            "On select(), table name must be a string")

    # -------------------------------------------------------------------------
    def test_select_nsw(self):
        """
        Calling select() with a non-string where argument should
        get an exception
        """
        self.exc_raises('select',
                            {'table': 'hpss.server',
                             'where': []},
                            "On select(), where clause must be a string")

    # -------------------------------------------------------------------------
    def test_select_o(self):
        """
        Calling select() specifying orderby should get the rows in the
        order requested
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        ordered_rows = db.select(table='hpss.logclient',
                                 orderby='logc_server_id')

        ford = [CrawlDBI.DBIdb2.hexstr(x['LOGC_SERVER_ID'])
                for x in ordered_rows]

        sord = sorted([CrawlDBI.DBIdb2.hexstr(x['LOGC_SERVER_ID'])
                                              for x in ordered_rows])
        
        self.expected(sord, ford)

    # -------------------------------------------------------------------------
    def test_select_q_ld(self):
        """
        Calling select() with a where clause containing '?' and data in the
        data list should return the data matching the where clause
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        crit = 'Server'
        rows = db.select(table='hpss.server',
                         fields=['desc_name', 'rpc_prog_num', 'server_type'],
                         where="desc_name like '%?%'",
                         data=(crit,))
        for x in rows:
            self.assertTrue(crit in x['DESC_NAME'],
                            "Expected '%s' in '%s' but it's missing" %
                            (crit, x['DESC_NAME']))

    # -------------------------------------------------------------------------
    def test_select_q_mtd(self):
        """
        Calling select() with a where clause with a '?' and an empty data list
        should get an exception
        """
        self.exc_raises('select',
                            {'table': "hpss.logpolicy",
                             'data': (),
                             'where': "DESC_NAME = ?"},
                            "0 params bound not matching 1 required")

    # -------------------------------------------------------------------------
    def test_select_w(self):
        """
        Calling select() specifying where should get only the rows requested
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        crit = 'Server'
        rows = db.select(table='hpss.server',
                         fields=['desc_name', 'rpc_prog_num', 'server_type'],
                         where="desc_name like '%%%s%%'" % crit)
        for x in rows:
            self.assertTrue(crit in x['DESC_NAME'],
                            "Expected '%s' in '%s' but it's missing" %
                            (crit, x['DESC_NAME']))

    # -------------------------------------------------------------------------
    def test_table_exists_yes(self):
        """
        For a table that exists, table_exists() should return True.
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        self.expected(True, db.table_exists(table='authzacl'))

    # -------------------------------------------------------------------------
    def test_table_exists_no(self):
        """
        For a table that does not exist, table_exists() should return False.
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        self.expected(False, db.table_exists(table='nonesuch'))

    # -------------------------------------------------------------------------
    def test_close_open(self):
        """
        Closing an open database should work.
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        db.close()
        self.expected(True, db._dbobj.closed)
        # self.fail('under construction')

    # -------------------------------------------------------------------------
    def test_close_closed(self):
        """
        Closing a closed database should generate an exception.
        """
        db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        db.close()
        try:
            db.close()
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            exp = "Cannot operate on a closed database"
            self.assertTrue(exp in str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))

    # -------------------------------------------------------------------------
    def test_repr(self):
        """
        What the human readable object format looks like
        """
        a = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
        b = CrawlDBI.DBIdb2(dbname='hcfg', tbl_prefix='hpss',
                            cfg=CrawlConfig.get_config())
        self.expected(str(b), str(a))
        # self.fail('under construction')

    # -------------------------------------------------------------------------
    def test_insert_exception(self):
        """
        On a db2 database, insert should throw an exception.
        """
        self.exc_raises("insert",
                            {'table': "hpss.bogus",
                             'data': [('a', 'b', 'c')]},
                            "INSERT not supported for DB2")
        # self.fail('under construction')
        
    # -------------------------------------------------------------------------
    def test_create_exception(self):
        """
        On a db2 database, create should throw an exception.
        """
        self.exc_raises("create",
                            {'table': "hpss.nonesuch",
                             'fields': self.fdef},
                            "CREATE not supported for DB2")
        # self.fail('under construction')
        
    # -------------------------------------------------------------------------
    def test_delete_exception(self):
        """
        On a db2 database, delete should throw an exception.
        """
        self.exc_raises("delete",
                            {'table': "hpss.bogus",
                             'data': [('a',)],
                             'where': "field = ?"},
                            "DELETE not supported for DB2")
        
    # -------------------------------------------------------------------------
    def test_drop_exception(self):
        """
        On a db2 database, drop should throw an exception.
        """
        self.exc_raises("drop",
                            {'table': "hpss.bogus"},
                            "DROP not supported for DB2")
        
    # -------------------------------------------------------------------------
    def test_update_exception(self):
        """
        On a db2 database, update should throw an exception.
        """
        self.exc_raises("update",
                            {'table': "hpss.bogus",
                             'fields': ['one', 'two'],
                             'data': [('a', 'b', 'c')],
                             'where': "one = ?"},
                            "UPDATE not supported for DB2")

    # # -------------------------------------------------------------------------
    # def exc_raises(self, funcname, kwargs, exp):
    #     db = CrawlDBI.DBI(cfg=make_tcfg(self.dbtype))
    #     try:
    #         if funcname == 'create':
    #             db.create(**kwargs)
    #         elif funcname == 'delete':
    #             db.delete(**kwargs)
    #         elif funcname == 'drop':
    #             db.drop(**kwargs)
    #         elif funcname == 'insert':
    #             db.insert(**kwargs)
    #         elif funcname == 'select':
    #             rows = db.select(**kwargs)
    #         elif funcname == 'update':
    #             db.update(**kwargs)
    #         else:
    #             raise StandardError("unsupported function name '%s'" %
    #                                 funcname)
    #         self.fail("Expected exception not thrown")
    #     except CrawlDBI.DBIerror, e:
    #         self.assertTrue(exp in str(e),
    #                         "Got the wrong DBIerror: %s" %
    #                         util.line_quote(str(e)))

# -----------------------------------------------------------------------------
if __name__ == '__main__':
    toolframe.ez_launch(test=['DBITest',
                              'DBIsqliteTest',
                              'DBImysqlTest',
                              'DBIdb2Test'],
                        logfile=testhelp.testlog(__name__))

                
