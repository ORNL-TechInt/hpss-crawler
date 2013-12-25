#!/usr/bin/env python
"""
Database interface classes
"""
import CrawlConfig
import CrawlDBI
import os
import socket
import sys
import testhelp
import toolframe
import traceback as tb
import util

mself = sys.modules[__name__]
logfile = "%s/crawl_test.log" % os.path.dirname(mself.__file__)

# -----------------------------------------------------------------------------
def setUpModule():
    """
    Set up for testing
    """
    testhelp.module_test_setup(DBIsqliteTest.testdir)

# -----------------------------------------------------------------------------
def tearDownModule():
    """
    Clean up after testing
    """
    testhelp.module_test_teardown(DBIsqliteTest.testdir)

# -----------------------------------------------------------------------------
class DBITest(testhelp.HelpedTestCase):
    """
    Tests for the DBI class
    """
    testdir = '%s/test.d' % os.path.dirname(mself.__file__)
    cfgfile = '%s/dbitest.cfg' % testdir
    testdb = '%s/test.db' % testdir
    
    # -------------------------------------------------------------------------
    def test_ctor_sqlite(self):
        """
        With a config object specifying sqlite as the database type, DBI should
        instantiate itself with an internal DBIsqlite object.
        """
        tcfg = CrawlConfig.CrawlConfig()
        tcfg.add_section('dbi')
        tcfg.set('dbi', 'dbtype', 'sqlite')
        a = CrawlDBI.DBI(dbname=self.testdb, cfg=tcfg)
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
        tcfg = CrawlConfig.CrawlConfig()
        tcfg.add_section('dbi')
        tcfg.set('dbi', 'dbtype', 'sqlite')
        a = CrawlDBI.DBI(dbname=self.testdb, cfg=tcfg)
        b = CrawlDBI.DBIsqlite(dbname=self.testdb)
        self.expected(str(b), str(a))

# -----------------------------------------------------------------------------
class DBItstBase(testhelp.HelpedTestCase):
    """
    Basic tests for the DBI<dbname> classes.

    Arranging the tests this way allows us to avoid having to write a complete,
    independent set of tests for each database type.

    Class DBIsqliteTest, which inherits the test methods from this one, will
    set the necessary parameters to select the sqlite database type so that
    when it is processed by the test running code, the tests will be run on an
    sqlite database. Similarly, DBImysqlTest will set the necessary parameters
    to select that database type, then run the inherited tests on a mysql
    database.
    """
    testdir = DBITest.testdir
    testdb = '%s/test.db' % testdir
    fdef = ['name text', 'size int', 'weight float']
    fnames = [x.split()[0] for x in fdef]
    testdata = [('frodo', 17, 108.5),
                ('zippo', 92, 12341.23),
                ('zumpy', 45, 9.3242)]
    
    # -------------------------------------------------------------------------
    def test_close(self):
        """
        Calling close() should free up the db resources and make the database
        handle unusable.
        """
        a = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        a.close()
        try:
            a.table_exists(table='dimension')
            self.fail("Expected exception on closed database not thrown")
        except CrawlDBI.DBIerror, e:
            exp = "Cannot operate on a closed database."
            self.assertTrue(exp in str(e),
                            "Expected '%s', got '%s'" % (exp, str(e)))
        except AssertionError:
            raise
    
    # -------------------------------------------------------------------------
    def test_create_mtf(self):
        """
        Calling create() with an empty field list should get an exception
        """
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        try:
            db.create(table='nogood', fields=[])
            self.fail("Expected exception on empty field list, not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On create(), fields must not be empty" in str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail('Expected DBIerror but got %s' %
                      util.line_quote(tb.format_exc()))

    # -------------------------------------------------------------------------
    def test_create_mtt(self):
        """
        Calling create() with an empty table name should get an exception
        """
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        try:
            db.create(table='', fields=['abc text'])
            self.fail("Expected exception on empty table name, not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On create(), table name must not be empty" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail('Expected DBIerror but got %s' %
                      util.line_quote(tb.format_exc()))

    # -------------------------------------------------------------------------
    def test_create_nlf(self):
        """
        Calling create() with a non-list as the fields argument should
        get an exception
        """
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        try:
            db.create(table='create_nlf', fields='notdict')
            self.fail("Expected exception on non-list fields, not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On create(), fields must be a list" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail('Expected DBIerror but got %s' %
                      util.line_quote(tb.format_exc()))

    # -------------------------------------------------------------------------
    def test_create_yes(self):
        """
        Calling create() with correct arguments should create the table
        """
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        db.create(table='create_yes', fields=['one text',
                                              'two int'])
        dbc = db.cursor()
        dbc.execute("""
        select name from sqlite_master where type='table' and name='create_yes'
        """)
        rows = dbc.fetchall()
        self.assertEqual(rows[0], ('create_yes',), 
                         "Table 'create_yes' should have been created," +
                         " was not.")

    # -------------------------------------------------------------------------
    def test_ctor_attrs(self):
        """
        Verify that a new object has the right attributes with the right
        default values
        """
        a = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        dirl = [q for q in dir(a) if not q.startswith('_')]
        xattr = ['close', 'create', 'dbname', 'delete', 'insert',
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
        try:
            a = CrawlDBI.DBI(dbname=self.testdb, badattr="frooble",
                             dbtype=self.dbtype)
            self.fail("Expected exception on bad attribute not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("Attribute 'badattr' is not valid" in str(e),
                            "Got the wrong DBIerror: %s" +
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except:
            self.fail("Got an unexpected exception: %s" +
                      util.line_quote(tb.format_exc()))
            
    # -------------------------------------------------------------------------
    def test_ctor_dbn_db(self):
        """
        File dbname exists and is a database file -- we will use it.
        """
        # first, we create a database file from scratch
        util.conditional_rm(self.testdb)
        tabname = util.my_name()
        dba = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        dba.create(table=tabname, fields=['field1 text'])
        dba.close()
        self.assertTrue(os.path.exists(self.testdb),
                        "Expected %s to exists but it does not" % self.testdb)
        s = os.stat(self.testdb)
        self.assertNotEqual(0, s.st_size,
                            "Expected %s to contain some data" % self.testdb)

        # now, when we try to access it, it should be there
        dbb = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
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
        try:
            db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
            self.fail("Expected exception was not thrown")
        except AssertionError:
            raise
        except CrawlDBI.DBIerror, e:
            self.assertTrue("unable to open database file" in str(e),
                            "Unexpected DBIerror thrown: %s" %
                            util.line_quote(tb.format_exc()))
    
    # -------------------------------------------------------------------------
    def test_ctor_dbn_empty(self):
        """
        File dbname exists and is empty -- we will use it as a database.
        """
        util.conditional_rm(self.testdb)
        testhelp.touch(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
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
        try:
            db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
            self.fail("Expected exception was not thrown")
        except AssertionError:
            raise
        except CrawlDBI.DBIerror, e:
            self.assertTrue("disk I/O error" in str(e),
                            "Unexpected DBIerror thrown: %s" %
                            util.line_quote(tb.format_exc()))
    
    # -------------------------------------------------------------------------
    def test_ctor_dbn_none(self):
        """
        Attempt to create an object with no dbname should get an exception
        """
        try:
            a = CrawlDBI.DBI(dbtype=self.dbtype)
            self.fail("Expected an exception but didn't get one")
        except CrawlDBI.DBIerror, e:
            gotx = True
            self.assertTrue("A database name is required" in str(e),
                            "Got the wrong DBIerror: " +
                            '"""\n%s\n"""' % str(e))
        except AssertionError:
            raise
        except:
            gotx = True
            self.fail("Got an unexpected exception: " +
                      '"""\n%s\n"""' % tb.format_exc())

    # -------------------------------------------------------------------------
    def test_ctor_dbn_nosuch(self):
        """
        File dbname does not exist -- initializing a db connection to it should
        create it.
        """
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
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
        try:
            db = CrawlDBI.DBI(dbname=sockname, dbtype=self.dbtype)
            self.fail("Expected exception was not thrown")
        except AssertionError:
            raise
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
        util.conditional_rm(self.testdb)
        os.mkdir(self.testdb + '_xyz', 0777)
        os.symlink(os.path.basename(self.testdb + '_xyz'), self.testdb)
        try:
            db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
            self.fail("Expected exception was not thrown")
        except AssertionError:
            raise
        except CrawlDBI.DBIerror, e:
            self.assertTrue("unable to open database file" in str(e),
                            "Unexpected DBIerror thrown: %s" %
                            util.line_quote(tb.format_exc()))

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
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        db.create(table='testtab', fields=['froob text'])
        db.close

        self.assertTrue(os.path.exists(self.testdb + '_xyz'),
                        "Expected %s to exist" %
                        self.testdb + '_xyz')
        s = os.stat(self.testdb)
        self.assertNotEqual(0, s.st_size,
                            "Expected %s to contain some data" % self.testdb)
    
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
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
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
            db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
            self.fail("Expected exception was not thrown")
        except AssertionError:
            raise
        except CrawlDBI.DBIerror, e:
            self.assertTrue("file is encrypted or is not a database" in str(e),
                            "Unexpected DBIerror thrown: %s" %
                            util.line_quote(tb.format_exc()))
    
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
        try:
            db.delete(table=td['tabname'], where='name=?')
            self.fail("Expected exception was not thrown")
        except AssertionError:
            raise
        except CrawlDBI.DBIerror, e:
            exp = "Criteria are not fully specified"
            self.assertTrue(exp in str(e),
                            "Expected '%s', got '%s'" % (exp, str(e)))
        
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
        try:
            db.delete(table=td['tabname'], where='name=foo', data=('meg',))
            self.fail("Expected exception was not thrown")
        except AssertionError:
            raise
        except CrawlDBI.DBIerror, e:
            exp = "Data would be ignored"
            self.assertTrue(exp in str(e),
                            "Expected '%s', got '%s'" % (exp, str(e)))
        
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
        try:
            db.delete(table='', where='name=?', data=('meg',))
            self.fail("Expected exception was not thrown")
        except AssertionError:
            raise
        except CrawlDBI.DBIerror, e:
            exp = "On delete(), table name must not be empty"
            self.assertTrue(exp in str(e),
                            "Expected '%s', got '%s'" % (exp, str(e)))
        
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
        try:
            db.delete(table=td['tabname'], where='name=?', data='meg')
            self.fail("Expected exception was not thrown")
        except AssertionError:
            raise
        except CrawlDBI.DBIerror, e:
            exp = "On delete(), data must be a tuple"
            self.assertTrue(exp in str(e),
                            "Expected '%s', got '%s'" % (exp, str(e)))
        
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
        try:
            db.delete(table=32, where='name=?', data='meg')
            self.fail("Expected exception was not thrown")
        except AssertionError:
            raise
        except CrawlDBI.DBIerror, e:
            exp = "On delete(), table name must be a string"
            self.assertTrue(exp in str(e),
                            "Expected '%s', got '%s'" % (exp, str(e)))
        
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
        try:
            db.delete(table=td['tabname'], where=[])
            self.fail("Expected exception was not thrown")
        except AssertionError:
            raise
        except CrawlDBI.DBIerror, e:
            exp = "On delete(), where clause must be a string"
            self.assertTrue(exp in str(e),
                            "Expected '%s', got '%s'" % (exp, str(e)))
                          
        
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
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        db.create(table='fnox', fields=['one text', 'two text'])
        try:
            db.insert(table='fnox',
                      fields=['one', 'two', 'three'],
                      data=[('abc', 'def', 99),
                            ('aardvark', 'buffalo', 78)])
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("table fnox has no column named three" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail("Expected DBIerror, got %s" %
                      util.line_quote(tb.format_exc()))
    
    # -------------------------------------------------------------------------
    def test_insert_mtd(self):
        """
        Calling insert with an empty data list should get an exception
        """
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        try:
            db.insert(table='mtd', fields=['one', 'two'], data=[])
            self.fail("Expected an exception but didn't get one")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On insert(), data list must not be empty" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail('Expected DBIerror but got %s' %
                      util.line_quote(tb.format_exc()))
    
    # -------------------------------------------------------------------------
    def test_insert_mtf(self):
        """
        Calling insert with an empty field list should get an exception
        """
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        try:
            db.insert(table='mtd', fields=[], data=[(1, 2)])
            self.fail("Expected an exception but didn't get one")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On insert(), fields list must not be empty" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail('Expected DBIerror but got %s' %
                      util.line_quote(tb.format_exc()))
    
    # -------------------------------------------------------------------------
    def test_insert_mtt(self):
        """
        Calling insert with an empty table name should get an exception
        """
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        try:
            db.insert(table='', fields=['one', 'two'], data=[(1, 2)])
            self.fail("Expected an exception but didn't get one")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On insert(), table name must not be empty" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail('Expected DBIerror but got %s' %
                      util.line_quote(tb.format_exc()))
    
    # -------------------------------------------------------------------------
    def test_insert_nst(self):
        """
        Calling insert with a non-string table name should get an exception
        """
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        try:
            db.insert(table=32, fields=['one', 'two'], data=[(1, 2)])
            self.fail("Expected an exception but didn't get one")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On insert(), table name must be a string" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail('Expected DBIerror but got %s' %
                      util.line_quote(tb.format_exc()))

    # -------------------------------------------------------------------------
    def test_insert_nlf(self):
        """
        Calling insert with a non-list fields arg should get an exception
        """
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        try:
            db.insert(table='nlf', fields='froo', data=[(1, 2)])
            self.fail("Expected an exception but didn't get one")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On insert(), fields must be a list" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail('Expected DBIerror but got %s' %
                      util.line_quote(tb.format_exc()))

    # -------------------------------------------------------------------------
    def test_insert_nld(self):
        """
        Calling insert with a non-list data arg should get an exception
        """
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        try:
            db.insert(table='nlf', fields=['froo', 'pizzazz'], data={})
            self.fail("Expected an exception but didn't get one")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On insert(), data must be a list" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail('Expected DBIerror but got %s' %
                      util.line_quote(tb.format_exc()))
    
    # -------------------------------------------------------------------------
    def test_insert_tnox(self):
        """
        Calling insert on a non-existent table should get an exception
        """
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        # db.create(table='tnox', fields=['one text', 'two text'])
        try:
            db.insert(table='tnox',
                      fields=['one', 'two'],
                      data=[('abc', 'def', 99),
                            ('aardvark', 'buffalo', 78)])
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("no such table: tnox" in str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail("Expected DBIerror, got %s" %
                      util.line_quote(tb.format_exc()))
    
    # -------------------------------------------------------------------------
    def test_insert_yes(self):
        """
        Calling insert with good arguments should put the data in the table
        """
        util.conditional_rm(self.testdb)

        tname=util.my_name().replace('test_', '')
        fdef = ['id int primary key', 'name text', 'size int']
        fnames = [x.split()[0] for x in fdef]
        testdata = [(1, 'sinbad', 54),
                    (2, 'zorro', 98)]

        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        db.create(table=tname, fields=fdef)
        db.insert(table=tname, fields=fnames, data=testdata)
                  
        dbc = db.cursor()
        dbc.execute("""
        select * from insert_yes
        """)
        rows = dbc.fetchall()
        for tup in testdata:
            self.assertTrue(tup in rows,
                            "Expected data %s not found in table" % str(tup))
    
    # -------------------------------------------------------------------------
    def test_select_f(self):
        """
        Calling select() specifying fields should get only the fields requested
        """
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        tname=util.my_name().replace('test_', '')
        fdef = ['name text', 'size int', 'weight float']
        fnames = [x.split()[0] for x in fdef]
        testdata = [('frodo', 17, 108.5),
                    ('zippo', 92, 12341.23),
                    ('zumpy', 45, 9.3242)]
        db.create(table=tname, fields=fdef)
        db.insert(table=tname, fields=fnames, data=testdata)

        rows = db.select(table=tname, fields=['size', 'weight'])
        self.assertEqual(len(rows[0]), 2,
                         "Expected two fields in each row, got %d" % len(rows[0]))
        for tup in testdata:
            self.assertTrue((tup[1], tup[2]) in rows,
                            "Expected %s in rows but it's not there" %
                            str((tup[1], tup[2])))
    

    # -------------------------------------------------------------------------
    def test_select_nq_mtd(self):
        """
        Calling select() with where with no '?' and an empty data list is fine.
        The data returned should match the where clause.
        """
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        tname=util.my_name().replace('test_', '')
        fdef = ['name text', 'size int', 'weight float']
        fnames = [x.split()[0] for x in fdef]
        testdata = [('frodo', 17, 108.5),
                    ('zippo', 92, 12341.23),
                    ('zumpy', 45, 9.3242)]
        db.create(table=tname, fields=fdef)
        db.insert(table=tname, fields=fnames, data=testdata)

        rows = db.select(table=tname, fields=[],
                         where='size = 92', data=())
        self.expected(1, len(rows))
        self.expected([testdata[1]], rows)

    # -------------------------------------------------------------------------
    def test_select_q_mtd(self):
        """
        Calling select() with a where clause with a '?' and an empty data list
        should get an exception
        """
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        tname=util.my_name().replace('test_', '')
        fdef = ['name text', 'size int', 'weight float']
        fnames = [x.split()[0] for x in fdef]
        testdata = [('frodo', 17, 108.5),
                    ('zippo', 92, 12341.23),
                    ('zumpy', 45, 9.3242)]
        db.create(table=tname, fields=fdef)
        db.insert(table=tname, fields=fnames, data=testdata)

        try:
            rows = db.select(table=tname, fields=[],
                             where='name = ?', data=())
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("Incorrect number of bindings supplied" in str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail("Expected DBIerror, got %s" %
                      util.line_quote(tb.format_exc()))

    # -------------------------------------------------------------------------
    def test_select_nq_ld(self):
        """
        Calling select() with where clause with no '?' and data in the list
        should get an exception -- the data would be ignored
        """
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        tname=util.my_name().replace('test_', '')
        fdef = ['name text', 'size int', 'weight float']
        fnames = [x.split()[0] for x in fdef]
        testdata = [('frodo', 17, 108.5),
                    ('zippo', 92, 12341.23),
                    ('zumpy', 45, 9.3242)]
        db.create(table=tname, fields=fdef)
        db.insert(table=tname, fields=fnames, data=testdata)

        try:
            rows = db.select(table=tname, fields=[],
                             where="name = 'zippo'", data=('frodo',))
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("Data would be ignored" in str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail("Expected DBIerror, got %s" %
                      util.line_quote(tb.format_exc()))

    # -------------------------------------------------------------------------
    def test_select_q_ld(self):
        """
        Calling select() with a where clause containing '?' and data in the
        data list should return the data matching the where clause
        """
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        tname=util.my_name().replace('test_', '')
        fdef = ['name text', 'size int', 'weight float']
        fnames = [x.split()[0] for x in fdef]
        testdata = [(unicode('frodo'), 17, 108.5),
                    (unicode('zippo'), 92, 12341.23),
                    (unicode('zumpy'), 45, 9.3242)]
        db.create(table=tname, fields=fdef)
        db.insert(table=tname, fields=fnames, data=testdata)

        rows = db.select(table=tname, fields=[],
                         where='name = ?', data=('zippo',))
        self.expected(1, len(rows))
        self.expected([testdata[1]], rows)

    # -------------------------------------------------------------------------
    def test_select_mtf(self):
        """
        Calling select() with an empty field list should get all the data -- an
        empty field list indicates the wildcard option
        """
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        tname=util.my_name().replace('test_', '')
        fdef = ['name text', 'size int', 'weight float']
        fnames = [x.split()[0] for x in fdef]
        testdata = [('frodo', 17, 108.5),
                    ('zippo', 92, 12341.23),
                    ('zumpy', 45, 9.3242)]
        db.create(table=tname, fields=fdef)
        db.insert(table=tname, fields=fnames, data=testdata)

        rows = db.select(table=tname, fields=[])
        self.assertEqual(len(rows[0]), 3,
                         "Expected three fields in each row, got %d" %
                         len(rows[0]))
        for tup in testdata:
            self.assertTrue(tup in rows,
                            "Expected %s in rows but it's not there" %
                            str(tup))
    
    # -------------------------------------------------------------------------
    def test_select_mto(self):
        """
        Calling select() with an empty orderby should get the data in the order
        inserted
        """
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        tname=util.my_name().replace('test_', '')
        fdef = ['name text', 'size int', 'weight float']
        fnames = [x.split()[0] for x in fdef]
        testdata = [('frodo', 17, 108.5),
                    ('zippo', 92, 12341.23),
                    ('zumpy', 45, 9.3242)]
        db.create(table=tname, fields=fdef)
        db.insert(table=tname, fields=fnames, data=testdata)

        rows = db.select(table=tname, fields=[], orderby='')
        self.assertEqual(len(rows[0]), 3,
                         "Expected three fields in each row, got %d" %
                         len(rows[0]))
        self.assertEqual(testdata, rows,
                         "Expected %s and %s to match" %
                         (str(testdata), str(rows)))
    
    # -------------------------------------------------------------------------
    def test_select_mtt(self):
        """
        Calling select() with an empty table name should get an exception
        """
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        tname=util.my_name().replace('test_', '')
        fdef = ['name text', 'size int', 'weight float']
        fnames = [x.split()[0] for x in fdef]
        testdata = [('frodo', 17, 108.5),
                    ('zippo', 92, 12341.23),
                    ('zumpy', 45, 9.3242)]
        db.create(table=tname, fields=fdef)
        db.insert(table=tname, fields=fnames, data=testdata)

        try:
            rows = db.select(table='', fields=[], orderby='')
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On select(), table name must not be empty" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail("Expected DBIerror, got %s" %
                      util.line_quote(tb.format_exc()))

    # -------------------------------------------------------------------------
    def test_select_mtw(self):
        """
        Calling select() with an empty where arg should get all the data
        """
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        tname=util.my_name().replace('test_', '')
        fdef = ['name text', 'size int', 'weight float']
        fnames = [x.split()[0] for x in fdef]
        testdata = [('frodo', 17, 108.5),
                    ('zippo', 92, 12341.23),
                    ('zumpy', 45, 9.3242)]
        db.create(table=tname, fields=fdef)
        db.insert(table=tname, fields=fnames, data=testdata)

        rows = db.select(table=tname, fields=[], where='')
        self.assertEqual(len(rows[0]), 3,
                         "Expected three fields in each row, got %d" %
                         len(rows[0]))
        self.assertEqual(testdata, rows,
                         "Expected %s and %s to match" %
                         (str(testdata), str(rows)))
    
    # -------------------------------------------------------------------------
    def test_select_nld(self):
        """
        Calling select() with a non-tuple as the data argument should
        get an exception
        """
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        tname=util.my_name().replace('test_', '')
        fdef = ['name text', 'size int', 'weight float']
        fnames = [x.split()[0] for x in fdef]
        testdata = [('frodo', 17, 108.5),
                    ('zippo', 92, 12341.23),
                    ('zumpy', 45, 9.3242)]
        db.create(table=tname, fields=fdef)
        db.insert(table=tname, fields=fnames, data=testdata)

        try:
            rows = db.select(table=tname, fields=[],
                             where='name = ?', data='zippo')
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On select(), data must be a tuple" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail("Expected DBIerror, got %s" %
                      util.line_quote(tb.format_exc()))
    
    # -------------------------------------------------------------------------
    def test_select_nlf(self):
        """
        Calling select() with a non-list as the fields argument should
        get an exception
        """
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        tname=util.my_name().replace('test_', '')
        fdef = ['name text', 'size int', 'weight float']
        fnames = [x.split()[0] for x in fdef]
        testdata = [('frodo', 17, 108.5),
                    ('zippo', 92, 12341.23),
                    ('zumpy', 45, 9.3242)]
        db.create(table=tname, fields=fdef)
        db.insert(table=tname, fields=fnames, data=testdata)

        try:
            rows = db.select(table=tname, fields=17)
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On select(), fields must be a list" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail("Expected DBIerror, got %s" %
                      util.line_quote(tb.format_exc()))
    
    # -------------------------------------------------------------------------
    def test_select_nso(self):
        """
        Calling select() with a non-string orderby argument should
        get an exception
        """
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        tname=util.my_name().replace('test_', '')
        fdef = ['name text', 'size int', 'weight float']
        fnames = [x.split()[0] for x in fdef]
        testdata = [('frodo', 17, 108.5),
                    ('zippo', 92, 12341.23),
                    ('zumpy', 45, 9.3242)]
        db.create(table=tname, fields=fdef)
        db.insert(table=tname, fields=fnames, data=testdata)

        try:
            rows = db.select(table=tname, fields=fnames, orderby=22)
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On select(), orderby clause must be a string" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail("Expected DBIerror, got %s" %
                      util.line_quote(tb.format_exc()))
    
    # -------------------------------------------------------------------------
    def test_select_nst(self):
        """
        Calling select() with a non-string table argument should
        get an exception
        """
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        tname=util.my_name().replace('test_', '')
        fdef = ['name text', 'size int', 'weight float']
        fnames = [x.split()[0] for x in fdef]
        testdata = [('frodo', 17, 108.5),
                    ('zippo', 92, 12341.23),
                    ('zumpy', 45, 9.3242)]
        db.create(table=tname, fields=fdef)
        db.insert(table=tname, fields=fnames, data=testdata)

        try:
            rows = db.select(table={}, fields=fnames)
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On select(), table name must be a string" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail("Expected DBIerror, got %s" %
                      util.line_quote(tb.format_exc()))
    
    # -------------------------------------------------------------------------
    def test_select_nsw(self):
        """
        Calling select() with a non-string where argument should
        get an exception
        """
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        tname=util.my_name().replace('test_', '')
        fdef = ['name text', 'size int', 'weight float']
        fnames = [x.split()[0] for x in fdef]
        testdata = [('frodo', 17, 108.5),
                    ('zippo', 92, 12341.23),
                    ('zumpy', 45, 9.3242)]
        db.create(table=tname, fields=fdef)
        db.insert(table=tname, fields=fnames, data=testdata)

        try:
            rows = db.select(table=tname, fields=fnames, where=22)
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On select(), where clause must be a string" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail("Expected DBIerror, got %s" %
                      util.line_quote(tb.format_exc()))
    
    # -------------------------------------------------------------------------
    def test_select_o(self):
        """
        Calling select() specifying orderby should get the rows in the
        order requested
        """
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        tname=util.my_name().replace('test_', '')
        fdef = ['name text', 'size int', 'weight float']
        fnames = [x.split()[0] for x in fdef]
        testdata = [('frodo', 17, 108.5),
                    ('zippo', 92, 12341.23),
                    ('zumpy', 45, 9.3242)]
        exp = [testdata[2], testdata[0], testdata[1]]
        db.create(table=tname, fields=fdef)
        db.insert(table=tname, fields=fnames, data=testdata)

        rows = db.select(table=tname, fields=[], orderby='weight')
        self.assertEqual(len(rows[0]), 3,
                         "Expected three fields in each row, got %d" %
                         len(rows[0]))
        self.assertEqual(exp, rows,
                         "Expected %s to match %s" % (str(exp), str(rows)))
    
    # -------------------------------------------------------------------------
    def test_select_w(self):
        """
        Calling select() specifying where should get only the rows requested
        """
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        tname=util.my_name().replace('test_', '')
        fdef = ['name text', 'size int', 'weight float']
        fnames = [x.split()[0] for x in fdef]
        testdata = [('frodo', 17, 108.5),
                    ('zippo', 92, 12341.23),
                    ('zumpy', 45, 9.3242)]
        exp = [testdata[0], testdata[2]]
        db.create(table=tname, fields=fdef)
        db.insert(table=tname, fields=fnames, data=testdata)

        rows = db.select(table=tname, fields=[], where="size < 50")
        self.assertEqual(len(rows[0]), 3,
                         "Expected three fields in each row, got %d" %
                         len(rows[0]))
        self.assertEqual(exp, rows,
                         "Expected %s to match %s" % (str(exp), str(rows)))
    
    # -------------------------------------------------------------------------
    def test_table_exists_yes(self):
        """
        If table foo exists, db.table_exists(table='foo') should return True
        """
        tname = util.my_name().replace('test_', '')
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        db.create(table=tname, fields=self.fdef)
        self.expected(True, db.table_exists(table=tname))

    # -------------------------------------------------------------------------
    def test_table_exists_no(self):
        """
        If table foo does not exist, db.table_exists(table='foo') should return
        False
        """
        tname = util.my_name().replace('test_', '')
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        self.expected(False, db.table_exists(table=tname))
        
    # -------------------------------------------------------------------------
    def test_update_f(self):
        """
        Calling update() specifying fields should update the fields requested
        """
        tname = util.my_name().replace('test_', '')
        udata = [('frodo', 23, 199.7),
                 ('zippo', 14, 201.3),
                 ('zumpy', 47, 202.1)]
        
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
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
    def test_update_mtd(self):
        """
        Calling update() with an empty data list should get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        try:
            db.update(table=tname, fields=self.fnames, data =[])
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On update(), data must not be empty" in str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail("Expected DBIerror, got %s" %
                      util.line_quote(tb.format_exc()))

    # -------------------------------------------------------------------------
    def test_update_mtf(self):
        """
        Calling update() with an empty field list should get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        try:
            db.update(table=tname, fields=[], data=self.testdata)
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On update(), fields must not be empty" in str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail("Expected DBIerror, got %s" %
                      util.line_quote(tb.format_exc()))
    
    # -------------------------------------------------------------------------
    def test_update_mtt(self):
        """
        Calling update() with an empty table name should get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        try:
            db.update(table='', fields=self.fnames, data=self.testdata)
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On update(), table name must not be empty" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail("Expected DBIerror, got %s" %
                      util.line_quote(tb.format_exc()))
    
    # -------------------------------------------------------------------------
    def test_update_mtw(self):
        """
        Calling update() with an empty where arg should update all the rows
        """
        tname = util.my_name().replace('test_', '')
        udata = [('frodo', 23, 199.7),
                 ('zippo', 14, 201.3),
                 ('zumpy', 47, 202.1)]
        
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
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
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        try:
            db.update(table=tname, fields=17, data=self.testdata)
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On update(), fields must be a list" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail("Expected DBIerror, got %s" %
                      util.line_quote(tb.format_exc()))
    
    # -------------------------------------------------------------------------
    def test_update_nld(self):
        """
        Calling update() with a non-list data argument should
        get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        try:
            db.update(table=tname, fields=self.fnames, data='notalist')
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On update(), data must be a list of tuples" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail("Expected DBIerror, got %s" %
                      util.line_quote(tb.format_exc()))
    
    # -------------------------------------------------------------------------
    def test_update_nst(self):
        """
        Calling update() with a non-string table argument should
        get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        try:
            db.update(table=38, fields=self.fnames, data=self.testdata)
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On update(), table name must be a string" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail("Expected DBIerror, got %s" %
                      util.line_quote(tb.format_exc()))
    
    # -------------------------------------------------------------------------
    def test_update_nsw(self):
        """
        Calling update() with a non-string where argument should
        get an exception
        """
        tname = util.my_name().replace('test_', '')
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        try:
            db.update(table=tname, fields=self.fnames, data=self.testdata,
                      where=[])
            self.fail("Expected exception not thrown")
        except CrawlDBI.DBIerror, e:
            self.assertTrue("On update(), where clause must be a string" in
                            str(e),
                            "Got the wrong DBIerror: %s" %
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except Exception, e:
            self.fail("Expected DBIerror, got %s" %
                      util.line_quote(tb.format_exc()))
    
    # -------------------------------------------------------------------------
    def test_update_w(self):
        """
        Calling update() specifying where should update only the rows requested
        """
        tname = util.my_name().replace('test_', '')
        udata = [('frodo', 23, 199.7),
                 ('zippo', 14, 201.3),
                 ('zumpy', 47, 202.1)]
        
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
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
        util.conditional_rm(self.testdb)
        flist = ['id integer primary key', 'name text', 'age int']
        testdata = {'tabname': 'test_table',
                    'flist': flist,
                    'ifields': [x.split()[0] for x in flist],
                    'rows': [(1, 'bob', 32),
                             (2, 'sam', 17),
                             (3, 'sally', 25),
                             (4, 'meg', 19),
                             (5, 'gertrude', 95)]}
        
        db = CrawlDBI.DBI(dbname=self.testdb, dbtype=self.dbtype)
        db.create(table=testdata['tabname'],
                  fields=testdata['flist'])
        db.insert(table=testdata['tabname'],
                  fields=testdata['ifields'],
                  data=testdata['rows'])
        return (db, testdata)
    
# -----------------------------------------------------------------------------
class DBIsqliteTest(DBItstBase):
    dbtype = 'sqlite'
    pass

# -----------------------------------------------------------------------------
if __name__ == '__main__':
    toolframe.ez_launch(test=['DBITest', 'DBIsqliteTest'],
                        logfile=logfile)

                