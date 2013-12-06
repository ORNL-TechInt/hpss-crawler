#!/usr/bin/env python
"""
Database interface classes
"""
import CrawlConfig
import os
import shutil
import sqlite3
import testhelp
import toolframe
import traceback as tb
import util

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
class DBI_abstract(object):
    """
    Each of the specific database interface classes inherit from this one
    """
    settable_attrl = ['dbname', 'host', 'username', 'password']
    
# -----------------------------------------------------------------------------
class DBI(object):
    """
    This is the generic database interface object. The application uses this
    class so it doesn't have to know anything about talking to the database
    type actually in use.
    """
    # -------------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        """
        Here we look for configuration information indicating which kind of
        database to use. In the cfg arguement, the caller can pass us 1) the
        name of a configuration file, or 2) a CrawlConfig object. If the cfg
        argument is not present, we try to check 'crawl.cfg' as the default
        configuration file. Anything else is in the cfg argument will generate
        an exception.

        Once we know which kind of database to use, we create an object
        specific to that database type and forward calls from the caller to
        that object.

        The database-specific class should initialize a connection to the
        database, setting autocommit mode so that we don't have to commit every
        little thing we do. If our operational mode ever becomes complicated
        enough, we may need more control over commits but autocommit will do
        for now.
        """
        if 'cfg' not in kwargs:
            cfgname = 'crawl.cfg'
            cfg = CrawlConfig.get_config(cfname=cfgname)
        elif type(kwargs['cfg']) == str:
            cfgname = kwargs['cfg']
            cfg = CrawlConfig.get_config(cfname=cfgname)
            del kwargs['cfg']
        elif isinstance(kwargs['cfg'], CrawlConfig.CrawlConfig):
            cfg = kwargs['cfg']
            del kwargs['cfg']
        else:
            raise DBIerror('Invalid type for cfg arg to DBI constructor')

        try:
            dbtype = cfg.get('dbi', 'dbtype')
        except CrawlConfig.NoSectionError:
            dbtype = 'sqlite'
        except CrawlConfig.NoOptionError:
            dbtype = 'sqlite'
            
        if dbtype == 'sqlite':
            self.dbobj = DBIsqlite(*args, **kwargs)
        elif dbtype == 'mysql':
            self.dbobj = DBImysql(*args, **kwargs)
        elif dbtype == 'db2':
            self.dbobj = DBIdb2(*args, **kwargs)
        else:
            raise DBIerror("Unknown database type")
        
    # -------------------------------------------------------------------------
    def __repr__(self):
        """
        Human readable representation for the object provided by the
        database-specific class.
        """
        return self.dbobj.__repr__()
    
    # -------------------------------------------------------------------------
    def table_exists(self, **kwargs):
        """
        Return True if the table argument is not empty and the named table
        exists (even if the table itself is empty). Otherwise, return False.
        """
        return self.dbobj.table_exists(**kwargs)

    # -------------------------------------------------------------------------
    def close(self):
        """
        Close the connection to the database. After a call to close(),
        operations are not allowed on the database.
        """
        return self.dbobj.close()
    
    # -------------------------------------------------------------------------
    def create(self, **kwargs):
        """
        Create the named table containing the fields listed. The fields
        list contains column specifications, for example:

            ['id int primary key', 'name text', 'category xtext', ... ]
        """
        return self.dbobj.create(**kwargs)
    
    # -------------------------------------------------------------------------
    def insert(self, **kwargs):
        """
        Insert data into the table. Fields is a list of field names. Data is a
        list of tuples.
        """
        return self.dbobj.insert(**kwargs)
    
    # -------------------------------------------------------------------------
    def select(self, **kwargs):
        """
        Retrieve data from the table. Table name must be present. If fields is
        empty, all fields are selected.

        If the where argument is empty, all rows are selected and returned. If
        it contains an expression like 'id < 5', only the matching rows are
        selected. The where argument may contain something like 'name = ?', in
        which case data should be a tuple containing the matching value(s) for
        the where clause.

        If orderby is empty, the rows are returned in the order they are
        retrieved from the database. If orderby contains an field name, the
        rows are returned in that order.
        """
        return self.dbobj.select(**kwargs)
    
    # -------------------------------------------------------------------------
    def update(self, **kwargs):
        """
        Update data in the table. Where indicates which records are to be
        updated. Fields is a list of field names. Data is a list of tuples.
        """
        return self.dbobj.update(**kwargs)
    
# -----------------------------------------------------------------------------
class DBIerror(Exception):
    """
    This class is used to return DBI errors to the application so it doesn't
    have to know anything about specific error types associated with the
    various database types.
    """
    def __init__(self, value):
        """
        Set the value for the exception. It should be a string.
        """
        self.value = str(value)
    def __str__(self):
        """
        Report the exception value (should be a string).
        """
        return str(self.value)

# -----------------------------------------------------------------------------
class DBIsqlite(DBI_abstract):
    # -------------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        """
        See DBI.__init__()
        """
        for attr in kwargs:
            if attr in self.settable_attrl:
                setattr(self, attr, kwargs[attr])
            else:
                raise DBIerror("Attribute '%s'" % attr +
                               " is not valid for %s" % self.__class__)
        if not hasattr(self, 'dbname'):
            raise DBIerror("A database name is required")
        try:
            self.dbh = sqlite3.connect(self.dbname)
            # set autocommit mode
            self.dbh.isolation_level = None
        except sqlite3.Error, e:
            raise DBIerror(''.join(e.args))

    # -------------------------------------------------------------------------
    def __repr__(self):
        """
        See DBI.__repr__()
        """
        rv = "DBIsqlite(dbname='%s')" % self.dbname
        return rv

    # -------------------------------------------------------------------------
    def table_exists(self, table=''):
        """
        See DBI.table_exists()
        """
        try:
            dbc = self.dbh.cursor()
            dbc.execute("""
                        select name from sqlite_master
                        where type='table'
                        and name=?
                        """, (table,))
            rows = dbc.fetchall()
            dbc.close()
            return 0 < len(rows)
        except sqlite3.Error, e:
            raise DBIerror(''.join(e.args))

    # -------------------------------------------------------------------------
    def create(self, table='', fields=[]):
        """
        See DBI.create()
        """
        # Handle bad arguments
        if type(fields) != list:
            raise DBIerror("On create(), fields must be a list")
        elif fields == []:
            raise DBIerror("On create(), fields must not be empty")
        if type(table) != str:
            raise DBIerror("On create(), table name must be a string")
        elif table == '':
            raise DBIerror("On create(), table name must not be empty")

        # Construct and run the create statement
        try:
            cmd = ("create table %s(" % table + ",".join(fields) + ")")
            c = self.dbh.cursor()
            c.execute(cmd)
        # Convert any sqlite3 error into a DBIerror
        except sqlite3.Error, e:
            raise DBIerror(''.join(e.args))

    # -------------------------------------------------------------------------
    def close(self):
        """
        See DBI.close() 
        """
        # Close the database connection
        try:
            self.dbh.close()
        # Convert any sqlite3 error into a DBIerror
        except sqlite3.Error, e:
            raise DBIerror(''.join(e.args))
    
    # -------------------------------------------------------------------------
    def insert(self, table='', fields=[], data=[]):
        """
        See DBI.insert()
        """
        # Handle any bad arguments
        if type(table) != str:
            raise DBIerror("On insert(), table name must be a string")
        elif table == '':
            raise DBIerror("On insert(), table name must not be empty")
        elif type(fields) != list:
            raise DBIerror("On insert(), fields must be a list")
        elif fields == []:
            raise DBIerror("On insert(), fields list must not be empty")
        elif type(data) != list:
            raise DBIerror("On insert(), data must be a list")
        elif data == []:
            raise DBIerror("On insert(), data list must not be empty")

        # Construct and run the insert statement
        try:
            cmd = ("insert into %s(" % table +
                   ",".join(fields) +
                   ") values (" +
                   ",".join(["?" for x in fields]) +
                   ")")
            c = self.dbh.cursor()
            c.executemany(cmd, data)
            c.close()
        # Translate sqlite specific exception into a DBIerror
        except sqlite3.Error, e:
            raise DBIerror(''.join(e.args))
        
    # -------------------------------------------------------------------------
    def select(self, table='', fields=[], where='', data=(), orderby=''):
        """
        See DBI.select()
        """
        # Handle invalid arguments
        if type(table) != str:
            raise DBIerror("On select(), table name must be a string")
        elif table == '':
            raise DBIerror("On select(), table name must not be empty")
        elif type(fields) != list:
            raise DBIerror("On select(), fields must be a list")
        elif type(where) != str:
            raise DBIerror("On select(), where clause must be a string")
        elif type(data) != tuple:
            raise DBIerror("On select(), data must be a tuple")
        elif type(orderby) != str:
            raise DBIerror("On select(), orderby clause must be a string")
        elif '?' not in where and data != ():
            raise DBIerror("Data would be ignored")

        # Build and run the select statement 
        try:
            cmd = "select "
            if 0 < len(fields):
                cmd += ",".join(fields)
            else:
                cmd += "*"
            cmd += " from %s" % table
            if where != '':
                cmd += " where %s" % where
            if orderby != '':
                cmd += " order by %s" % orderby

            c = self.dbh.cursor()
            if '?' in cmd:
                c.execute(cmd, data)
            else:
                c.execute(cmd)
            rv = c.fetchall()
            c.close()
            return rv
        # Translate any sqlite3 errors to DBIerror
        except sqlite3.Error, e:
            raise DBIerror(''.join(e.args))

    # -------------------------------------------------------------------------
    def update(self, table='', where='', fields=[], data=[]):
        """
        See DBI.update()
        """
        # Handle invalid arguments
        if type(table) != str:
            raise DBIerror("On update(), table name must be a string")
        elif table == '':
            raise DBIerror("On update(), table name must not be empty")
        elif type(where) != str:
            raise DBIerror("On update(), where clause must be a string")
        elif type(fields) != list:
            raise DBIerror("On update(), fields must be a list")
        elif fields == []:
            raise DBIerror("On update(), fields must not be empty")
        elif type(data) != list:
            raise DBIerror("On update(), data must be a list of tuples")
        elif data == []:
            raise DBIerror("On update(), data must not be empty")

        # Build and run the update statement
        try:
            cmd = "update %s" % table
            cmd += " set %s" % ",".join(["%s=?" % x for x in fields])
            if where != '':
                cmd += " where %s" % where

            c = self.dbh.cursor()
            c.executemany(cmd, data)
            c.close()
        # Translate database-specific exceptions into DBIerrors
        except sqlite3.Error, e:
            raise DBIerror(''.join(e.args))

# -----------------------------------------------------------------------------
# class DBImysql(DBI):
#     pass

# -----------------------------------------------------------------------------
class DBITest(testhelp.HelpedTestCase):
    """
    Tests for the DBI class
    """
    testdir = 'test.d'
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
        a = DBI(dbname=self.testdb, cfg=tcfg)
        self.assertTrue(hasattr(a, 'dbobj'),
                        "Expected to find a dbobj attribute on %s" % a)
        self.assertTrue(isinstance(a.dbobj, DBIsqlite),
                        "Expected %s to be a DBIsqlite object" % a.dbobj)
        
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
        a = DBI(dbname=self.testdb, cfg=tcfg)
        b = DBIsqlite(dbname=self.testdb)
        self.expected(str(b), str(a))

# -----------------------------------------------------------------------------
class DBIsqliteTest(testhelp.HelpedTestCase):
    """
    Tests for the DBIsqlite class
    """
    testdir = 'test.d'
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
        a = DBIsqlite(dbname=self.testdb)
        a.close()
        try:
            a.table_exists(table='dimension')
            self.fail("Expected exception on closed database not thrown")
        except DBIerror, e:
            self.expected("Cannot operate on a closed database.", str(e))
            pass
        except AssertionError:
            raise
        except Exception, e:
            self.fail("Expected DBIerror, got %s" % type(e))
    
    # -------------------------------------------------------------------------
    def test_create_mtf(self):
        """
        Calling create() with an empty field list should get an exception
        """
        db = DBIsqlite(dbname=self.testdb)
        try:
            db.create(table='nogood', fields=[])
            self.fail("Expected exception on empty field list, not thrown")
        except DBIerror, e:
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
        db = DBIsqlite(dbname=self.testdb)
        try:
            db.create(table='', fields=['abc text'])
            self.fail("Expected exception on empty table name, not thrown")
        except DBIerror, e:
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
        db = DBIsqlite(dbname=self.testdb)
        try:
            db.create(table='create_nlf', fields='notdict')
            self.fail("Expected exception on non-list fields, not thrown")
        except DBIerror, e:
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
        self.reset_db()
        db = DBIsqlite(dbname=self.testdb)
        db.create(table='create_yes', fields=['one text',
                                              'two int'])
        dbc = db.dbh.cursor()
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
        a = DBIsqlite(dbname=self.testdb)
        dirl = [q for q in dir(a) if not q.startswith('__')]
        xattr = ['close', 'create', 'dbh', 'dbname', 'insert', 'select',
                 'settable_attrl', 'table_exists', 'update']

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
            a = DBIsqlite(dbname=self.testdb, badattr="frooble")
            self.fail("Expected exception on bad attribute not thrown")
        except DBIerror, e:
            self.assertTrue("Attribute 'badattr' is not valid" in str(e),
                            "Got the wrong DBIerror: %s" +
                            util.line_quote(str(e)))
        except AssertionError:
            raise
        except:
            self.fail("Got an unexpected exception: %s" +
                      util.line_quote(tb.format_exc()))
            
    # -------------------------------------------------------------------------
    def test_ctor_no_dbname(self):
        """
        Attempt to create an object with no dbname should get an exception
        """
        try:
            a = DBIsqlite()
            self.fail("Expected an exception but didn't get one")
        except DBIerror, e:
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
    def test_insert_fnox(self):
        """
        Calling insert on fields not in the table should get an exception
        """
        self.reset_db
        db = DBIsqlite(dbname=self.testdb)
        db.create(table='fnox', fields=['one text', 'two text'])
        try:
            db.insert(table='fnox',
                      fields=['one', 'two', 'three'],
                      data=[('abc', 'def', 99),
                            ('aardvark', 'buffalo', 78)])
            self.fail("Expected exception not thrown")
        except DBIerror, e:
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
        db = DBIsqlite(dbname=self.testdb)
        try:
            db.insert(table='mtd', fields=['one', 'two'], data=[])
            self.fail("Expected an exception but didn't get one")
        except DBIerror, e:
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
        db = DBIsqlite(dbname=self.testdb)
        try:
            db.insert(table='mtd', fields=[], data=[(1, 2)])
            self.fail("Expected an exception but didn't get one")
        except DBIerror, e:
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
        db = DBIsqlite(dbname=self.testdb)
        try:
            db.insert(table='', fields=['one', 'two'], data=[(1, 2)])
            self.fail("Expected an exception but didn't get one")
        except DBIerror, e:
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
        db = DBIsqlite(dbname=self.testdb)
        try:
            db.insert(table=32, fields=['one', 'two'], data=[(1, 2)])
            self.fail("Expected an exception but didn't get one")
        except DBIerror, e:
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
        db = DBIsqlite(dbname=self.testdb)
        try:
            db.insert(table='nlf', fields='froo', data=[(1, 2)])
            self.fail("Expected an exception but didn't get one")
        except DBIerror, e:
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
        db = DBIsqlite(dbname=self.testdb)
        try:
            db.insert(table='nlf', fields=['froo', 'pizzazz'], data={})
            self.fail("Expected an exception but didn't get one")
        except DBIerror, e:
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
        self.reset_db
        db = DBIsqlite(dbname=self.testdb)
        # db.create(table='tnox', fields=['one text', 'two text'])
        try:
            db.insert(table='tnox',
                      fields=['one', 'two'],
                      data=[('abc', 'def', 99),
                            ('aardvark', 'buffalo', 78)])
            self.fail("Expected exception not thrown")
        except DBIerror, e:
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
        self.reset_db()

        tname=util.my_name().replace('test_', '')
        fdef = ['id int primary key', 'name text', 'size int']
        fnames = [x.split()[0] for x in fdef]
        testdata = [(1, 'sinbad', 54),
                    (2, 'zorro', 98)]

        db = DBIsqlite(dbname=self.testdb)
        db.create(table=tname, fields=fdef)
        db.insert(table=tname, fields=fnames, data=testdata)
                  
        dbc = db.dbh.cursor()
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
        self.reset_db()
        db = DBIsqlite(dbname=self.testdb)
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
        self.reset_db()
        db = DBIsqlite(dbname=self.testdb)
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
        self.reset_db()
        db = DBIsqlite(dbname=self.testdb)
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
        except DBIerror, e:
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
        self.reset_db()
        db = DBIsqlite(dbname=self.testdb)
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
        except DBIerror, e:
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
        self.reset_db()
        db = DBIsqlite(dbname=self.testdb)
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
        self.reset_db()
        db = DBIsqlite(dbname=self.testdb)
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
        self.reset_db()
        db = DBIsqlite(dbname=self.testdb)
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
        self.reset_db()
        db = DBIsqlite(dbname=self.testdb)
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
        except DBIerror, e:
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
        self.reset_db()
        db = DBIsqlite(dbname=self.testdb)
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
        Calling select() with a non-list as the data argument should
        get an exception
        """
        self.reset_db()
        db = DBIsqlite(dbname=self.testdb)
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
        except DBIerror, e:
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
        self.reset_db()
        db = DBIsqlite(dbname=self.testdb)
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
        except DBIerror, e:
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
        self.reset_db()
        db = DBIsqlite(dbname=self.testdb)
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
        except DBIerror, e:
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
        self.reset_db()
        db = DBIsqlite(dbname=self.testdb)
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
        except DBIerror, e:
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
        self.reset_db()
        db = DBIsqlite(dbname=self.testdb)
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
        except DBIerror, e:
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
        self.reset_db()
        db = DBIsqlite(dbname=self.testdb)
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
        self.reset_db()
        db = DBIsqlite(dbname=self.testdb)
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
        self.reset_db
        db = DBIsqlite(dbname=self.testdb)
        db.create(table=tname, fields=self.fdef)
        self.expected(True, db.table_exists(tname))

    # -------------------------------------------------------------------------
    def test_table_exists_no(self):
        """
        If table foo does not exist, db.table_exists(table='foo') should return
        False
        """
        tname = util.my_name().replace('test_', '')
        self.reset_db
        db = DBIsqlite(dbname=self.testdb)
        self.expected(False, db.table_exists(tname))
        
    # -------------------------------------------------------------------------
    def test_update_f(self):
        """
        Calling update() specifying fields should update the fields requested
        """
        tname = util.my_name().replace('test_', '')
        udata = [('frodo', 23, 199.7),
                 ('zippo', 14, 201.3),
                 ('zumpy', 47, 202.1)]
        
        self.reset_db
        db = DBIsqlite(dbname=self.testdb)
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
        db = DBIsqlite(dbname=self.testdb)
        try:
            db.update(table=tname, fields=self.fnames, data =[])
            self.fail("Expected exception not thrown")
        except DBIerror, e:
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
        db = DBIsqlite(dbname=self.testdb)
        try:
            db.update(table=tname, fields=[], data=self.testdata)
            self.fail("Expected exception not thrown")
        except DBIerror, e:
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
        db = DBIsqlite(dbname=self.testdb)
        try:
            db.update(table='', fields=self.fnames, data=self.testdata)
            self.fail("Expected exception not thrown")
        except DBIerror, e:
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
        
        self.reset_db
        db = DBIsqlite(dbname=self.testdb)
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
        db = DBIsqlite(dbname=self.testdb)
        try:
            db.update(table=tname, fields=17, data=self.testdata)
            self.fail("Expected exception not thrown")
        except DBIerror, e:
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
        db = DBIsqlite(dbname=self.testdb)
        try:
            db.update(table=tname, fields=self.fnames, data='notalist')
            self.fail("Expected exception not thrown")
        except DBIerror, e:
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
        db = DBIsqlite(dbname=self.testdb)
        try:
            db.update(table=38, fields=self.fnames, data=self.testdata)
            self.fail("Expected exception not thrown")
        except DBIerror, e:
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
        db = DBIsqlite(dbname=self.testdb)
        try:
            db.update(table=tname, fields=self.fnames, data=self.testdata,
                      where=[])
            self.fail("Expected exception not thrown")
        except DBIerror, e:
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
        
        self.reset_db
        db = DBIsqlite(dbname=self.testdb)
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
    def reset_db(self):
        """
        Delete the database table so we can start over.
        """
        if os.path.exists(self.testdb):
            os.unlink(self.testdb)
        
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    toolframe.ez_launch(test=['DBITest', 'DBIsqliteTest'],
                        logfile='crawl_test.log')

                
