#!/usr/bin/env python
"""
Database interface classes
"""
import CrawlConfig
import sqlite3

# -----------------------------------------------------------------------------
class DBI_abstract(object):
    """
    Each of the specific database interface classes (DBIsqlite, DBImysql, etc.)
    inherit from this one
    """
    settable_attrl = ['dbname', 'host', 'username', 'password', 'tbl_prefix']
    
# -----------------------------------------------------------------------------
class DBI(object):
    """
    This is the generic database interface object. The application uses this
    class so it doesn't have to know anything about talking to the database
    type actually in use.

    When a DBI object is created, it looks for an argument named "dbtype" in
    kwargs that should contain "sqlite", "mysql", or "db2".

    The DBI creates an internal object of the appropriate type and then
    forwards all method calls to it.
    """
    # -------------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        """
        Here we look for configuration information indicating which kind of
        database to use. In the cfg argument, the caller can pass us 1) the
        name of a configuration file, or 2) a CrawlConfig object. If the cfg
        argument is not present, we try to check 'crawl.cfg' as the default
        configuration file. Anything else in the cfg argument (i.e., not a
        string and not a CrawlConfig object) will generate an exception.

        Once we know which kind of database to use, we create an object
        specific to that database type and forward calls from the caller to
        that object.

        The database-specific class should initialize a connection to the
        database, setting autocommit mode so that we don't have to commit every
        little thing we do. If our operational mode ever becomes complicated
        enough, we may need more control over commits but autocommit will do
        for now.
        """

        # Check for invalid arguments in kwargs. Only 'cfg' is accepted. Other
        # classes should not care about database name, database type, etc. That
        # should all come from the configuration.
        for key in kwargs:
            if key != 'cfg':
                raise DBIerror("Attribute '%s' is not valid for %s" %
                               (key, self.__class__))
        if 0 < len(args):
            if not isinstance(args[0], CrawlConfig.CrawlConfig):
                raise DBIerror("Unrecognized argument to %s. " % self.__class__ +
                               "Only 'cfg=<config>' is accepted")


        # Figure out the dbtype
        try:
            if 'cfg' in kwargs:
                cfg = kwargs['cfg']
                del kwargs['cfg']
                dbtype = cfg.get('dbi', 'dbtype')
                tbl_pfx = cfg.get('dbi', 'tbl_prefix')
                dbname = cfg.get('dbi', 'dbname')
            else:
                cfg = CrawlConfig.get_config()
                dbtype = cfg.get('dbi', 'dbtype')
                tbl_pfx = cfg.get('dbi', 'tbl_prefix')
                dbname = cfg.get('dbi', 'dbname')
        except CrawlConfig.NoSectionError:
            dbtype = 'sqlite'
        except CrawlConfig.NoOptionError:
            dbtype = 'sqlite'

        # Next, get the dbname and table prefix from the config
        try:
            dbname = cfg.get('dbi', 'dbname')
        except:
            raise DBIerror("A database name is required in configuration")

        try:
            tbl_pfx = cfg.get('dbi', 'tbl_prefix')
        except:
            raise DBIerror("A table prefix is required in configuration")
        
        kwargs['dbname'] = dbname
        kwargs['tbl_prefix'] = tbl_pfx
        if dbtype == 'sqlite':
            self._dbobj = DBIsqlite(*args, **kwargs)
        elif dbtype == 'mysql':
            self._dbobj = DBImysql(*args, **kwargs)
        elif dbtype == 'db2':
            self._dbobj = DBIdb2(*args, **kwargs)
        else:
            raise DBIerror("Unknown database type")

        self.dbname = self._dbobj.dbname

    # -------------------------------------------------------------------------
    def __repr__(self):
        """
        Human readable representation for the object provided by the
        database-specific class.
        """
        return self._dbobj.__repr__()
    
    # -------------------------------------------------------------------------
    def table_exists(self, **kwargs):
        """
        Return True if the table argument is not empty and the named table
        exists (even if the table itself is empty). Otherwise, return False.
        """
        return self._dbobj.table_exists(**kwargs)

    # -------------------------------------------------------------------------
    def close(self):
        """
        Close the connection to the database. After a call to close(),
        operations are not allowed on the database.
        """
        return self._dbobj.close()
    
    # -------------------------------------------------------------------------
    def create(self, **kwargs):
        """
        Create the named table containing the fields listed. The fields
        list contains column specifications, for example:

            ['id int primary key', 'name text', 'category xtext', ... ]
        """
        return self._dbobj.create(**kwargs)
    
    # -------------------------------------------------------------------------
    def cursor(self, **kwargs):
        """
        Return a database cursor
        """
        return self._dbobj.cursor(**kwargs)
    
    # -------------------------------------------------------------------------
    def delete(self, **kwargs):
        """
        Delete data from the table. table is a table name (string). where is a
        where clause (string). data is a tuple of fields.
        """
        return self._dbobj.delete(**kwargs)
    
    # -------------------------------------------------------------------------
    def insert(self, **kwargs):
        """
        Insert data into the table. Fields is a list of field names. Data is a
        list of tuples.
        """
        return self._dbobj.insert(**kwargs)
    
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
        return self._dbobj.select(**kwargs)
    
    # -------------------------------------------------------------------------
    def update(self, **kwargs):
        """
        Update data in the table. Where indicates which records are to be
        updated. Fields is a list of field names. Data is a list of tuples.
        """
        return self._dbobj.update(**kwargs)
    
# -----------------------------------------------------------------------------
class DBIerror(Exception):
    """
    This class is used to return DBI errors to the application so it doesn't
    have to know anything about specific error types associated with the
    various database types.
    """
    def __init__(self, value, dbname=None):
        """
        Set the value for the exception. It should be a string.
        """
        self.value = str(value)
        self.dbname = dbname
    def __str__(self):
        """
        Report the exception value (should be a string).
        """
        return "%s (dbname=%s)" % (str(self.value), self.dbname)

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
        if not hasattr(self, 'tbl_prefix'):
            raise DBIerror("A table prefix is required")
        try:
            self.dbh = sqlite3.connect(self.dbname)
            # set autocommit mode
            self.dbh.isolation_level = None
            self.table_exists(table="sqlite_master")
        except sqlite3.Error, e:
            raise DBIerror(''.join(e.args), dbname=self.dbname)

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
            raise DBIerror(''.join(e.args), dbname=self.dbname)

    # -------------------------------------------------------------------------
    def create(self, table='', fields=[]):
        """
        See DBI.create()
        """
        # Handle bad arguments
        if type(fields) != list:
            raise DBIerror("On create(), fields must be a list",
                           dbname=self.dbname)
        elif fields == []:
            raise DBIerror("On create(), fields must not be empty",
                           dbname=self.dbname)
        if type(table) != str:
            raise DBIerror("On create(), table name must be a string",
                           dbname=self.dbname)
        elif table == '':
            raise DBIerror("On create(), table name must not be empty",
                           dbname=self.dbname)

        # Construct and run the create statement
        try:
            cmd = ("create table if not exists %s(" % table +
                   ", ".join(fields) +
                   ")")
            c = self.dbh.cursor()
            c.execute(cmd)
        # Convert any sqlite3 error into a DBIerror
        except sqlite3.Error, e:
            raise DBIerror(''.join(e.args), dbname=self.dbname)

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
            raise DBIerror(''.join(e.args), dbname=self.dbname)
    
    # -------------------------------------------------------------------------
    def cursor(self):
        """
        See DBI.cursor()
        """
        try:
            rval = self.dbh.cursor()
            return rval
        except sqlite3.Error, e:
            raise DBIerror(''.join(e.args), dbname=self.dbname)

    # -------------------------------------------------------------------------
    def delete(self, table='', where='', data=()):
        """
        See DBI.delete()
        """
        # Handle invalid arguments
        if type(table) != str:
            raise DBIerror("On delete(), table name must be a string",
                           dbname=self.dbname)
        elif table == '':
            raise DBIerror("On delete(), table name must not be empty",
                           dbname=self.dbname)
        elif type(where) != str:
            raise DBIerror("On delete(), where clause must be a string",
                           dbname=self.dbname)
        elif type(data) != tuple:
            raise DBIerror("On delete(), data must be a tuple",
                           dbname=self.dbname)
        elif '?' not in where and data != ():
            raise DBIerror("Data would be ignored", dbname=self.dbname)
        elif '?' in where and data == ():
            raise DBIerror("Criteria are not fully specified",
                           dbname=self.dbname)

        # Build and run the select statement 
        try:
            cmd = "delete from %s" % table
            if where != '':
                cmd += " where %s" % where

            c = self.dbh.cursor()
            if '?' in cmd:
                c.execute(cmd, data)
            else:
                c.execute(cmd)

            c.close()
        # Translate any sqlite3 errors to DBIerror
        except sqlite3.Error, e:
            raise DBIerror(cmd + ': ' + ''.join(e.args), dbname=self.dbname)

    # -------------------------------------------------------------------------
    def insert(self, table='', fields=[], data=[]):
        """
        See DBI.insert()
        """
        # Handle any bad arguments
        if type(table) != str:
            raise DBIerror("On insert(), table name must be a string",
                           dbname=self.dbname)
        elif table == '':
            raise DBIerror("On insert(), table name must not be empty",
                           dbname=self.dbname)
        elif type(fields) != list:
            raise DBIerror("On insert(), fields must be a list",
                           dbname=self.dbname)
        elif fields == []:
            raise DBIerror("On insert(), fields list must not be empty",
                           dbname=self.dbname)
        elif type(data) != list:
            raise DBIerror("On insert(), data must be a list",
                           dbname=self.dbname)
        elif data == []:
            raise DBIerror("On insert(), data list must not be empty",
                           dbname=self.dbname)

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
            raise DBIerror(cmd + ": " + ''.join(e.args),
                           dbname=self.dbname)
        
    # -------------------------------------------------------------------------
    def select(self, table='', fields=[], where='', data=(), orderby=''):
        """
        See DBI.select()
        """
        # Handle invalid arguments
        if type(table) != str:
            raise DBIerror("On select(), table name must be a string",
                           dbname=self.dbname)
        elif table == '':
            raise DBIerror("On select(), table name must not be empty",
                           dbname=self.dbname)
        elif type(fields) != list:
            raise DBIerror("On select(), fields must be a list",
                           dbname=self.dbname)
        elif type(where) != str:
            raise DBIerror("On select(), where clause must be a string",
                           dbname=self.dbname)
        elif type(data) != tuple:
            raise DBIerror("On select(), data must be a tuple",
                           dbname=self.dbname)
        elif type(orderby) != str:
            raise DBIerror("On select(), orderby clause must be a string",
                           dbname=self.dbname)
        elif '?' not in where and data != ():
            raise DBIerror("Data would be ignored",
                           dbname=self.dbname)

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
            raise DBIerror(''.join(e.args),
                           dbname=self.dbname)

    # -------------------------------------------------------------------------
    def update(self, table='', where='', fields=[], data=[]):
        """
        See DBI.update()
        """
        # Handle invalid arguments
        if type(table) != str:
            raise DBIerror("On update(), table name must be a string",
                           dbname=self.dbname)
        elif table == '':
            raise DBIerror("On update(), table name must not be empty",
                           dbname=self.dbname)
        elif type(where) != str:
            raise DBIerror("On update(), where clause must be a string",
                           dbname=self.dbname)
        elif type(fields) != list:
            raise DBIerror("On update(), fields must be a list",
                           dbname=self.dbname)
        elif fields == []:
            raise DBIerror("On update(), fields must not be empty",
                           dbname=self.dbname)
        elif type(data) != list:
            raise DBIerror("On update(), data must be a list of tuples",
                           dbname=self.dbname)
        elif data == []:
            raise DBIerror("On update(), data must not be empty",
                           dbname=self.dbname)

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
            raise DBIerror(''.join(e.args),
                           dbname=self.dbname)

# -----------------------------------------------------------------------------
# class DBImysql(DBI):
#     pass
