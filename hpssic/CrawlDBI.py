#!/usr/bin/env python
"""
Database interface classes
"""
import base64
import CrawlConfig
import pdb
import sqlite3
import sys
import util
import warnings

try:
    import ibm_db as db2
    import ibm_db_dbi
    db2_available = True
except ImportError:
    db2_available = False

try:
    import MySQLdb as mysql
    import _mysql_exceptions as mysql_exc
    mysql_available = True
except ImportError:
    mysql_available = False


HPSS_SECTION = 'dbi-hpss'
CRWL_SECTION = 'dbi-crawler'


# -----------------------------------------------------------------------------
class DBI_abstract(object):
    """
    Each of the specific database interface classes (DBIsqlite, DBImysql, etc.)
    inherit from this one
    """
    settable_attrl = ['cfg', 'dbname', 'host', 'username', 'password',
                      'tbl_prefix']

    # -------------------------------------------------------------------------
    def prefix(self, tabname):
        if self.tbl_prefix == '':
            return tabname

        if tabname.startswith(self.tbl_prefix):
            return tabname
        elif tabname.startswith('@'):
            return tabname[1:]
        else:
            return self.tbl_prefix + tabname


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

        # Check for invalid arguments in kwargs. Only 'cfg', 'dbtype', and
        # 'dbname' are accepted. Other classes may need to select 'hpss' for
        # the database type and if so will need to specify the database name,
        # but everything else should come from the configuration.

        for key in kwargs:
            if key not in ['cfg', 'dbtype', 'dbname']:
                raise DBIerror("Attribute '%s' is not valid for %s" %
                               (key, self.__class__))
        if 0 < len(args):
            if not isinstance(args[0], CrawlConfig.CrawlConfig):
                raise DBIerror("Unrecognized argument to %s. " %
                               self.__class__ +
                               "Only 'cfg=<config>' is accepted")
        if 'dbtype' in kwargs and kwargs['dbtype'] != 'db2':
            raise DBIerror("Only dbtype='db2' may be specified explicitly")

        if 'dbname' in kwargs and 'dbtype' not in kwargs:
            raise DBIerror("dbname may only be specified if dbtype = 'db2'")

        # Figure out the dbtype
        try:
            if 'dbtype' in kwargs and 'cfg' in kwargs:
                cfg = kwargs['cfg']
                dbtype = kwargs['dbtype']
                del kwargs['dbtype']
                tbl_pfx = 'hpss'
                dbname = kwargs['dbname']
            elif 'dbtype' not in kwargs and 'cfg' in kwargs:
                cfg = kwargs['cfg']
                dbtype = cfg.get('dbi', 'dbtype')
                tbl_pfx = cfg.get('dbi', 'tbl_prefix')
                dbname = cfg.get('dbi', 'dbname')
            elif 'dbtype' in kwargs and 'cfg' not in kwargs:
                cfg = CrawlConfig.get_config()
                dbtype = kwargs['dbtype']
                del kwargs['dbtype']
                tbl_pfx = 'hpss'
                dbname = kwargs['dbname']
            elif 'dbtype' not in kwargs and 'cfg' not in kwargs:
                cfg = CrawlConfig.get_config()
                dbtype = cfg.get('dbi', 'dbtype')
                tbl_pfx = cfg.get('dbi', 'tbl_prefix')
                dbname = cfg.get('dbi', 'dbname')
                kwargs['cfg'] = cfg
            else:
                cfg = args[0]
        elif 'cfg' in kwargs and kwargs['cfg'] is not None:
            cfg = kwargs['cfg']
        else:
            cfg = CrawlConfig.get_config()

        # pdb.set_trace()
        dbtype = ''
        dbname = ''
        tbl_pfx = ''
        if 'dbtype' not in kwargs:
            raise DBIerror("dbtype must be 'hpss' or 'crawler'")
        elif kwargs['dbtype'] == 'db2' or kwargs['dbtype'] == 'hpss':
            dbtype = cfg.get(HPSS_SECTION, 'dbtype')
            tbl_pfx = cfg.get(HPSS_SECTION, 'tbl_prefix')
            if 'dbname' not in kwargs:
                raise DBIerror("With dbtype=%s, dbname must be specified" %
                               dbtype)
            elif kwargs['dbname'] not in cfg.options(HPSS_SECTION):
                raise DBIerror("dbname %s not defined in the configuration" %
                               kwargs['dbname'])
            else:
                dbname = cfg.get(HPSS_SECTION, kwargs['dbname'])
        elif kwargs['dbtype'] == 'dbi' or kwargs['dbtype'] == 'crawler':
            if 'dbname' in kwargs:
                raise DBIerror("dbname may not be specified here")
            dbtype = cfg.get(CRWL_SECTION, 'dbtype')
            try:
                dbname = cfg.get(CRWL_SECTION, 'dbname')
            except CrawlConfig.NoOptionError, e:
                raise DBIerror(e)
            tbl_pfx = cfg.get(CRWL_SECTION, 'tbl_prefix')

        okw = {}
        okw['cfg'] = cfg
        okw['dbname'] = dbname
        okw['tbl_prefix'] = tbl_pfx

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
    def alter(self, **kwargs):
        """
        Alter a table as indicated by the arguments.
        Syntax:
          db.alter(table=<tabname>, addcol=<col desc>, pos='first|after <col>')
          db.alter(table=<tabname>, dropcol=<col name>)
        """
        return self._dbobj.alter(**kwargs)

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
    def describe(self, **kwargs):
        """
        Return a table description.
        """
        return self._dbobj.describe(**kwargs)

    # -------------------------------------------------------------------------
    def drop(self, **kwargs):
        """
        Drop the named table.
        """
        return self._dbobj.drop(**kwargs)

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
        sqlite: See DBI.__init__()
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

        if self.tbl_prefix != '':
            self.tbl_prefix = self.tbl_prefix.rstrip('_') + '_'
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
        sqlite: See DBI.__repr__()
        """
        rv = "DBIsqlite(dbname='%s')" % self.dbname
        return rv

    # -------------------------------------------------------------------------
    def alter(self, table='', addcol=None, dropcol=None, pos=None):
        """
        Sqlite ignores pos if it's set and does not support dropcol.
        """
        if type(table) != str:
            raise DBIerror("On alter(), table name must be a string",
                           dbname=self.dbname)
        elif table == '':
            raise DBIerror("On alter(), table name must not be empty",
                           dbname=self.dbname)
        elif dropcol is not None:
            raise DBIerror("SQLite does not support dropping columns",
                           dbname=self.dbname)
        elif addcol is None:
            raise DBIerror("ALTER requires an action")
        elif addcol.strip() == "":
            raise DBIerror("On alter(), addcol must not be empty")
        elif any([x in addcol for x in ['"', "'", ';', '=']]):
            raise DBIerror("Invalid addcol argument")

        try:
            cmd = ("alter table %s add column %s" %
                   (self.prefix(table), addcol))
            c = self.dbh.cursor()
            c.execute(cmd)
        except sqlite3.Error, e:
            raise DBIerror(''.join(e.args), dbname=self.dbname)

    # -------------------------------------------------------------------------
    def close(self):
        """
        sqlite: See DBI.close()
        """
        # Close the database connection
        try:
            self.dbh.close()
        # Convert any sqlite3 error into a DBIerror
        except sqlite3.Error, e:
            raise DBIerror(''.join(e.args), dbname=self.dbname)

    # -------------------------------------------------------------------------
    def create(self, table='', fields=[]):
        """
        sqlite: See DBI.create()
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
            cmd = ("create table %s(" % self.prefix(table) +
                   ", ".join(fields) +
                   ")")
            c = self.dbh.cursor()
            c.execute(cmd)
        # Convert any sqlite3 error into a DBIerror
        except sqlite3.Error, e:
            raise DBIerror(''.join(e.args), dbname=self.dbname)

    # -------------------------------------------------------------------------
    def cursor(self):
        """
        sqlite: See DBI.cursor()
        """
        try:
            rval = self.dbh.cursor()
            return rval
        except sqlite3.Error, e:
            raise DBIerror(''.join(e.args), dbname=self.dbname)

    # -------------------------------------------------------------------------
    def delete(self, table='', where='', data=()):
        """
        sqlite: See DBI.delete()
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
            cmd = "delete from %s" % self.prefix(table)
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
    def describe(self, table=''):
        """
        sqlite: Return the description of a table.
        """
        cmd = "pragma table_info(%s)" % self.prefix(table)
        c = self.dbh.cursor()
        c.execute(cmd)
        rows = c.fetchall()
        return rows

    # -------------------------------------------------------------------------
    def drop(self, table=''):
        """
        sqlite: See DBI.create()
        """
        # Handle bad arguments
        if type(table) != str:
            raise DBIerror("On drop(), table name must be a string",
                           dbname=self.dbname)
        elif table == '':
            raise DBIerror("On drop(), table name must not be empty",
                           dbname=self.dbname)

        # Construct and run the drop statement
        try:
            cmd = ("drop table %s" % self.prefix(table))
            c = self.dbh.cursor()
            c.execute(cmd)
        # Convert any sqlite3 error into a DBIerror
        except sqlite3.Error, e:
            raise DBIerror(''.join(e.args), dbname=self.dbname)

    # -------------------------------------------------------------------------
    def insert(self, table='', fields=[], data=[]):
        """
        sqlite: See DBI.insert()
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
            cmd = ("insert into %s(" % self.prefix(table) +
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
    def select(self, table='',
               fields=[],
               where='',
               data=(),
               groupby='',
               orderby='',
               limit=None):
        """
        sqlite: See DBI.select()
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
        elif type(groupby) != str:
            raise DBIerror("On select(), groupby clause must be a string",
                           dbname=self.dbname)
        elif type(orderby) != str:
            raise DBIerror("On select(), orderby clause must be a string",
                           dbname=self.dbname)
        elif '?' not in where and data != ():
            raise DBIerror("Data would be ignored",
                           dbname=self.dbname)
        elif limit is not None and type(limit) not in [int, float]:
            raise DBIerror("On select(), limit must be an int")

        # Build and run the select statement
        try:
            cmd = "select "
            if 0 < len(fields):
                cmd += ",".join(fields)
            else:
                cmd += "*"
            cmd += " from %s" % self.prefix(table)
            if where != '':
                cmd += " where %s" % where
            if groupby != '':
                cmd += " group by %s" % groupby
            if orderby != '':
                cmd += " order by %s" % orderby
            if limit is not None:
                cmd += " limit %d" % int(limit)

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
    def table_exists(self, table=''):
        """
        sqlite: See DBI.table_exists()
        """
        try:
            dbc = self.dbh.cursor()
            dbc.execute("""
                        select name from sqlite_master
                        where type='table'
                        and name=?
                        """, (self.prefix(table),))
            rows = dbc.fetchall()
            dbc.close()
            return 0 < len(rows)
        except sqlite3.Error, e:
            raise DBIerror(''.join(e.args), dbname=self.dbname)

    # -------------------------------------------------------------------------
    def update(self, table='', where='', fields=[], data=[]):
        """
        sqlite: See DBI.update()
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
        elif '"?"' in where or "'?'" in where:
            raise DBIerror("Parameter placeholders should not be quoted")

        # Build and run the update statement
        try:
            cmd = "update %s" % self.prefix(table)
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


if mysql_available:
    # -------------------------------------------------------------------------
    class DBImysql(DBI_abstract):
        # ---------------------------------------------------------------------
        def __init__(self, *args, **kwargs):
            """
            mysql: See DBI.__init__()
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

            if self.tbl_prefix != '':
                self.tbl_prefix = self.tbl_prefix.rstrip('_') + '_'
            cfg = kwargs['cfg']
            host = cfg.get('dbi', 'host')
            username = cfg.get('dbi', 'username')
            password = base64.b64decode(cfg.get('dbi', 'password'))
            try:
                self.dbh = mysql.connect(host=host,
                                         user=username,
                                         passwd=password,
                                         db=self.dbname)
                self.dbh.autocommit(True)
                self.closed = False
            except mysql_exc.Error, e:
                self.err_handler(e)

        # ---------------------------------------------------------------------
        def __repr__(self):
            """
            mysql: See DBI.__repr__()
            """
            rv = "DBImysql(dbname='%s')" % self.dbname
            return rv

        # ---------------------------------------------------------------------
        def err_handler(self, err):
            """
            mysql: Error handler
            """
            if isinstance(err, mysql_exc.ProgrammingError):
                raise DBIerror(str(err), dbname=self.dbname)
            elif 1 < len(err.args) and err.args[0] in [1047, 2003]:
                print("RETRY")
                time.sleep(self.sleeptime)
                self.sleeptime = min(2*self.sleeptime, 1.0)
            else:
                raise DBIerror("%d: %s" % err.args, dbname=self.dbname)

        # ---------------------------------------------------------------------
        def alter(self, table='', addcol=None, dropcol=None, pos=None):
            """
            mysql: Alter the table as indicated.
            """
            if self.closed:
                raise DBIerror("Cannot operate on a closed database",
                               dbname=self.dbname)
            cmd = ''
            if type(table) != str:
                raise DBIerror("On alter(), table name must be a string",
                               dbname=self.dbname)
            elif table == '':
                raise DBIerror("On alter(), table name must not be empty",
                               dbname=self.dbname)
            elif addcol is not None:
                if addcol.strip() == '':
                    raise DBIerror("On alter(), addcol must not be empty",
                                   dbname=self.dbname)
                if any([x in addcol for x in ['"', "'", ';', '=']]):
                    raise DBIerror("Invalid addcol argument")
                if pos:
                    cmd = ("alter table %s add column %s %s" %
                           (self.prefix(table), addcol, pos))
                else:
                    cmd = ("alter table %s add column %s" %
                           (self.prefix(table), addcol))
            elif dropcol is not None:
                if dropcol.strip() == '':
                    raise DBIerror("On alter, dropcol must not be empty",
                                   dbname=self.dbname)
                if any([x in dropcol for x in ['"', "'", ';', '=']]):
                    raise DBIerror("Invalid dropcol argument")
                cmd = ("alter table %s drop column %s" %
                       (self.prefix(table), dropcol))
            if cmd == '':
                raise DBIerror("ALTER requires an action")

            try:
                c = self.dbh.cursor()
                c.execute(cmd)
            except mysql_exc.Error, e:
                self.err_handler(e)

        # ---------------------------------------------------------------------
        def close(self):
            """
            mysql: See DBI.close()
            """
            # Close the database connection
            if self.closed:
                raise DBIerror("Cannot operate on a closed database.")
            try:
                self.dbh.close()
                self.closed = True
            # Convert any mysql error into a DBIerror
            except mysql_exc.Error, e:
                self.err_handler(e)

        # ---------------------------------------------------------------------
        def create(self, table='', fields=[]):
            """
            mysql: See DBI.create()
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
            mysql_f = [x.replace('autoincrement', 'auto_increment')
                       for x in fields]
            try:
                cmd = ("create table %s(" % self.prefix(table) +
                       ", ".join(mysql_f) +
                       ") engine = innodb")
                c = self.dbh.cursor()
                c.execute(cmd)

            # Convert any db specific error into a DBIerror
            except mysql_exc.Error, e:
                self.err_handler(e)

        # ---------------------------------------------------------------------
        def cursor(self):
            """
            mysql: get a cursor
            """
            try:
                rval = self.dbh.cursor()
                return rval
            except mysql_exc.Error, e:
                self.err_handler(e)

        # ---------------------------------------------------------------------
        def delete(self, table='', where='', data=()):
            """
            mysql: delete records
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
                cmd = "delete from %s" % self.prefix(table)
                if where != '':
                    cmd += " where %s" % where.replace('?', '%s')

                c = self.dbh.cursor()
                if '%s' in cmd:
                    c.execute(cmd, data)
                else:
                    c.execute(cmd)

                c.close()
            # Translate any db specific errors to DBIerror
            except mysql_exc.Error, e:
                self.err_handler(e)

        # ---------------------------------------------------------------------
        def describe(self, table=''):
            """
            mysql: Return a table description
            """
            try:
                cmd = """select column_name, ordinal_position, data_type
                             from information_schema.columns
                             where table_name = %s"""
                c = self.dbh.cursor()
                c.execute(cmd, (self.prefix(table),))
                r = c.fetchall()
                return r
            except mysql_exc.Error, e:
                self.err_handler(e)

        # ---------------------------------------------------------------------
        def drop(self, table=''):
            """
            Drop a mysql table
            """
            # Handle bad arguments
            if type(table) != str:
                raise DBIerror("On drop(), table name must be a string",
                               dbname=self.dbname)
            elif table == '':
                raise DBIerror("On drop(), table name must not be empty",
                               dbname=self.dbname)

            # Construct and run the drop statement
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore",
                                            "Unknown table '.*'")
                    cmd = ("drop table %s" % self.prefix(table))
                    c = self.dbh.cursor()
                    c.execute(cmd)

            # Convert any db specific error into a DBIerror
            except mysql_exc.Error, e:
                self.err_handler(e)

        # ---------------------------------------------------------------------
        def insert(self, table='', fields=[], data=[]):
            """
            Insert into a mysql database
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
                cmd = ("insert into %s(" % self.prefix(table) +
                       ",".join(fields) +
                       ") values (" +
                       ",".join(["%s" for x in fields]) +
                       ")")
                c = self.dbh.cursor()
                c.executemany(cmd, data)
                c.close()
            # Translate sqlite specific exception into a DBIerror
            except mysql_exc.Error, e:
                self.err_handler(e)

        # ---------------------------------------------------------------------
        def select(self,
                   table='',
                   fields=[],
                   where='',
                   data=(),
                   groupby='',
                   orderby='',
                   limit=None):
            """
            Select from a mysql database.
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
            elif type(groupby) != str:
                raise DBIerror("On select(), groupby clause must be a string",
                               dbname=self.dbname)
            elif type(orderby) != str:
                raise DBIerror("On select(), orderby clause must be a string",
                               dbname=self.dbname)
            elif '?' not in where and data != ():
                raise DBIerror("Data would be ignored",
                               dbname=self.dbname)
            elif limit is not None and type(limit) not in [int, float]:
                raise DBIerror("On select(), limit must be an int")

            # Build and run the select statement
            cmd = "select "
            if 0 < len(fields):
                cmd += ",".join(fields)
            else:
                cmd += "*"
            cmd += " from %s" % self.prefix(table)
            if where != '':
                cmd += " where %s" % where.replace('?', '%s')
            if groupby != '':
                cmd += " group by %s" % groupby
            if orderby != '':
                cmd += " order by %s" % orderby
            if limit is not None:
                cmd += " limit 0, %d" % int(limit)

            self.sleeptime = 0.1
            while True:
                try:
                    c = self.dbh.cursor()
                    if '%s' in cmd:
                        c.execute(cmd, data)
                    else:
                        c.execute(cmd)
                    rv = c.fetchall()
                    c.close()
                    return rv
                # Translate any sqlite3 errors to DBIerror
                except mysql_exc.Error, e:
                    self.err_handler(e)

        # ---------------------------------------------------------------------
        def table_exists(self, table=''):
            """
            Check whether a table exists in a mysql database
            """
            if self.closed:
                raise DBIerror("Cannot operate on a closed database.")
            try:
                dbc = self.dbh.cursor()
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore",
                                            "Can't read dir of .*")
                    dbc.execute("""
                                select table_name
                                from information_schema.tables
                                where table_name=%s
                                """, (self.prefix(table),))
                rows = dbc.fetchall()
                dbc.close()
                return 0 < len(rows)
            except mysql_exc.Error, e:
                self.err_handler(e)

        # ---------------------------------------------------------------------
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
            elif '"?"' in where or "'?'" in where:
                raise DBIerror("Parameter placeholders should not be quoted")

            # Build and run the update statement
            try:
                cmd = "update %s" % self.prefix(table)
                cmd += " set %s" % ",".join(["%s=" % x + "%s" for x in fields])
                if where != '':
                    cmd += " where %s" % where.replace('?', '%s')

                c = self.dbh.cursor()
                c.executemany(cmd, data)
                c.close()
            # Translate database-specific exceptions into DBIerrors
            except mysql_exc.Error, e:
                self.err_handler(e)


if db2_available:
    # -----------------------------------------------------------------------------
    class DBIdb2(DBI_abstract):
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
                self.tbl_prefix = 'hpss'

            if self.tbl_prefix != '':
                self.tbl_prefix = self.tbl_prefix.rstrip('.') + '.'
            try:
                cfg = kwargs['cfg']
            except:
                cfg = CrawlConfig.get_config()
            util.env_update(cfg)
            host = cfg.get('db2', 'hostname')
            port = cfg.get('db2', 'port')
            try:
                username = cfg.get('db2', 'username')
            except:
                username = ''
            try:
                password = base64.b64decode(cfg.get('db2', 'password'))
            except:
                password = ''
            try:
                cxnstr = ("database=%s;" % self.dbname +
                          "hostname=%s;" % host +
                          "port=%s;" % port +
                          "uid=%s;" % username +
                          "pwd=%s;" % password)
                self.dbh = db2.connect(cxnstr, "", "")
                self.closed = False
            except ibm_db_dbi.Error, e:
                raise DBIerror("%s" % str(e))

        # ---------------------------------------------------------------------
        def __repr__(self):
            """
            See DBI.__repr__()
            """
            rv = "DBIdb2(dbname='%s')" % self.dbname
            return rv

        # ---------------------------------------------------------------------
        def __recognized_exception__(self, exc):
            """
            Return True if exc is recognized as a DB2 error. Otherwise False.
            """
            try:
                q = self.db2_exc_list
            except AttributeError:
                self.db2_exc_list = ["params bound not matching",
                                     "SQLSTATE=",
                                     "[IBM][CLI Driver][DB2"]
            rval = False
            for x in self.db2_exc_list:
                if x in str(exc):
                    rval = True
                    break
            return rval

        # ---------------------------------------------------------------------
        def alter(self, table='', addcol=None, dropcol=None, pos=None):
            """
            See DBI.alter()
            """
            raise DBIerror("ALTER not supported for DB2")

        # ---------------------------------------------------------------------
        def close(self):
            """
            See DBI.close()
            """
            # Close the database connection
            if self.closed:
                raise DBIerror("Cannot operate on a closed database.")
            try:
                db2.close(self.dbh)
                # self.dbh.close()
                self.closed = True
            # Convert any mysql error into a DBIerror
            except ibm_db_dbi.Error, e:
                raise DBIerror("%d: %s" % e.args, dbname=self.dbname)

        # ---------------------------------------------------------------------
        def create(self, table='', fields=[]):
            """
            See DBI.create()
            """
            raise DBIerror("CREATE not supported for DB2")

        # ---------------------------------------------------------------------
        def cursor(self):
            """
            See DBI.cursor()
            """
            try:
                rval = self.dbh.cursor()
                return rval
            except mysql_exc.Error, e:
                raise DBIerror("%d: %s" % e.args, dbname=self.dbname)

        # ---------------------------------------------------------------------
        def delete(self, **kwargs):
            """
            See DBI.delete()
            """
            raise DBIerror("DELETE not supported for DB2")

        # ---------------------------------------------------------------------
        def describe(self, **kwargs):
            """
            Return a table description
            """
            pass   # yagni

        # ---------------------------------------------------------------------
        def drop(self, table=''):
            raise DBIerror("DROP not supported for DB2")

        # ---------------------------------------------------------------------
        def insert(self, table='', fields=[], data=[]):
            """
            Insert not supported for DB2
            """
            raise DBIerror("INSERT not supported for DB2")

        # ---------------------------------------------------------------------
        def select(self,
                   table='',
                   fields=[],
                   where='',
                   data=(),
                   groupby='',
                   orderby='',
                   limit=None):
            """
            Select from a DB2 database.
            """
            # Handle invalid arguments
            if type(table) != str and type(table) != list:
                raise DBIerror("On select(), table name must be " +
                               "a string or a list",
                               dbname=self.dbname)
            elif table == '' or table == []:
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
            elif type(groupby) != str:
                raise DBIerror("On select(), groupby clause must be a string",
                               dbname=self.dbname)
            elif type(orderby) != str:
                raise DBIerror("On select(), orderby clause must be a string",
                               dbname=self.dbname)
            elif '?' not in where and data != ():
                raise DBIerror("Data would be ignored",
                               dbname=self.dbname)
            elif limit is not None and type(limit) not in [int, float]:
                raise DBIerror("On select(), limit must be an int")

            # Build and run the select statement
            try:
                cmd = "select "
                if 0 < len(fields):
                    cmd += ",".join(fields)
                else:
                    cmd += "*"

                if type(table) == str:
                    cmd += " from %s" % self.prefix(table)
                elif type(table) == list:
                    cmd += " from %s" % ",".join([self.prefix(x)
                                                  for x in table])

                if where != '':
                    cmd += " where %s" % where
                if groupby != '':
                    cmd += " group by %s" % groupby
                if orderby != '':
                    cmd += " order by %s" % orderby
                if limit is not None:
                    cmd += " fetch first %d rows only" % int(limit)

                rval = []
                if '?' in cmd:
                    stmt = db2.prepare(self.dbh, cmd)
                    r = db2.execute(stmt, data)
                    x = db2.fetch_assoc(stmt)
                    while (x):
                        rval.append(x)
                        x = db2.fetch_assoc(stmt)
                else:
                    r = db2.exec_immediate(self.dbh, cmd)
                    x = db2.fetch_assoc(r)
                    while (x):
                        rval.append(x)
                        x = db2.fetch_assoc(r)

                return rval

            # Translate any db2 errors to DBIerror
            except ibm_db_dbi.Error, e:
                errmsg = str(e) + "\nSQL: '" + cmd + "'"
                raise DBIerror(errmsg, dbname=self.dbname)
            except Exception, e:
                if self.__recognized_exception__(e):
                    errmsg = str(e) + "\nSQL: '" + cmd + "'"
                    raise DBIerror(errmsg, dbname=self.dbname)
                else:
                    raise

        # ---------------------------------------------------------------------
        def table_exists(self, table=''):
            """
            Check whether a table exists in a db2 database
            """
            if self.closed:
                raise DBIerror("Cannot operate on a closed database.")
            try:
                rows = self.select(table="@syscat.tables",
                                   fields=['tabname'],
                                   where="tabschema = 'HPSS' and " +
                                   "tabname = '%s'" % table.upper())
                return 0 < len(rows)
            except mysql_exc.Error, e:
                raise DBIerror("%d: %s" % e.args, dbname=self.dbname)

        # ---------------------------------------------------------------------
        def update(self, table='', where='', fields=[], data=[]):
            """
            See DBI.update()
            """
            raise DBIerror("UPDATE not supported for DB2")

        # ---------------------------------------------------------------------
        @classmethod
        def hexstr(cls, bfid):
            """
            Convert a raw bitfile id into a hexadecimal string as presented by
            DB2.
            """
            rval = "x'" + DBIdb2.hexstr_uq(bfid) + "'"
            return rval

        @classmethod
        # ---------------------------------------------------------------------
        def hexstr_uq(cls, bfid):
            """
            Convert a raw bitfile id into an unquoted hexadecimal string as
            presented by DB2.
            """
            rval = "".join(["%02x" % ord(c) for c in list(bfid)])
            return rval.upper()
