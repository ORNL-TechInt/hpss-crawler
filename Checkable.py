#!/bin/env python

import os
import pdb
import sqlite3 as sql
import stat
import sys
import testhelp
import time
import traceback as tb
import unittest

# -----------------------------------------------------------------------------
class Checkable(object):
    dbname = ''
    # -------------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        self.dbname = Checkable.dbname
        self.path = '---'
        self.last_check = 0
        self.type = '-'
        self.rowid = None
        self.args = args
        for k in kwargs:
            if k not in ['rowid', 'path', 'type', 'last_check']:
                raise StandardError("Attribute %s is invalid for Checkable" %
                                    k)
            setattr(self, k, kwargs[k])
        super(Checkable, self).__init__()
        
    # -------------------------------------------------------------------------
    def __eq__(self, other):
        return (isinstance(other, self.__class__) and
                (self.path == other.path) and
                (self.type == other.type))

    # -------------------------------------------------------------------------
    @classmethod
    def ex_nihilo(cls, filename='drill.db'):
        """
        Start from scratch. The first thing we want to do is to add "/" to the
        queue, then take that Checkable object and call its .check() method.
        """
        Checkable.dbname = filename
        try:
            if not os.path.exists(filename):
                db = sql.connect(filename)
                cx = db.cursor()
                cx.execute("""create table checkables(id int primary key,
                                                      path text,
                                                      type text,
                                                      last_check int)""")
                cx.execute("""insert into checkables(path, type, last_check)
                                          values('/', 'd', 0)""")
                db.commit()
                db.close()
        except sql.Error, e:
            print("SQLite Error: %s" % str(e))

    # -------------------------------------------------------------------------
    @classmethod
    def get_list(cls, filename='drill.db'):
        """
        Return the current list of checkables.
        """
        Checkable.dbname = filename
        rval = []
        try:
            if not os.path.exists(filename):
                raise StandardError("Please call .ex_nihilo() first")
            db = sql.connect(filename)
            cx = db.cursor()
            cx.execute('select rowid, path, type, last_check' +
                       ' from checkables order by rowid')
            rows = cx.fetchall()
            for row in rows:
                new = Checkable(rowid=row[0], path=row[1], type=row[2],
                                last_check=row[3])
                rval.append(new)
            db.close()
            return rval
        except sql.Error, e:
            print("SQLite Error: %s" % str(e))

    # -------------------------------------------------------------------------
    def check(self):
        """
        For a directory:
         - get a list of its contents if possible,
         - create a Checkable object for each item and persist it to the
           database
        For a file:
         - if it's readable, decide whether to make it an official check
         - if so, do all the check steps for the file
         - update the time of last check
         - persist the object to the database
        """
        pass

    # -------------------------------------------------------------------------
    def persist(self):
        """
        Update the object's entry in the database.

        There are two situations where we need to push data into the database.

        1) We have found a new object to track and want to add it to the
           database. However, if the path matches an entry already in the
           database, we don't want to add a duplicate. Rather, we should update
           the one already there. So this will usually be an insert but may
           sometimes be an update by path. In this case, we'll have the
           following incoming values:

           rowid: None
           path: ...
           type: ...
           last_check: 0

        2) I have just checked an existing object and I want to update its
           last_check time. In this case, we'll have the following incoming values:

           rowid: != None
           path: ...
           type: ...
           last_check: != 0

        In any other case, we'll throw an exception.
        """
        try:
            db = sql.connect(self.dbname)
            cx = db.cursor()

            if self.rowid != None and self.last_check != 0.0:
                # Updating the check time for an existing object 
                cx.execute("update checkables set last_check=? where rowid=?",
                           (self.last_check, self.rowid))
            elif self.rowid == None and self.last_check == 0.0:
                # Adding (or perhaps updating) a new checkable
                cx.execute("select * from checkables where path=?",
                           (self.path,))
                rows = cx.fetchall()
                if 0 == len(rows):
                    # path not in db -- we insert it
                    cx.execute("""insert into checkables(path, type, last_check)
                                  values(?, ?, ?)""",
                               (self.path, self.type, self.last_check))
                elif 1 == len(rows):
                    # path is in db -- we update it
                    cx.execute("""update checkables set type=?, last_check=?
                                  where path=?""",
                               (self.type, self.last_check, self.path))
                else:
                    raise StandardError("There seems to be more than one"
                                        + " occurrence of '%s' in the database" %
                                        self.path)
            else:
                raise StandardError("Invalid conditions: "
                                    + "rowid = %s; " % str(self.rowid)
                                    + "path = '%s'; " % self.path
                                    + "type = '%s'; " % self.type
                                    + "last_check = %f" % self.last_check)

            db.commit()
            db.close()
        except sql.Error, e:
            print("SQLite Error: %s" % str(e))

# -----------------------------------------------------------------------------
class CheckableTest(unittest.TestCase):
    testfile = 'test.db'
    methods = ['__init__', 'ex_nihilo', 'get_list', 'check', 'persist']
    
    # -------------------------------------------------------------------------
    def test_check_dir(self):
        """
        Calling .check() on a directory should give us back a list of Checkable
        objects representing the entries in the directory
        """
        raise testhelp.UnderConstructionError('under construction')
    
    # -------------------------------------------------------------------------
    def test_check_file(self):
        """
        Calling .check() on a file should execute the check actions for that
        file.
        """
        raise testhelp.UnderConstructionError('under construction')
    
    # -------------------------------------------------------------------------
    def test_ctor(self):
        """
        Verify that the constructor gives us an object with the right methods
        """
        x = Checkable()
        for method in self.methods:
            self.assertEqual(method in dir(x), True,
                             "Checkable object is missing %s method" % method)
        self.assertEqual(x.path, '---',
                         "Unspecified path should be '---', got '%s'" % x.path)
        self.assertEqual(x.rowid, None,
                         "For a Checkable that did not come out of the db, " +
                         "rowid should be None")

    # -------------------------------------------------------------------------
    def test_ctor_args(self):
        """
        Verify that the constructor accepts and sets path, type, and last_check
        """
        x = Checkable(path='/one/two/three', type='f', last_check=72)
        for method in self.methods:
            self.assertEqual(method in dir(x), True,
                         "Checkable object is missing %s method" % method)
        self.assertEqual(x.path, '/one/two/three',
                         "Path should be '/one/two/three', got '%s'" % x.path)
        self.assertEqual(x.type, 'f',
                         "Type should be 'f', got '%s'" % x.path)
        self.assertEqual(x.last_check, 72,
                         "last_check should be 72, got '%d'" % x.last_check)
            
    # -------------------------------------------------------------------------
    def test_ctor_bad_args(self):
        """
        Verify that the constructor accepts and sets path, type, and last_check
        """
        try:
            got_exception = False
            x = Checkable(path_x='/one/two/three', type='f', last_check=72)
        except StandardError, e:
            got_exception = True
        except Exception, e:
            tb.print_exc()
        self.assertEqual(got_exception, True,
                         "Expected an exception but didn't get one.")
            
    # -------------------------------------------------------------------------
    def test_ex_nihilo_scratch(self):
        """
        If the database file does not already exist, calling ex_nihilo() should
        create it.
        """
        if os.path.exists(self.testfile):
            os.unlink(self.testfile)
        Checkable.ex_nihilo(filename=self.testfile)
        self.assertEqual(os.path.exists(self.testfile), True,
                         "File '%s' should be created by ex_nihilo()" %
                         (self.testfile))
        db = sql.connect(self.testfile)
        cx = db.cursor()
        cx.execute('select * from checkables')
        rows = cx.fetchall()
        self.assertEqual(len(rows), 1,
                         "Expected 1 row in table checkables, found %d" %
                         len(rows))
        cx.execute('select max(rowid) from checkables')
        max_id = cx.fetchone()[0]
        self.assertEqual(max_id, 1,
                         "Expected last row id to be 1, but is %d" %
                         max_id)
        self.assertEqual(rows[0][1], '/',
                         "Expected path '/' but got '%s' instead" % rows[0][1])
        self.assertEqual(rows[0][2], 'd',
                         "Expected type 'd' but got '%s' instead" % rows[0][2])
        self.assertEqual(rows[0][3], 0,
                         "Expected last_check to be 0 but got '%s' instead" %
                         rows[0][3])
        
    # -------------------------------------------------------------------------
    def test_ex_nihilo_exist(self):
        """
        If the database file does already exist, calling ex_nihilo() should do
        nothing.
        """
        if os.path.exists(self.testfile):
            os.unlink(self.testfile)
        testhelp.touch(self.testfile)
        s = os.stat(self.testfile)
        newtime = s[stat.ST_MTIME] - 500
        os.utime(self.testfile, (s[stat.ST_ATIME], newtime))
        Checkable.ex_nihilo(filename=self.testfile)

        p = os.stat(self.testfile)
        self.assertEqual(p[stat.ST_MTIME], newtime,
                         "The mtime on the db file should be %d but is %d" %
                         (newtime, p[stat.ST_MTIME]))
        self.assertEqual(p[stat.ST_SIZE], 0,
                         "The size of the db file should be 0 but is %d" %
                         (p[stat.ST_SIZE]))

    # -------------------------------------------------------------------------
    def test_get_list_nosuch(self):
        """
        Calling .get_list() before .ex_nihilo() should cause an exception
        """
        if os.path.exists(self.testfile):
            os.unlink(self.testfile)
        try:
            got_exception = False
            Checkable.get_list(filename=self.testfile)
        except StandardError, e:
            got_exception = True
            self.assertEqual(str(e), "Please call .ex_nihilo() first",
                             "Expected 'Please call .ex_nihilo() first'," +
                             " got '%s'" % str(e))
        except Exception, e:
            raise StandardError("Not the expected exception: " +
                                "\"\"\"\n%s\"\"\"" % tb.format_exc())
        self.assertEqual(got_exception, True,
                         "Expected an exception but didn't get one.")
    
    # -------------------------------------------------------------------------
    def test_get_list_known(self):
        """
        Calling .get_list() should give us back a list of Checkable objects
        representing what is in the table
        """
        if os.path.exists(self.testfile):
            os.unlink(self.testfile)
        testdata = [('/', 'd', 0),
                    ('/abc', 'd', 17),
                    ('/xyz', 'f', 92),
                    ('/abc/foo', 'f', 0),
                    ('/abc/bar', 'f', time.time())]
                    
        Checkable.ex_nihilo(filename=self.testfile)
        db = sql.connect(self.testfile)
        cx = db.cursor()
        cx.executemany("insert into checkables(path, type, last_check) values(?, ?, ?)",
                       testdata[1:])
            
        db.commit()
        x = Checkable.get_list(self.testfile)

        self.assertEqual(len(x), 5,
                         "List should contain 5 entries, has %d" % len(x))
        for idx, item in enumerate(x):
            self.assertEqual(item.path, testdata[idx][0])
            self.assertEqual(item.type, testdata[idx][1])
            self.assertEqual(item.last_check, testdata[idx][2])
    
    # -------------------------------------------------------------------------
    def test_persist_dir_d(self):
        """
        Send in a new directory with path matching a duplicate in database
        (rowid == None, last_check == 0, type == 'd'). Exception should be
        thrown.
        """
        if os.path.exists(self.testfile):
            os.unlink(self.testfile)
        Checkable.ex_nihilo(filename=self.testfile)
        self.db_duplicates()
        x = Checkable.get_list(filename=self.testfile)
        self.assertEqual(len(x), 3,
                         "Expected 3 result, got %d" % len(x))

        foo = Checkable(path='/abc/def', type='d')
        try:
            foo.persist()
            got_exception = False
        except StandardError, e:
            self.assertEqual('There seems to be more than one' in str(e),
                             True,
                             "Got a StandardError but the message is wrong")
            got_exception = True
        except:
            self.fail("Got unexpected exception: \"\"\"\n%s\n\"\"\"" %
                      tb.format_exc())

        x = Checkable.get_list(filename=self.testfile)
        self.assertEqual(len(x), 3,
                         "Expected 3 results, got %d" % len(x))
        self.assertEqual(foo in x, True,
                         "Object foo not found in database")
        root = Checkable(path='/', type='d')
        self.assertEqual(root in x, True,
                         "Object root not found in database")
    
    # -------------------------------------------------------------------------
    def test_persist_dir_n(self):
        """
        Send in a new directory (rowid == None, last_check == 0, type == 'd',
        path does not match). New record should be added.
        """
        raise testhelp.UnderConstructionError('under construction')
    
    # -------------------------------------------------------------------------
    def test_persist_dir_p(self):
        """
        Send in a new directory with matching path (rowid == None, last_check
        == 0, type == 'd'). Existing path should be updated.
        """
        raise testhelp.UnderConstructionError('under construction')
    
    # -------------------------------------------------------------------------
    def test_persist_dir_v(self):
        """
        Send in an invalid directory (rowid != None, last_check == 0, type ==
        'd') Exception should be thrown.
        """
        raise testhelp.UnderConstructionError('under construction')
    
    # -------------------------------------------------------------------------
    def test_persist_dir_x(self):
        """
        Send in an existing directory with a new last_check time (rowid !=
        None, path exists, type == 'd', last_check changed). Last check time
        should be updated.
        """
        raise testhelp.UnderConstructionError('under construction')
    
    # -------------------------------------------------------------------------
    def test_persist_file_d(self):
        """
        Send in a new file with path matching a duplicate in database (rowid ==
        None, last_check == 0, type == 'f'). Exception should be thrown.
        """
        raise testhelp.UnderConstructionError('under construction')
    
    # -------------------------------------------------------------------------
    def test_persist_file_n(self):
        """
        Send in a new file (rowid == None, last_check == 0, path does not
        match, type == 'f'). New record should be added.
        """
        raise testhelp.UnderConstructionError('under construction')
    
    # -------------------------------------------------------------------------
    def test_persist_file_p(self):
        """
        Send in a new file with matching path (rowid == None, last_check
        == 0, type == 'f'). Existing path should be updated.
        """
        raise testhelp.UnderConstructionError('under construction')
    
    # -------------------------------------------------------------------------
    def test_persist_file_v(self):
        """
        Send in an invalid file (rowid == None, last_check != 0, type == 'f')
        Exception should be thrown.
        """
        if os.path.exists(self.testfile):
            os.unlink(self.testfile)
        Checkable.ex_nihilo(filename=self.testfile)
        self.db_one_file()
        x = Checkable.get_list(filename=self.testfile)
        self.expected(2, len(x))
        self.expected("/home/tpb/TODO", x[1].path)
        self.expected(0, x[1].last_check)

        now = time.time()
        x[1].last_check = now
        try:
            x[1].rowid = None
            x[1].persist()
        except StandardError, e:
            self.assertEqual("Invalid conditions:" in str(e), True,
                             "Got the wrong StandardError: "
                             + '\n"""\n%s\n"""' % tb.format_exc())
        except:
            self.fail("Got unexpected exception: "
                      + '"""\n%s\n"""' % tb.format_exc())

        x = Checkable.get_list(filename=self.testfile)
        self.expected(2, len(x))
        self.expected("/home/tpb/TODO", x[1].path)
        self.expected(0, x[1].last_check)

    # -------------------------------------------------------------------------
    def test_persist_file_x(self):
        """
        Send in an existing file with a new last_check time (rowid != None,
        path exists, type == 'f', last_check changed). Last check time should
        be updated.
        """
        if os.path.exists(self.testfile):
            os.unlink(self.testfile)
        Checkable.ex_nihilo(filename=self.testfile)
        self.db_one_file()
        x = Checkable.get_list(filename=self.testfile)
        self.expected(2, len(x))
        self.expected("/home/tpb/TODO", x[1].path)
        self.expected(0, x[1].last_check)

        now = time.time()
        x[1].last_check = now
        try:
            x[1].persist()
        except:
            self.fail("Got unexpected exception: \"\"\"\n%s\n\"\"\"" %
                      tb.format_exc())

        x = Checkable.get_list(filename=self.testfile)
        self.expected(2, len(x))
        self.expected(self.ymdhms(now), self.ymdhms(x[1].last_check))

    # -------------------------------------------------------------------------
    def db_duplicates(self):
        try:
            db = sql.connect(self.testfile)
            cx = db.cursor()
            cx.execute("""insert into checkables(path, type, last_check)
                                      values('/abc/def', 'd', 0)""")
            cx.execute("""insert into checkables(path, type, last_check)
                                      values('/abc/def', 'd', 0)""")
            db.commit()
            db.close()
        except sql.Error, e:
            print("SQLite Error: %s" % str(e))
            
    # -------------------------------------------------------------------------
    def db_one_file(self):
        try:
            db = sql.connect(self.testfile)
            cx = db.cursor()
            cx.execute("""insert into checkables(path, type, last_check)
                                      values('/home/tpb/TODO', 'f', 0)""")
            db.commit()
            db.close()
        except sql.Error, e:
            print("SQLite Error: %s" % str(e))
            
    # -------------------------------------------------------------------------
    def expected(self, expval, actual):
        msg = "Expected "
        if type(expval) == int:
            msg += "%d"
        elif type(expval) == float:
            msg += "%g"
        else:
            msg += "'%s'"

        msg += ", got "
        if type(actual) == int:
            msg += "%d"
        elif type(actual) == float:
            msg += "%g"
        else:
            msg += "'%s'"
        
        self.assertEqual(expval, actual, msg % (expval, actual))

    # -------------------------------------------------------------------------
    def ymdhms(self, dt):
        return time.strftime("%Y.%m%d.%H%M%S", time.localtime(dt))
    
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    if '-d' in sys.argv:
        sys.argv.remove('-d')
        pdb.set_trace()
    testhelp.main(sys.argv)
    
