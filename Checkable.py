#!/usr/bin/env python

import os
import pdb
import pexpect
import re
import sqlite3 as sql
import stat
import sys
import testhelp
import time
import toolframe
import traceback as tb
import unittest

# -----------------------------------------------------------------------------
class Checkable(object):
    dbname = ''
    # -------------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        self.dbname = Checkable.dbname
        self.path = '---'
        self.type = '-'
        self.checksum = ''
        self.last_check = 0
        self.rowid = None
        self.args = args
        for k in kwargs:
            if k not in ['rowid', 'path', 'type', 'checksum', 'last_check']:
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
    def ex_nihilo(cls, filename='drill.db', dataroot='/'):
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
                                                      checksum text,
                                                      last_check int)""")
                cx.execute("""insert into checkables(path, type, checksum,
                                                     last_check)
                                          values(?, ?, ?, ?)""",
                           (dataroot, 'd', '', 0))
                db.commit()
                db.close()
        except sql.Error, e:
            print("SQLite Error: %s" % str(e))

    # -----------------------------------------------------------------------------
    def fdparse(self, value):
        rgx = ('(.)([r-][w-][x-]){3}(\s+\S+){3}(\s+\d+)(\s+\w{3}' +
               '\s+\d+\s+[\d:]+)\s+(\S+)')
        q = re.search(rgx, value)
        if q:
            (type, ign1, ign2, ign3, ign4, fname) = q.groups()
            if type == '-': type = 'f'
            return(type, fname)
        else:
            return None

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
            cx.execute('select rowid, path, type, checksum, last_check' +
                       ' from checkables order by rowid')
            rows = cx.fetchall()
            for row in rows:
                new = Checkable(rowid=row[0], path=row[1], type=row[2],
                                checksum=row[3], last_check=row[4])
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
         - return the list
        For a file:
         - if it's readable, decide whether to make it an official check
         - if so, do all the check steps for the file
         - update the time of last check
         - persist the object to the database
        """
        rval = []
        hsi_prompt = "]:"
        S = pexpect.spawn('hsi')
        # S.logfile = sys.stdout
        S.expect(hsi_prompt)

        S.sendline("cd %s" % self.path)
        S.expect(hsi_prompt)
        if 'Not a directory' in S.before:
            # run the checks on the file
            pass
        elif 'Access denied' in S.before:
            # it's a directory but I don't have access to it
            pass
        else:
            # it's a directory -- get the list
            S.sendline("ls -l")
            S.expect(hsi_prompt)

            lines = S.before.split('\n')
            for line in lines:
                r = self.fdparse(line)
                if None != r:
                    fpath = '/'.join([self.path, r[1]])
                    new = Checkable(path=fpath, type=r[0])
                    new.persist()
                    rval.append(new)

        S.sendline("quit")
        S.expect(pexpect.EOF)
        S.close()

        self.last_check = time.time()
        self.persist()
        return rval

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
                    cx.execute("""insert into checkables(path, type, checksum,
                                                         last_check)
                                  values(?, ?, ?, ?)""",
                               (self.path, self.type, self.checksum,
                                self.last_check))
                elif 1 == len(rows):
                    # path is in db -- we update it
                    cx.execute("""update checkables set type=?, checksum=?,
                                                        last_check=?
                                  where path=?""",
                               (self.type, self.checksum, self.last_check,
                                self.path))
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
def tearDownModule():
    if os.path.exists(CheckableTest.testfile):
        os.unlink(CheckableTest.testfile)

# -----------------------------------------------------------------------------
class CheckableTest(testhelp.HelpedTestCase):
    testfile = 'test.db'
    methods = ['__init__', 'ex_nihilo', 'get_list', 'check', 'persist']
    testpath = '/home/tpb/TODO'
    
    # -------------------------------------------------------------------------
    def test_check_dir(self):
        """
        Calling .check() on a directory should give us back a list of Checkable
        objects representing the entries in the directory
        """
        if os.path.exists(self.testfile):
            os.unlink(self.testfile)
        Checkable.ex_nihilo(filename=self.testfile)
        testdir='/home/tpb/hic_test'
        self.db_add_one(path=testdir, type='d')
        x = Checkable.get_list(filename=self.testfile)
        
        self.expected(2, len(x))
        dirlist = x[1].check()

        self.assertIn(Checkable(path=testdir + '/crawler.tar',
                                type='f'), dirlist)
        self.assertIn(Checkable(path=testdir + '/crawler.tar.idx',
                                type='f'), dirlist)
        self.assertIn(Checkable(path=testdir + '/subdir1',
                                type='d'), dirlist)
        self.assertIn(Checkable(path=testdir + '/subdir2',
                                type='d'), dirlist)
        # raise testhelp.UnderConstructionError('under construction')
    
    # -------------------------------------------------------------------------
    def test_check_file(self):
        """
        Calling .check() on a file should execute the check actions for that
        file.
        """
        if os.path.exists(self.testfile):
            os.unlink(self.testfile)
        Checkable.ex_nihilo(filename=self.testfile)
        testdir='/home/tpb/hic_test'
        self.db_add_one(path=testdir, type='d')
        self.db_add_one(path=testdir + '/crawler.tar', type='f')
        self.db_add_one(path=testdir + '/crawler.tar.idx', type='f')

        x = Checkable.get_list(filename=self.testfile)
        checked = []
        for item in [z for z in x if z.type == 'f']:
            self.expected(0, item.last_check)
            item.check()

        x = Checkable.get_list(filename=self.testfile)
        for item in [z for z in x if z.type == 'f']:
            self.assertNotEqual(0, item.last_check,
                                "Expected last_check to be updated but " +
                                "it was not")

    # -------------------------------------------------------------------------
    def test_ctor(self):
        """
        Verify that the constructor gives us an object with the right methods
        """
        x = Checkable()
        for method in self.methods:
            self.assertEqual(method in dir(x), True,
                             "Checkable object is missing %s method" % method)
        self.expected('---', x.path)
        self.expected(None, x.rowid)

    # -------------------------------------------------------------------------
    def test_ctor_args(self):
        """
        Verify that the constructor accepts and sets path, type, and last_check
        """
        x = Checkable(path='/one/two/three', type='f', last_check=72)
        for method in self.methods:
            self.assertEqual(method in dir(x), True,
                         "Checkable object is missing %s method" % method)
        self.expected('/one/two/three', x.path)
        self.expected('f', x.type)
        self.expected(72, x.last_check)
            
    # -------------------------------------------------------------------------
    def test_ctor_bad_args(self):
        """
        Verify that the constructor accepts and sets path, type, and last_check
        """
        try:
            x = Checkable(path_x='/one/two/three', type='f', last_check=72)
            self.fail("Expected an exception but didn't get one.")
        except StandardError, e:
            self.assertEqual('Attribute path_x is invalid for Checkable'
                             in str(e), True,
                             "Got the wrong StandardError: "
                             + '\n"""\n%s\n"""' % tb.format_exc())
        except Exception, e:
            self.fail("Expected a StandardError but got this instead:"
                      + '\n"""\n%s\n"""' % tb.format_exc())
            
    # -------------------------------------------------------------------------
    def test_ex_nihilo_drspec(self):
        """
        If the database file does not already exist, calling ex_nihilo() should
        create it. For this test, we specify and verify a dataroot value.
        """
        # make sure the .db file does not exist
        if os.path.exists(self.testfile):
            os.unlink(self.testfile)

        # this call should create it
        Checkable.ex_nihilo(filename=self.testfile, dataroot="/home/somebody")

        # check that it exists
        self.assertEqual(os.path.exists(self.testfile), True,
                         "File '%s' should be created by ex_nihilo()" %
                         (self.testfile))

        # assuming it does, look inside and make sure the checkables table got
        # initialized correctly
        db = sql.connect(self.testfile)
        cx = db.cursor()

        # there should be one row
        cx.execute('select * from checkables')
        rows = cx.fetchall()
        self.expected(1, len(rows))

        # the one row should reference the root directory
        cx.execute('select max(rowid) from checkables')
        max_id = cx.fetchone()[0]
        self.expected(1, max_id)
        self.expected('/home/somebody', rows[0][1])
        self.expected('d', rows[0][2])
        self.expected('', rows[0][3])
        self.expected(0, rows[0][4])
        
    # -------------------------------------------------------------------------
    def test_ex_nihilo_exist(self):
        """
        If the database file does already exist, calling ex_nihilo() should do
        nothing.
        """
        # make sure the .db file does not exist
        if os.path.exists(self.testfile):
            os.unlink(self.testfile)

        # create a dummy .db file and set its mtime back by 500 seconds
        testhelp.touch(self.testfile)
        s = os.stat(self.testfile)
        newtime = s[stat.ST_MTIME] - 500
        os.utime(self.testfile, (s[stat.ST_ATIME], newtime))

        # call the test target routine
        Checkable.ex_nihilo(filename=self.testfile)

        # verify that the file's mtime is unchanged and its size is 0
        p = os.stat(self.testfile)
        self.expected(self.ymdhms(newtime), self.ymdhms(p[stat.ST_MTIME]))
        self.expected(0, p[stat.ST_SIZE])
        
    # -------------------------------------------------------------------------
    def test_ex_nihilo_scratch(self):
        """
        If the database file does not already exist, calling ex_nihilo() should
        create it.
        """
        # make sure the .db file does not exist
        if os.path.exists(self.testfile):
            os.unlink(self.testfile)

        # this call should create it
        Checkable.ex_nihilo(filename=self.testfile)

        # check that it exists
        self.assertEqual(os.path.exists(self.testfile), True,
                         "File '%s' should be created by ex_nihilo()" %
                         (self.testfile))

        # assuming it does, look inside and make sure the checkables table got
        # initialized correctly
        db = sql.connect(self.testfile)
        cx = db.cursor()

        # there should be one row
        cx.execute('select * from checkables')
        rows = cx.fetchall()
        self.expected(1, len(rows))

        # the one row should reference the root directory
        cx.execute('select max(rowid) from checkables')
        max_id = cx.fetchone()[0]
        self.expected(1, max_id)
        self.expected('/', rows[0][1])
        self.expected('d', rows[0][2])
        self.expected('', rows[0][3])
        self.expected(0, rows[0][4])
        
    # -------------------------------------------------------------------------
    def test_fdparse_ldr(self):
        """
        Parse an ls -l line from hsi where we're looking at a directory with a
        recent date (no year). fdparse() should return type='d', path=<file
        path>.
        """
        n = Checkable(path='xyx', type='d')
        line = ('drwx------    2 tpb       ccsstaff         ' +
                '512 Oct 17 13:54 subdir1')
        (t,f) = n.fdparse(line)
        self.expected('d', t)
        self.expected('subdir1', f)
    
    # -------------------------------------------------------------------------
    def test_fdparse_ldy(self):
        """
        Parse an ls -l line from hsi where we're looking at a directory with a year
        in the date. fdparse() should return type='d', path=<file path>.
        """
        n = Checkable(path='xyx', type='d')
        line = ('drwxr-xr-x    2 tpb       ccsstaff         ' +
                '512 Dec 17  2004 incase')
        (t,f) = n.fdparse(line)
        self.expected('d', t)
        self.expected('incase', f)
    
    # -------------------------------------------------------------------------
    def test_fdparse_lfr(self):
        """
        Parse an ls -l line from hsi where we're looking at a file with a
        recent date (no year). fdparse() should return type='f', path=<file
        path>.
        """
        n = Checkable(path='xyx', type='d')
        line = ('-rw-------    1 tpb       ccsstaff     ' +
                '1720832 Oct 17 13:56 crawler.tar')
        (t,f) = n.fdparse(line)
        self.expected('f', t)
        self.expected('crawler.tar', f)
    
    # -------------------------------------------------------------------------
    def test_fdparse_lfy(self):
        """
        Parse an ls -l line from hsi where we're looking at a file with a year
        in the date. fdparse() should return type='f', path=<file path>.
        """
        n = Checkable(path='xyx', type='d')
        line = ('-rw-------    1 tpb       ccsstaff        4896' +
                ' Dec 30  2011 pytest.tar.idx')
        (t,f) = n.fdparse(line)
        self.expected('f', t)
        self.expected('pytest.tar.idx', f)
    
    # -------------------------------------------------------------------------
    def test_fdparse_nomatch(self):
        """
        Parse an ls -l line from hsi that does not describe a file or directory
        in the date. fdparse() should return None.
        """
        n = Checkable(path='xyx', type='d')
        line = '/home/tpb/cli_test:'
        z = n.fdparse(line)
        self.expected(None, z)
    
    # -------------------------------------------------------------------------
    def test_get_list_nosuch(self):
        """
        Calling .get_list() before .ex_nihilo() should cause an exception
        """
        if os.path.exists(self.testfile):
            os.unlink(self.testfile)
        try:
            Checkable.get_list(filename=self.testfile)
            self.fail("Expected an exception but didn't get one.")
        except StandardError, e:
            self.assertEqual("Please call .ex_nihilo() first" in str(e), True,
                             "Got the wrong StandardError: "
                             + '\n"""\n%s\n"""' % tb.format_exc())
        except Exception, e:
            self.fail("Expected a StandardError but got this instead:"
                      + '\n"""\n%s\n"""' % tb.format_exc())
    
    # -------------------------------------------------------------------------
    def test_get_list_known(self):
        """
        Calling .get_list() should give us back a list of Checkable objects
        representing what is in the table
        """
        if os.path.exists(self.testfile):
            os.unlink(self.testfile)
        testdata = [('/', 'd', '', 0),
                    ('/abc', 'd', '', 17),
                    ('/xyz', 'f', '', 92),
                    ('/abc/foo', 'f', '', 0),
                    ('/abc/bar', 'f', '', time.time())]
                    
        Checkable.ex_nihilo(filename=self.testfile)
        db = sql.connect(self.testfile)
        cx = db.cursor()
        cx.executemany("insert into checkables(path, type, checksum, last_check)" +
                       " values(?, ?, ?, ?)",
                       testdata[1:])
            
        db.commit()
        x = Checkable.get_list(self.testfile)

        self.expected(5, len(x))

        for idx, item in enumerate(x):
            self.expected(testdata[idx][0], item.path)
            self.expected(testdata[idx][1], item.type)
            self.expected(testdata[idx][2], item.checksum)
            self.expected(testdata[idx][3], item.last_check)
    
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
        self.expected(3, len(x))

        foo = Checkable(path='/abc/def', type='d')
        try:
            foo.persist()
            self.fail("Expected an exception but didn't get one.")
        except StandardError, e:
            self.assertEqual('There seems to be more than one' in str(e), True,
                             "Got the wrong StandardError: "
                             + '\n"""\n%s\n"""' % tb.format_exc())
        except:
            self.fail("Expected a StandardError but got this instead:"
                      + '\n"""\n%s\n"""' % tb.format_exc())

        x = Checkable.get_list(filename=self.testfile)
        self.expected(3, len(x))
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
        if os.path.exists(self.testfile):
            os.unlink(self.testfile)
        Checkable.ex_nihilo(filename=self.testfile)
        x = Checkable.get_list(filename=self.testfile)
        self.expected(1, len(x))

        foo = Checkable(path='/abc/def', type='d')
        try:
            foo.persist()
        except:
            self.fail("Got unexpected exception:"
                      + '\n"""\n%s\n"""' % tb.format_exc())

        x = Checkable.get_list(filename=self.testfile)
        self.expected(2, len(x))
        self.assertEqual(foo in x, True,
                         "Object foo not found in database")
        root = Checkable(path='/', type='d')
        self.assertEqual(root in x, True,
                         "Object root not found in database")
    
    # -------------------------------------------------------------------------
    def test_persist_dir_p(self):
        """
        Send in a new directory with matching path (rowid == None, last_check
        == 0, type == 'd'). Existing path should be updated.
        """
        if os.path.exists(self.testfile):
            os.unlink(self.testfile)
        Checkable.ex_nihilo(filename=self.testfile)

        now = time.time()
        self.db_add_one(path=self.testpath, type='d', last_check=now)
        
        x = Checkable.get_list(filename=self.testfile)
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected(now, x[1].last_check)

        x[1].last_check = 0
        try:
            x[1].rowid = None
            x[1].persist()
        except:
            self.fail("Got unexpected exception: "
                      + '"""\n%s\n"""' % tb.format_exc())

        x = Checkable.get_list(filename=self.testfile)
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected('d', x[1].type)
        self.expected(0, x[1].last_check)
    
        # raise testhelp.UnderConstructionError('under construction')
    
    # -------------------------------------------------------------------------
    def test_persist_dir_v(self):
        """
        Send in an invalid directory (rowid != None, last_check == 0, type ==
        'd'). Exception should be thrown.
        """
        if os.path.exists(self.testfile):
            os.unlink(self.testfile)
        Checkable.ex_nihilo(filename=self.testfile)
        now = time.time()
        self.db_add_one(path='/home', type='d', last_check=now)

        x = Checkable.get_list(filename=self.testfile)

        self.expected(2, len(x))
        self.assertIn(Checkable(path='/', type='d'), x)
        self.assertIn(Checkable(path='/home', type='d'), x)
                      
        x[0].last_check = now = time.time()
        x[0].persist()
        
        x[0].last_check = 0
        try:
            x[0].persist()
            self.fail("Expected an exception but didn't get one.")
        except StandardError, e:
            self.assertEqual("Invalid conditions:" in str(e), True,
                             "Got the wrong StandardError: "
                             + '\n"""\n%s\n"""' % tb.format_exc())
        except:
            self.fail("Got unexpected exception: "
                      + '"""\n%s\n"""' % tb.format_exc())

        x = Checkable.get_list(filename=self.testfile)
        self.expected(2, len(x))
        self.assertIn(Checkable(path='/', type='d'), x)
        self.assertIn(Checkable(path='/home', type='d'), x)
        self.expected(self.ymdhms(now), self.ymdhms(x[1].last_check))
        
    # -------------------------------------------------------------------------
    def test_persist_dir_x(self):
        """
        Send in an existing directory with a new last_check time (rowid !=
        None, path exists, type == 'd', last_check changed). Last check time
        should be updated.
        """
        if os.path.exists(self.testfile):
            os.unlink(self.testfile)
        Checkable.ex_nihilo(filename=self.testfile)

        x = Checkable.get_list(filename=self.testfile)
        self.expected(1, len(x))
        self.expected(0, x[0].last_check)

        x[0].last_check = now = time.time()
        try:
            x[0].persist()
        except:
            self.fail("Got an unexpected exception:" 
                      + '\n"""\n%s\n"""' % tb.format_exc())

        x = Checkable.get_list(filename=self.testfile)
        self.expected(1, len(x))
        self.expected('/', x[0].path)
        self.expected('d', x[0].type)
        self.expected(now, x[0].last_check)
            
    # -------------------------------------------------------------------------
    def test_persist_file_d(self):
        """
        Send in a new file with path matching a duplicate in database (rowid ==
        None, last_check == 0, type == 'f'). Exception should be thrown.
        """
        if os.path.exists(self.testfile):
            os.unlink(self.testfile)
        Checkable.ex_nihilo(filename=self.testfile)
        self.db_add_one(path=self.testpath, type='f')
        self.db_add_one(path=self.testpath, type='f')

        x = Checkable.get_list(filename=self.testfile)
        self.expected(3, len(x))
        self.assertEqual(x[1], x[2],
                         "There should be a duplicate entry in the database.")
        
        foo = Checkable(path=self.testpath, type='f')
        try:
            foo.persist()
            self.fail("Expected an exception but didn't get one.")
        except StandardError, e:
            self.assertEqual('There seems to be more than one' in str(e), True,
                             "Got the wrong StandardError: "
                             + '\n"""\n%s\n"""' % tb.format_exc())
        except:
            self.fail("Expected a StandardError but got this instead:"
                      + '\n"""\n%s\n"""' % tb.format_exc())

        x = Checkable.get_list(filename=self.testfile)
        self.expected(3, len(x))
        self.assertEqual(foo in x, True,
                         "Object foo not found in database")
        root = Checkable(path='/', type='d')
        self.assertEqual(root in x, True,
                         "Object root not found in database")
        self.assertEqual(x[1], x[2],
                         "There should be a duplicate entry in the database.")
        
    # -------------------------------------------------------------------------
    def test_persist_file_n(self):
        """
        Send in a new file (rowid == None, last_check == 0, path does not
        match, type == 'f'). New record should be added.
        """
        if os.path.exists(self.testfile):
            os.unlink(self.testfile)
        Checkable.ex_nihilo(filename=self.testfile)

        x = Checkable.get_list(filename=self.testfile)
        self.expected(1, len(x))
        self.expected("/", x[0].path)
        self.expected("", x[0].checksum)
        self.expected(0, x[0].last_check)

        foo = Checkable(path=self.testpath, type='f')
        try:
            foo.persist()
        except:
            self.fail("Got unexpected exception: "
                      + '"""\n%s\n"""' % tb.format_exc())

        x = Checkable.get_list(filename=self.testfile)
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected(0, x[1].last_check)
    
    # -------------------------------------------------------------------------
    def test_persist_file_p(self):
        """
        Send in a new file with matching path (rowid == None, last_check
        == 0, type == 'f'). Existing path should be updated.
        """
        if os.path.exists(self.testfile):
            os.unlink(self.testfile)
        Checkable.ex_nihilo(filename=self.testfile)
        now = time.time()
        self.db_add_one(last_check=now)
        x = Checkable.get_list(filename=self.testfile)
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected(now, x[1].last_check)

        x[1].last_check = 0
        try:
            x[1].rowid = None
            x[1].persist()
        except:
            self.fail("Got unexpected exception: "
                      + '"""\n%s\n"""' % tb.format_exc())

        x = Checkable.get_list(filename=self.testfile)
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected(0, x[1].last_check)
    
    # -------------------------------------------------------------------------
    def test_persist_file_v(self):
        """
        Send in an invalid file (rowid == None, last_check != 0, type == 'f')
        Exception should be thrown.
        """
        if os.path.exists(self.testfile):
            os.unlink(self.testfile)
        Checkable.ex_nihilo(filename=self.testfile)
        self.db_add_one()
        x = Checkable.get_list(filename=self.testfile)
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
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
        self.expected(self.testpath, x[1].path)
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
        self.db_add_one()
        x = Checkable.get_list(filename=self.testfile)
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
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
            cx.execute("""insert into checkables(path, type, checksum,
                                                 last_check)
                                      values('/abc/def', 'd', '', 0)""")
            cx.execute("""insert into checkables(path, type, checksum,
                                                 last_check)
                                      values('/abc/def', 'd', '', 0)""")
            db.commit()
            db.close()
        except sql.Error, e:
            print("SQLite Error: %s" % str(e))
            
    # -------------------------------------------------------------------------
    def db_add_one(self, path=testpath, type='f', checksum='', last_check=0):
        try:
            db = sql.connect(self.testfile)
            cx = db.cursor()
            cx.execute("""insert into checkables(path, type, checksum,
                                                 last_check)
                                      values(?, ?, ?, ?)""",
                       (path, type, checksum, last_check))
            db.commit()
            db.close()
        except sql.Error, e:
            print("SQLite Error: %s" % str(e))

    # -------------------------------------------------------------------------
    def ymdhms(self, dt):
        return time.strftime("%Y.%m%d.%H%M%S", time.localtime(dt))
    
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    toolframe.ez_launch(test='CheckableTest',
                        logfile='crawl_test.log')
    
