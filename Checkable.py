#!/usr/bin/env python
"""
An item in HPSS that can be checked for integrity

As we scan the population of files in HPSS, we select a representative sample.
We expect this sample to gradually grow. We will never delete entries from the
sample, so eventually it is expected that it will match the population. In the
meantime, we want to get useful information about the reliability of HPSS by
examining the files in the sample and verifying that each file's data matches
its checksum.

The following information is tracked for each Checkable:

    path
    type (file or directory)
    cos (HPSS class of service)
    last_check (last time the item was checked)
    checksum (md5 hash representing the data most recently read from the file)

Note that directories are only used to find more files. A directory does not
have a cos or a checksum.
"""
import CrawlDBI
import hashlib
import os
import pdb
import pexpect
import random
import re
import stat
import sys
import testhelp
import time
import toolframe
import traceback as tb
import unittest
import util

# -----------------------------------------------------------------------------
class Checkable(object):
    """
    This class represents an HPSS entity that can be checked. That is, it has
    attributes that can be validated (cos, checksum) or it contains other
    things that can be checked (eg., a directory).

    Note that only files have cos and checksum. Directories only have path,
    type, and last_check.
    """
    # !@! seems like dbname should be the default database name (e.g.,
    # 'drill.db')
    dbname = ''
    # -------------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        """
        Initialize a Checkable object -- set the path, type, checksum, cos, and
        last_check to default values, then update them based on the arguments.
        """
        self.dbname = Checkable.dbname
        self.path = '---'
        self.type = '-'
        self.checksum = ''
        self.cos = ''
        self.last_check = 0
        self.rowid = None
        self.args = args
        for k in kwargs:
            if k not in ['rowid',
                         'path',
                         'type',
                         'checksum',
                         'cos',
                         'last_check']:
                raise StandardError("Attribute %s is invalid for Checkable" %
                                    k)
            setattr(self, k, kwargs[k])
        super(Checkable, self).__init__()
        
    # -------------------------------------------------------------------------
    def __repr__(self):
        """
        Return a human-readable representation of a Checkable object.
        """
        return("Checkable(rowid=%s, " % str(self.rowid) +
               "path='%s', " % self.path +
               "type='%s', " % self.type +
               "cos='%s', " % self.cos +
               "checksum='%s', " % self.checksum +
               "last_check=%f)" % self.last_check)

    # -------------------------------------------------------------------------
    def __eq__(self, other):
        """
        Two Checkable objects are equal if they are both instances of Checkable
        and their path and type members are equal.
        """
        return (isinstance(other, self.__class__) and
                (self.path == other.path) and
                (self.type == other.type))

    # -------------------------------------------------------------------------
    @classmethod
    def ex_nihilo(cls, filename='drill.db', dataroot='/'):
        """
        Start from scratch. Create the database if necessary. Create the
        table(s) if necessary. Bootstrap the queue by adding the root
        director(ies).

        !@! Currently, all Checkables go in table 'checkables'. We want to
         change this so that directories go in table 'queue' and files go in
         table 'sample'. These two tables must be created here. The sample
         table needs all the attributes (path, cos, checksum, last_check) while
         the queue table only needs path and last_check. The type field need
         not be stored in the database because it will be distinguished by
         which table the item is in.
        """
        Checkable.dbname = filename
        if not os.path.exists(filename):
            db = CrawlDBI.DBI(dbname=filename)
            db.create(table='checkables',
                      fields=['id int primary key',
                              'path text',
                              'type text',
                              'checksum text',
                              'cos text',
                              'last_check int'])
            db.insert(table='checkables',
                      fields=['path', 'type', 'checksum', 'cos', 'last_check'],
                      data=[(dataroot, 'd', '', '', 0)])
            db.close()

    # -----------------------------------------------------------------------------
    @classmethod
    def fdparse(cls, value):
        """
        Parse a file or directory name and type ('f' or 'd') from hsi output.
        Return the two values in a tuple if successful, or None if not.

        In "ls -P" output, directory lines look like

            DIRECTORY       /home/tpb/bearcat

        File lines look like (cos is '5081')

            FILE    /home/tpb/halloy_test   111670  111670  3962+300150
                X0352700        5081    0       1  01/29/2004       15:25:02
                03/19/2012      13:09:50
        """
        try:
            q = cls.rgxP
        except AttributeError:
            cls.rgxP = re.compile("(FILE|DIRECTORY)\s+(\S+)(\s+\d+\s+\d+" +
                                  "\s+\S+\s+\S+\s+(\d+))?")
            cls.rgxl = re.compile("(.)([r-][w-][x-]){3}(\s+\S+){3}" +
                                  "(\s+\d+)(\s+\w{3}\s+\d+\s+[\d:]+)" +
                                  "\s+(\S+)")
            cls.map = {'DIRECTORY': 'd',
                       'd': 'd',
                       'FILE': 'f',
                       '-': 'f'}
            
        ltup = re.findall(cls.rgxP, value)
        if ltup:
            (type, fname, ign1, cos) = ltup[0]
            return(cls.map[type], fname, cos)

        ltup = re.findall(cls.rgxl, value)
        if ltup:
            (type, ign1, ign2, ign3, ign4, fname) = ltup[0]
            return(cls.map[type], fname, '')

        return None

    # -------------------------------------------------------------------------
    @classmethod
    def get_list(cls, filename='drill.db'):
        """
        Return the current list of Checkables from the database.
        """
        Checkable.dbname = filename
        rval = []
        db = CrawlDBI.DBI(dbname=filename)
        rows = db.select(table='checkables',
                         fields=['rowid', 'path', 'type',
                                 'checksum', 'cos', 'last_check'],
                         orderby='last_check')
        for row in rows:
            new = Checkable(rowid=row[0],
                            path=row[1],
                            type=row[2],
                            checksum=row[3],
                            cos=row[4],
                            last_check=row[5])
            rval.append(new)
        db.close()
        return rval

    # -------------------------------------------------------------------------
    def check(self, odds):
        """
        For a directory:
         - get a list of its contents if possible,
         - create a Checkable object for each item and persist it to the
           database
         - return the list of Checkables found in the directory
        For a file:
         - if it's readable, decide whether to make it an official check
         - if no, no Checkable is created for this file
         - if yes,
            - if we have a checksum for the file:
                - retrieve the file and compute the checksum and verify it
            - else:
                - retrieve the file, compute the checksum and record it
            - record/verify the file's COS
            - update last_check with the current time
            - persist the object to the database

        The value of odds indicates the likelihood with which we should check
        files: 1 in odds

        potential outcomes
         1 read a directory, returning a list of the contents
         2 tried to read a directory but it was not accessible
         3 skipped a non-directory file
         4 fetched and checksummed a non-directory file
         5 fetched a checksummed file and matched its checksum
         6 fetched a checksummed file and the checksum match failed
        """
        # fire up hsi
        rval = []
        hsi_prompt = "]:"
        S = pexpect.spawn('hsi -q', timeout=300)
        # S.logfile = sys.stdout
        S.expect(hsi_prompt)

        # Attempt to cd to the path represented by the current object
        S.sendline("cd %s" % self.path)
        S.expect(hsi_prompt)
        if 'Not a directory' in S.before:
            # it's not a directory, so run the checks on the file
            if self.checksum != '':
                # we have a checksum so we're going to compare it
                # fetch the file to disk
                localname = "/tmp/" + os.path.basename(self.path)
                S.sendline("get %s : %s" % (localname, self.path))
                S.expect(hsi_prompt)

                # compute the hash
                m = hashlib.md5()
                m.update(util.contents(localname))

                # turn the hash into a hexadecimal string
                filesum =  ''.join(["%02x" % ord(i) for i in m.digest()])

                # compare the stored checksum against the newly computed one
                if self.checksum != filesum:
                    # outcome 6 -- checksums do not match
                    rval = Alert("Recorded checksum '%s' " % self.checksum +
                                 "does not match computed " +
                                 "checksum '%s' " + filesum +
                                 "for file %s" % self.path)
                else:
                    # outcome 5 -- checksums do match
                    rval = "matched"
            elif 0 < odds and random.randrange(int(odds)) <= 1:
                # outcome 4 -- we're adding it to the sample so we have to
                # fetch the file and compute a checksum on it
                localname = "/tmp/" + os.path.basename(self.path)
                S.sendline("get %s : %s" % (localname, self.path))
                S.expect(hsi_prompt)
                m = hashlib.md5()
                m.update(util.contents(localname))
                self.checksum = ''.join(["%02x" % ord(i) for i in m.digest()])
                os.unlink(localname)
                rval = self
            else:
                # outcome 3 -- we're skipping the file
                rval = 'skipped'
        elif 'Access denied' in S.before:
            # outcome 2 -- it's a directory but I don't have access to it
            rval = 'access denied'
        else:
            # outcome 1 -- it's a directory -- get the list. we fill in rval
            # with the list of the directory's contents.
            S.sendline("ls -P")
            S.expect(hsi_prompt)

            lines = S.before.split('\n')
            for line in lines:
                r = Checkable.fdparse(line)
                if None != r:
                    if '/' not in r[1]:
                        fpath = '/'.join([self.path, r[1]])
                    else:
                        fpath = r[1]
                    new = Checkable(path=fpath, type=r[0], cos=r[2])
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

        There are several situations where we need to push data into the
        database.

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

        2) We have just checked an existing object and want to update its
           last_check time. In this case, we'll have the following incoming values:

           rowid: != None
           path: ...
           type: ...
           last_check: != 0

        3) The cos of an existing file has changed. Incoming values:

           rowid: != None
           path: ...
           type: 'f'
           cos: != ''
           
        In any other case, we'll throw an exception.
        """
        try:
            db = CrawlDBI.DBI(dbname=self.dbname)
            if self.rowid != None and self.last_check != 0.0:
                # Updating the check time for an existing object 
                db.update(table='checkables',
                          fields=['last_check', 'checksum', 'cos'],
                          where='rowid=?',
                          data=[(self.last_check,
                                 self.checksum,
                                 self.cos,
                                 self.rowid)])
            elif self.rowid == None and self.last_check == 0.0:
                # Adding (or perhaps updating) a new/existing checkable
                rows = db.select(table='checkables',
                                 fields=[],
                                 where='path=?',
                                 data=(self.path,))
                if 0 == len(rows):
                    # path not in db -- we insert it
                    db.insert(table='checkables',
                              fields=['path',
                                      'type',
                                      'checksum',
                                      'cos',
                                      'last_check'],
                              data=[(self.path,
                                     self.type,
                                     self.checksum,
                                     self.cos,
                                     self.last_check)])
                elif 1 == len(rows):
                    # path is in db -- if type or cos has changed, reset
                    # last_check and checksum. Otherwise, leave it alone

                    if self.type != rows[0][2]:
                        db.update(table='checkables',
                                  fields=['type', 'last_check', 'checksum'],
                                  where='path=?',
                                  data=[(self.type, 0, '', self.path)])
                    if self.type == 'd':
                        db.update(table='checkables',
                                  fields=['cos'],
                                  where='path=?',
                                  data=[('', self.path)])
                    elif self.cos != rows[0][4]:
                        db.update(table='checkables',
                                  fields=['cos'],
                                  where='path=?',
                                  data=[(self.cos, self.path)])
                else:
                    raise StandardError("There seems to be more than one" +
                                        " occurrence of '%s' in the database" %
                                        self.path)
            else:
                raise StandardError("Invalid conditions: "
                                    + "rowid = %s; " % str(self.rowid)
                                    + "path = '%s'; " % self.path
                                    + "type = '%s'; " % self.type
                                    + "checksum = '%s'; " % self.checksum
                                    + "last_check = %f" % self.last_check)

            db.close()
        except CrawlDBI.DBIerror, e:
            print("Database Error: %s" % str(e))

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
    testdir = './test.d'
    testfile = '%s/test.db' % testdir
    methods = ['__init__', 'ex_nihilo', 'get_list', 'check', 'persist']
    testpath = '/home/tpb/TODO'
    
    # -------------------------------------------------------------------------
    def test_check_dir(self):
        """
        Calling .check() on a directory should give us back a list of Checkable
        objects representing the entries in the directory
        """
        util.conditional_rm(self.testfile)
        Checkable.ex_nihilo(filename=self.testfile)
        testdir='/home/tpb/hic_test'
        self.db_add_one(path=testdir, type='d')
        x = Checkable.get_list(filename=self.testfile)
        
        self.expected(2, len(x))
        dirlist = x[1].check(50)

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
    
    # -------------------------------------------------------------------------
    def test_check_file(self):
        """
        Calling .check() on a file should execute the check actions for that
        file.
        """
        util.conditional_rm(self.testfile)
        Checkable.ex_nihilo(filename=self.testfile)
        testdir='/home/tpb/hic_test'
        self.db_add_one(path=testdir, type='d')
        self.db_add_one(path=testdir + '/crawler.tar', type='f')
        self.db_add_one(path=testdir + '/crawler.tar.idx', type='f')

        x = Checkable.get_list(filename=self.testfile)
        checked = []
        for item in [z for z in x if z.type == 'f']:
            self.expected(0, item.last_check)
            item.check(-1)

        x = Checkable.get_list(filename=self.testfile)
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
        self.expected('', x.checksum)
        self.expected('', x.cos)
        self.expected(0, x.last_check)
        self.expected(None, x.rowid)

    # -------------------------------------------------------------------------
    def test_ctor_args(self):
        """
        Verify that the constructor accepts and sets rowid, path, type,
        checksum, cos, and last_check
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
        Verify that the constructor rejects invalid arguments
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
    def test_eq(self):
        """
        Two (Checkable) objects should be equal iff both are instances of class
        Checkable and their path and type attributes are equal.
        """
        now = time.time()
        a = Checkable(rowid=92,
                      path='/foo/bar',
                      type='f',
                      checksum='12345',
                      cos='9283',
                      last_check=now)
        b = Checkable(rowid=97,
                      path='/foo/bar',
                      type='f',
                      checksum='something else',
                      cos='23743',
                      last_check=now + 23)
        c = Checkable(rowid=43,
                      path='/foo/bar',
                      type='d',
                      checksum='7777',
                      cos='2843',
                      last_check=now - 32)
        d = Checkable(rowid=18,
                      path='/foo/fiddle',
                      type='f',
                      checksum='1234591',
                      cos='9222',
                      last_check=now + 10132)
        e = lambda: None
        setattr(e, 'path', '/foo/bar')
        setattr(e, 'type', 'f')
        f = Checkable(rowid=49,
                      path='/foo/fiddle',
                      type='d',
                      checksum='77sd7',
                      cos='739',
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
        util.conditional_rm(self.testfile)

        # this call should create it
        Checkable.ex_nihilo(filename=self.testfile, dataroot="/home/somebody")

        # check that it exists
        self.assertEqual(os.path.exists(self.testfile), True,
                         "File '%s' should be created by ex_nihilo()" %
                         (self.testfile))

        # assuming it does, look inside and make sure the checkables table got
        # initialized correctly
        db = CrawlDBI.DBI(dbname=self.testfile)

        # there should be one row
        rows = db.select(table='checkables', fields=[])
        self.expected(1, len(rows))

        # the one row should reference the root directory
        [(max_id,)] = db.select(table='checkables', fields=['max(rowid)'])
        self.expected(1, max_id)                       # id
        self.expected('/home/somebody', rows[0][1])    # path
        self.expected('d', rows[0][2])                 # type
        self.expected('', rows[0][3])                  # checksum
        self.expected('', rows[0][4])                  # cos
        self.expected(0, rows[0][5])                   # last_check

    # -------------------------------------------------------------------------
    def test_ex_nihilo_exist(self):
        """
        If the database file does already exist, calling ex_nihilo() should do
        nothing.

        The current behavior is that if an existing (non database) file is
        named as the database, no attempt will be made to create the tables.
        This is desirable. We don't want to create the tables unless we created
        the file. Otherwise, the user might overwrite a file inadvertently by
        trying to treat it as a database.

        !@! Update ex_nihilo to throw an exception if it detects that it has
         been asked to use a non-database file for the database.
        """
        # make sure the .db file does not exist
        util.conditional_rm(self.testfile)

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
        util.conditional_rm(self.testfile)

        # this call should create it
        Checkable.ex_nihilo(filename=self.testfile)

        # check that it exists
        self.assertEqual(os.path.exists(self.testfile), True,
                         "File '%s' should be created by ex_nihilo()" %
                         (self.testfile))

        # assuming it does, look inside and make sure the checkables table got
        # initialized correctly
        db = CrawlDBI.DBI(dbname=self.testfile)
        
        # there should be one row
        rows = db.select(table='checkables', fields=[])
        self.expected(1, len(rows))

        # the one row should reference the root directory
        [(max_id, )] = db.select(table='checkables', fields=['max(rowid)'])
        self.expected(1, max_id)           # id
        self.expected('/', rows[0][1])     # path
        self.expected('d', rows[0][2])     # type
        self.expected('', rows[0][3])      # checksum
        self.expected('', rows[0][4])      # cos
        self.expected(0, rows[0][5])       # last_check
        
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
        (t,f,c) = n.fdparse(line)
        self.expected('d', t)
        self.expected('subdir1', f)
        self.expected('', c)
    
    # -------------------------------------------------------------------------
    def test_fdparse_ldy(self):
        """
        Parse an ls -l line from hsi where we're looking at a directory with a year
        in the date. fdparse() should return type='d', path=<file name>.
        """
        n = Checkable(path='xyx', type='d')
        line = ('drwxr-xr-x    2 tpb       ccsstaff         ' +
                '512 Dec 17  2004 incase')
        (t,f,c) = n.fdparse(line)
        self.expected('d', t)
        self.expected('incase', f)
        self.expected('', c)
    
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
        (t,f,c) = n.fdparse(line)
        self.expected('f', t)
        self.expected('crawler.tar', f)
        self.expected('', c)
    
    # -------------------------------------------------------------------------
    def test_fdparse_lfy(self):
        """
        Parse an ls -X line from hsi where we're looking at a file with a year
        in the date. fdparse() should return type='f', path=<file name>.
        """
        n = Checkable(path='xyx', type='d')
        line = ('-rw-------    1 tpb       ccsstaff        4896' +
                ' Dec 30  2011 pytest.tar.idx')
        (t,f,c) = n.fdparse(line)
        self.expected('f', t)
        self.expected('pytest.tar.idx', f)
        self.expected('', c)

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
        n = Checkable(path='xyx', type='d')
        line = "DIRECTORY       /home/tpb/apache"
        (t,f,c) = n.fdparse(line)
        self.expected('d', t)
        self.expected('/home/tpb/apache', f)
        self.expected('', c)
    
    # -------------------------------------------------------------------------
    def test_fdparse_Pf(self):
        """
        Parse an ls -P line from hsi where we're looking at a file. fdparse()
        should return type='f', path=<file path>, cos.
        """
        n = Checkable(path='xyx', type='d')
        line = ("FILE    /home/tpb/LoadL_admin   88787   88787   " +
                "3962+411820     X0352700        5081    0       1       " +
                "03/14/2003      07:12:43        03/19/2012       13:09:50")
        (t,f,c) = n.fdparse(line)
        self.expected('f', t)
        self.expected('/home/tpb/LoadL_admin', f)
        self.expected('5081', c)
    
    # -------------------------------------------------------------------------
    def test_get_list_nosuch(self):
        """
        Calling .get_list() before .ex_nihilo() should cause an exception
        """
        util.conditional_rm(self.testfile)

        try:
            Checkable.get_list(filename=self.testfile)
            self.fail("Expected an exception but didn't get one.")
        except CrawlDBI.DBIerror, e:
            self.assertEqual("no such table: checkables" in str(e), True,
                             "Got the wrong DBIerror: %s" %
                             util.line_quote(tb.format_exc()))
        except Exception, e:
            self.fail("Expected a CrawlDBI.DBIerror but got this instead: %s" %
                      util.line_quote(tb.format_exc()))
    
    # -------------------------------------------------------------------------
    def test_get_list_known(self):
        """
        Calling .get_list() should give us back a list of Checkable objects
        representing what is in the table
        """
        # make sure the .db file does not exist
        util.conditional_rm(self.testfile)

        # create some test data (path, type, checksum, cos, last_check)
        testdata = [('/', 'd', '', '', 0),
                    ('/abc', 'd', '', '', 17),
                    ('/xyz', 'f', '', '', 92),
                    ('/abc/foo', 'f', '', '', 5),
                    ('/abc/bar', 'f', '', '', time.time())]

        # testdata has to be sorted by last_check since that's the way get_list
        # will order the list it returns
        testdata.sort(key=lambda x : x[4])

        # create the .db file
        Checkable.ex_nihilo(filename=self.testfile)

        # put the test data into the database
        db = CrawlDBI.DBI(dbname=self.testfile)
        db.insert(table='checkables',
                  fields=['path', 'type', 'checksum', 'cos', 'last_check'],
                  data=testdata[1:])
        db.close()
        
        # run the target routine
        x = Checkable.get_list(self.testfile)

        # we should have gotten back the same number of records as went into
        # the database
        self.expected(len(testdata), len(x))

        # verify that the data from the database matches the testdata that was
        # inserted
        for idx, item in enumerate(x):
            self.expected(testdata[idx][0], item.path)
            self.expected(testdata[idx][1], item.type)
            self.expected(testdata[idx][2], item.checksum)
            self.expected(testdata[idx][3], item.cos)
            self.expected(testdata[idx][4], item.last_check)
    
    # -------------------------------------------------------------------------
    def test_persist_checksum(self):
        """
        Verify that checksum gets stored by persist().
        """
        util.conditional_rm(self.testfile)
        testpath = 'Checkable.py'
        Checkable.ex_nihilo(filename=self.testfile, dataroot=testpath)

        when = time.time()
        
        x = Checkable.get_list(filename=self.testfile)
        m = hashlib.md5()
        m.update(util.contents(testpath))
        csum = ''.join(["%02x" % ord(i) for i in m.digest()])
        x[0].checksum = csum
        x[0].last_check = when
        x[0].persist()

        y = Checkable.get_list(filename=self.testfile)
        self.expected(csum, y[0].checksum)
        self.expected(when, y[0].last_check)

    # -------------------------------------------------------------------------
    def test_persist_dir_duplicate(self):
        """
        Send in a new directory with path matching a duplicate in database
        (rowid == None, last_check == 0, type == 'd'). Exception should be
        thrown.
        """
        util.conditional_rm(self.testfile)
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
    def test_persist_dir_new(self):
        """
        Send in a new directory (rowid == None, last_check == 0, type == 'd',
        path does not match). New record should be added.
        """
        util.conditional_rm(self.testfile)
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
    def test_persist_dir_exist_dd(self):
        """
        Send in a new directory with matching path (rowid == None, last_check
        == 0, type == 'd'). Existing path should not be updated.

        !@! It appears that cos is getting cleared but checksum is preserved.
         Seems like for a directory, either they should both be untouched or
         both be set to the empty string.
        """
        util.conditional_rm(self.testfile)
        Checkable.ex_nihilo(filename=self.testfile)

        now = time.time()
        self.db_add_one(path=self.testpath, type='d', checksum='abc',
                        cos='1234', last_check=now)
        
        x = Checkable.get_list(filename=self.testfile)
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected(now, x[1].last_check)
        self.expected('1234', x[1].cos)

        x[1].last_check = 0
        x[1].checksum = ''
        x[1].cos = '9876'
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
        self.expected(now, x[1].last_check)
        self.expected('abc', x[1].checksum)
        self.expected('', x[1].cos)
    
    # -------------------------------------------------------------------------
    def test_persist_dir_exist_fd(self):
        """
        Send in a new directory with matching path (rowid == None, last_check
        == 0, type == 'f'), changing type (f -> d). Existing path should be
        updated.
        """
        util.conditional_rm(self.testfile)
        Checkable.ex_nihilo(filename=self.testfile)

        now = time.time()
        self.db_add_one(path=self.testpath, type='f', checksum='abc',
                        cos='1234', last_check=now)
        
        x = Checkable.get_list(filename=self.testfile)
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected(now, x[1].last_check)
        self.expected('1234', x[1].cos)
        
        x[1].last_check = 0
        x[1].cos= ''
        try:
            x[1].rowid = None
            x[1].type = 'd'
            x[1].persist()
        except:
            self.fail("Got unexpected exception: "
                      + '"""\n%s\n"""' % tb.format_exc())

        x = Checkable.get_list(filename=self.testfile)
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected('d', x[1].type)
        self.expected(0, x[1].last_check)
        self.expected('', x[1].checksum)
        self.expected('', x[1].cos)
    
    # -------------------------------------------------------------------------
    def test_persist_dir_invalid(self):
        """
        Send in an invalid directory (rowid != None, last_check == 0, type ==
        'd'). Exception should be thrown.
        """
        util.conditional_rm(self.testfile)
        Checkable.ex_nihilo(filename=self.testfile)
        now = time.time()
        self.db_add_one(path='/home', type='d', last_check=now)

        x = Checkable.get_list(filename=self.testfile)

        self.expected(2, len(x))
        c = Checkable(path='/', type='d')
        self.assertTrue(c in x, "expected to find '%s' in '%s'" % (c, x))
        c = Checkable(path='/home', type='d')
        self.assertTrue(c in x, "expected to find '%s' in '%s'" % (c, x))
                      
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
        c = Checkable(path='/', type='d')
        self.assertTrue(c in x, "expected to find '%s' in '%s'" % (c, x))
        Checkable(path='/home', type='d')
        self.assertTrue(c in x, "expected to find '%s' in '%s'" % (c, x))
        self.expected(self.ymdhms(now), self.ymdhms(x[1].last_check))
        
    # -------------------------------------------------------------------------
    def test_persist_dir_update(self):
        """
        Send in an existing directory with a new last_check time (rowid !=
        None, path exists, type == 'd', last_check changed). Last check time
        should be updated.
        """
        util.conditional_rm(self.testfile)
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
    def test_persist_file_duplicate(self):
        """
        Send in a new file with path matching a duplicate in database (rowid ==
        None, last_check == 0, type == 'f'). Exception should be thrown.
        """
        util.conditional_rm(self.testfile)
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
    def test_persist_file_new(self):
        """
        Send in a new file (rowid == None, last_check == 0, path does not
        match, type == 'f'). New record should be added.
        """
        util.conditional_rm(self.testfile)
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
    def test_persist_file_exist_df(self):
        """
        Send in a new file with matching path (rowid == None, last_check
        == 0, type == 'f'). Existing path should be updated.
        """
        util.conditional_rm(self.testfile)
        Checkable.ex_nihilo(filename=self.testfile)
        now = time.time()
        self.db_add_one(last_check=now, type='d')
        x = Checkable.get_list(filename=self.testfile)
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected(now, x[1].last_check)

        x[1].last_check = 0
        x[1].cos = '1234'
        try:
            x[1].rowid = None
            x[1].type = 'f'
            x[1].persist()
        except:
            self.fail("Got unexpected exception: "
                      + '"""\n%s\n"""' % tb.format_exc())

        x = Checkable.get_list(filename=self.testfile)
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected('f', x[1].type)
        self.expected(0, x[1].last_check)
        self.expected('', x[1].checksum)
        self.expected('1234', x[1].cos)
    
    # -------------------------------------------------------------------------
    def test_persist_file_exist_ff(self):
        """
        Send in a new file with matching path (rowid == None, last_check
        == 0, type == 'f'). Existing path should not be updated.
        """
        util.conditional_rm(self.testfile)
        Checkable.ex_nihilo(filename=self.testfile)
        now = time.time()
        self.db_add_one(last_check=now, type='f', checksum='abc', cos='1111')
        x = Checkable.get_list(filename=self.testfile)
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected(now, x[1].last_check)

        x[1].last_check = 0
        x[1].cos = '2222'
        try:
            x[1].rowid = None
            x[1].persist()
        except:
            self.fail("Got unexpected exception: "
                      + '"""\n%s\n"""' % tb.format_exc())

        x = Checkable.get_list(filename=self.testfile)
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected('f', x[1].type)
        self.expected(now, x[1].last_check)
        self.expected('abc', x[1].checksum)
        self.expected('2222', x[1].cos)
        
    # -------------------------------------------------------------------------
    def test_persist_file_invalid(self):
        """
        Send in an invalid file (rowid == None, last_check != 0, type == 'f')
        Exception should be thrown.
        """
        util.conditional_rm(self.testfile)
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
                             "Got the wrong StandardError: %s" %
                             util.line_quote(tb.format_exc()))
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
        util.conditional_rm(self.testfile)
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
    def test_repr(self):
        """
        Verify the output of the __repr__() method
        """
        now = time.time()
        exp = ("Checkable(rowid=17, " +
               "path='/abc/def', " +
               "type='d', " +
               "cos='9999', " +
               "checksum='flibbertygibbet', " + 
               "last_check=%f)" % now)
                    
        x = Checkable(rowid=17, path='/abc/def', type='d',
                      checksum='flibbertygibbet', cos='9999',
                      last_check=now)
        self.expected(exp, x.__repr__())
                        
    # -------------------------------------------------------------------------
    def db_duplicates(self):
        """
        Store a duplicate entry in the file table.
        """
        db = CrawlDBI.DBI(dbname=self.testfile)
        db.insert(table='checkables',
                  fields=['path', 'type', 'checksum', 'cos', 'last_check'],
                  data=[('/abc/def', 'd', '', '', 0)])
        db.insert(table='checkables',
                  fields=['path', 'type', 'checksum', 'cos', 'last_check'],
                  data=[('/abc/def', 'd', '', '', 0)])
        db.close()
        
    # -------------------------------------------------------------------------
    def db_add_one(self,
                   path=testpath,
                   type='f',
                   checksum='',
                   cos='',
                   last_check=0):
        """
        Add one record to the database. All arguments except self are optional.
        """
        db = CrawlDBI.DBI(dbname=self.testfile)
        db.insert(table='checkables',
                  fields=['path', 'type', 'checksum', 'cos', 'last_check'],
                  data=[(path, type, checksum, cos, last_check)])
        db.close()

    # -------------------------------------------------------------------------
    def ymdhms(self, dt):
        """
        Given a tm tuple, return a formatted data/time string
        ("YYYY.mmdd.HHMMSS")
        """
        return time.strftime("%Y.%m%d.%H%M%S", time.localtime(dt))
    
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    toolframe.ez_launch(test='CheckableTest',
                        logfile='crawl_test.log')
    
