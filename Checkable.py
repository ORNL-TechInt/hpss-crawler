#!/usr/bin/env python
"""
An item in HPSS that can be checked for integrity

As we scan the population of files in HPSS, we select a representative sample.
We expect this sample to gradually grow. We will never delete entries from the
sample, so eventually it is expected that it will match the population. In the
meantime, we want to get useful information about the reliability of HPSS by
examining the files in the sample and verifying that each file's data matches
its checksum.

The following information is tracked for each Checkable in the database:

    path
    type (file or directory)
    cos (HPSS class of service)
    last_check (last time the item was checked)

Checksums are held directly in HPSS and managed using the hsi hashcreate and
hashverify commands.

Note that directories are only used to find more files. A directory does not
have a cos or a checksum.
"""
import Alert
import CrawlDBI
import Dimension
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

default_dbname = 'HIC.db'

# -----------------------------------------------------------------------------
class Checkable(object):
    """
    This class represents an HPSS entity that can be checked. That is, it has
    attributes that can be validated (e.g., cos) or it contains other
    things that can be checked (eg., a directory).

    Since HPSS provides a mechanism for storing checksums, we will use it
    through hsi, so we don't store the checksum itself in the object or the
    database.

    Note that only files have cos (and checksum). Directories only have path,
    type, and last_check. So directories will have a cos field, but it will
    always be empty.
    """
    dbname = default_dbname
    # -------------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        """
        Initialize a Checkable object -- set the path, type, checksum, cos, and
        last_check to default values, then update them based on the arguments.
        """
        self.dbname = Checkable.dbname
        self.path = '---'
        self.type = '-'
        self.cos = ''
        self.checksum = 0
        self.last_check = 0
        self.rowid = None
        self.args = args
        self.hsi_prompt = "]:"
        for k in kwargs:
            if k not in ['rowid',
                         'path',
                         'type',
                         'checksum',
                         'cos',
                         'dim',
                         'last_check']:
                raise StandardError("Attribute %s is invalid for Checkable" %
                                    k)
            setattr(self, k, kwargs[k])
        if self.checksum == None:
            self.checksum = 0
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
               "checksum=%d, " % self.checksum +
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
    def addable(self, category):
        # determine which dimensions vote for this item
        rval = False
        votes = {}
        for k in self.dim:
            votes[k] = self.dim[k].vote(category)
            
        # if the dimensions vote for it, roll the dice to decide
        if 0 < sum(votes.values()):
            if random.random() < self.probability:
                rval = True
        return rval
    
    # -------------------------------------------------------------------------
    def check(self, probability):
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

        The value of probability [0.0 .. 1.0] indicates the likelihood with
        which we should check files.

        potential outcomes            return
         read a directory             list of Checkable objects
                                         (files checksummed)
         file checksum fail           Alert
         invalid Checkable type       raise StandardError
         access denied                "access denied"
         verified file checksum       "matched"
         checksum a file              "checksummed"
         skipped a file               "skipped"
         hpss unavailable             "unavailable"
        """
        # fire up hsi
        self.probability = probability
        rval = []
        S = pexpect.spawn('hsi -q', timeout=300)
        # S.logfile = sys.stdout
        which = S.expect([self.hsi_prompt,
                          "HPSS Unavailable",
                          "connect: Connection refused"])
        if 0 != which:
            return "unavailable"    # outcome 5

        # if the current object is a directory,
        #    cd to it
        #    run 'ls -P'
        #    add subdirectories to the list of checkables
        #    decide whether to add each file to the sample
        #    if we add the file to the sample, we run hashcreate for it and set
        #       its checksum member to 1
        # elif the current object is a file,
        #    run hashverify on it
        #    assess the result
        #    raise an alert if the hashverify failed
        # else
        #    raise an exception for invalid Checkable type

        if self.type == 'd':
            S.sendline("cd %s" % self.path)
            S.expect(self.hsi_prompt)
            if 'Not a directory' in S.before:
                # it's not a directory. get the file info (ls -P) and decide
                # whether to add it to the sample.
                S.sendline("ls -P %s" % self.path)
                S.expect(self.hsi_prompt)
                result = self.harvest(S)
                if 0 != self.checksum:
                    rval = "checksummed"
                else:
                    rval = "skipped"

            elif "Access denied" in S.before:
                # it's a directory I don't have access to. Drop it from the
                # database and continue
                self.db_delete()
                rval = "access denied"
                
            else:
                # we have cd'd into a directory. We run "ls -P" for the
                # directory contents. For each subdirectory, we persist it to
                # the database and add it to the list to return. For each file,
                # we decide whether to add it to the sample. If so, we persist
                # it and add it to rval. Otherwise , we drop it.
                S.sendline("ls -P")
                S.expect(self.hsi_prompt)
                rval = self.harvest(S)
                
        elif self.type == 'f':
            S.sendline("hashverify %s" % self.path)
            S.expect(self.hsi_prompt)
            if "%s: (md5) OK" % self.path in S.before:
                # hash was verified successfully
                rval = "matched"
            elif "no valid checksum found" in S.before:
                S.sendline("hashcreate %s" % self.path)
                S.expect(self.hsi_prompt)
                self.checksum = 1
                rval = "checksummed"
            else:
                # hash verification failed
                rval = Alert.Alert("Checksum mismatch: %s" % S.before)
        else:
            # we have an invalid Checkable type (not 'd' or 'f')
            raise StandardError("Invalid Checkable type: %s" % self.type)

        S.sendline("quit")
        S.expect(pexpect.EOF)
        S.close()

        self.last_check = time.time()
        self.persist()
        return rval

    # -------------------------------------------------------------------------
    def db_delete(self):
        db = CrawlDBI.DBI(dbname=self.dbname)
        db.delete(table='checkables',
                  where='path=?',
                  data=(self.path,))
        db.close()

    # -------------------------------------------------------------------------
    @classmethod
    def ex_nihilo(cls, dbname=default_dbname, dataroot='/'):
        """
        Start from scratch. Create the database if necessary. Create the
        table(s) if necessary. Bootstrap the queue by adding the root
        director(ies).
        """
        db = CrawlDBI.DBI(dbname=dbname)
        db.create(table='checkables',
                  fields=['id integer primary key',
                          'path text',
                          'type text',
                          'cos text',
                          'checksum int',
                          'last_check int'])
        rows = db.select(table='checkables', fields=[])
        if len(rows) == 0:
            db.insert(table='checkables',
                      fields=['path', 'type', 'cos', 'checksum', 'last_check'],
                      data=[(dataroot, 'd', '', 0, 0)])
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
    def find_by_path(self):
        """
        Look up the current item in the database based on path, returning the
        database row(s).
        """
        db = CrawlDBI.DBI(dbname=self.dbname)
        rv = db.select(table='checkables',
                       fields=[],
                       where='path=?',
                       data=(self.path,))
        db.close()
        return rv

    # -------------------------------------------------------------------------
    @classmethod
    def get_list(cls, dbname=default_dbname):
        """
        Return the current list of Checkables from the database.
        """
        # Checkable.dbname = filename
        rval = []
        db = CrawlDBI.DBI(dbname=dbname)
        rows = db.select(table='checkables',
                         fields=['rowid', 'path', 'type',
                                 'cos', 'checksum', 'last_check'],
                         orderby='last_check')
        for row in rows:
            new = Checkable(rowid=row[0],
                            path=row[1],
                            type=row[2],
                            cos=row[3],
                            checksum=row[4],
                            last_check=row[5])
            rval.append(new)
        db.close()
        return rval

    # -------------------------------------------------------------------------
    def harvest(self, S):
        """
        Given a list of files and directories from hsi, step through them and
        handle each one.
        """
        if not hasattr(self, 'dim'):
            setattr(self, 'dim', {})
            self.dim['cos'] = Dimension.Dimension(name='cos')
        rval = []
        data = S.before
        for line in data.split("\n"):
            r = Checkable.fdparse(line)
            if None != r:
                if 'd' == r[0]:
                    new = Checkable(path=r[1], type=r[0], cos=r[2])
                    new.persist()
                    rval.append(new)
                elif 'f' == r[0]:
                    if r[1] == self.path and r[0] == self.type:
                        new = self
                    else:
                        new = Checkable(path=r[1], type=r[0], cos=r[2])
                        if new.persist():
                            self.dim['cos'].update_category(new.cos)

                    if self.addable(new.cos):
                        self.dim['cos'].update_category(new.cos,
                                                        p_suminc=0,
                                                        s_suminc=1)
                        new.checksum = 1
                        S.sendline("hashcreate %s" % new.path)
                        S.expect(self.hsi_prompt)
                    else:
                        # we're not adding it to the sample, but if it already
                        # has a checksum, it's already part of the sample and
                        # we need to reflect that.
                        S.sendline("hashlist %s" % new.path)
                        S.expect(self.hsi_prompt)
                        if re.search("\n[0-9a-f]+\smd5\s%s" % new.path,
                                    S.before):
                            new.checksum = 1
                            self.dim['cos'].update_category(new.cos,
                                                            p_suminc=0,
                                                            s_suminc=1)
                    new.persist()
                    rval.append(new)
        self.dim['cos'].persist()
        return rval

    # -------------------------------------------------------------------------
    def persist(self):
        """
        Insert or update the object's entry in the database.

        If rowid is None, the object must be new, so last_check must be 0.0. If
        not, throw an exception.

        If path is empty, something is wrong. Throw an exception.

        If type == 'd' and cos != '', something is wrong. Throw an exception.

        Otherwise, if rowid != None, update by rowid (setting last_check to 0
        if type changes).

        If rowid == None, look the entry up by path. If more than one
        occurrence exists, raise an exception. If only one occurrence exists,
        update it (setting last_check to 0 if type changes). If no occurrence
        exists, insert it.

        Return True if the item was added to the database, otherwise False.
        This is used to decide whether the count the item in the population.
        """
        if self.rowid != None and self.last_check == 0.0:
            raise StandardError("%s has rowid != None, last_check == 0.0" %
                                self)
        if self.rowid == None and self.last_check != 0.0:
            raise StandardError("%s has rowid == None, last_check != 0.0" %
                                self)
        if self.path == '':
            raise StandardError("%s has an empty path" % self)
        if self.type == 'd' and self.cos != '':
            raise StandardError("%s has type 'd', non-empty cos" % self)
        if self.type != 'f' and self.type != 'd':
            raise StandardError("%s has invalid type" % self)

        # assume it's already present
        rval = False
        db = CrawlDBI.DBI(dbname=self.dbname)
        if self.rowid != None:
            # we have a rowid, so it should be in the database
            rows = db.select(table='checkables',
                             fields=[],
                             where='rowid=?',
                             data=(self.rowid,))
            # it wasn't there! raise an exception
            if 0 == len(rows):
                raise StandardError("%s has rowid, should be in database" %
                                    self)
            # exactly one copy is in database. That's good.
            elif 1 == len(rows):
                # oops! the paths don't match. raise an exception
                if rows[0][1] != self.path:
                    raise StandardError("Path value does not match for " +
                                        "%s and %s" % (self, rows[0][1]))
                # The type is changing. Let's set last_check to 0.0. We can
                # also reset checksum and cos. If it's going 'f' -> 'd',
                # checksum should be 0 and cos should be '' for a directory. If
                # it's going 'd' -> 'f', we assume we don't have a checksum for
                # the new file and we don't know what the cos is.
                if rows[0][2] != self.type:
                    self.cos = ''
                    self.checksum = 0
                    self.last_check = 0.0
                # update it
                db.update(table='checkables',
                          fields=['type', 'cos', 'checksum', 'last_check'],
                          where='rowid=?',
                          data=[(self.type, self.cos, self.checksum,
                                 self.last_check, self.rowid)])
            # hmm... we found more than one copy in the database. raise an
            # exception
            else:
                raise StandardError("There appears to be more than one copy " +
                                    "of %s in the database" % self)

        else:
            rows = self.find_by_path()
            # it's not there -- we need to insert it
            if 0 == len(rows):
                db.insert(table='checkables',
                          fields=['path', 'type', 'cos', 'checksum',
                                  'last_check'],
                          data=[(self.path, self.type, self.cos, self.checksum,
                                 self.last_check)])
                rval = True
            # it is in the database -- we need to update it
            elif 1 == len(rows):
                # if type is changing, last_check resets to 0.0, checksum to 0,
                # cos to ''
                if rows[0][2] != self.type:
                    self.last_check = 0.0
                    self.checksum = 0
                    self.cos = ''
                    flist=['type', 'cos', 'checksum', 'last_check']
                    dlist=[(self.type, self.cos, self.checksum,
                            self.last_check, rows[0][0])]
                elif rows[0][5] < self.last_check:
                    flist=['type', 'cos', 'checksum', 'last_check']
                    dlist=[(self.type, self.cos, self.checksum,
                            self.last_check, rows[0][0])]
                else:
                    flist=['type', 'cos', 'checksum']
                    dlist=[(self.type, self.cos, self.checksum, rows[0][0])]
                # update it
                db.update(table='checkables',
                          fields=flist,
                          where='rowid=?',
                          data=dlist)
            # more than a single copy in the database -- raise an exception
            else:
                raise StandardError("There appears to be more than one copy " +
                                    "of %s in the database" % self)
        db.close()
        return rval
    
    # -------------------------------------------------------------------------
    @classmethod
    def set_dbname(cls, dbname=default_dbname):
        """
        Set the database name for future Checkable objects
        """
        Checkable.dbname = dbname

# -----------------------------------------------------------------------------
def setUpModule():
    """
    Create the test directory in preparation to run the tests.
    """
    Checkable.set_dbname(CheckableTest.testdb)
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
    testdb = '%s/test.db' % testdir
    methods = ['__init__', 'ex_nihilo', 'get_list', 'check', 'persist']
    testpath = '/home/tpb/TODO'
    
    # -------------------------------------------------------------------------
    def test_check_dir(self):
        """
        Calling .check() on a directory should give us back a list of Checkable
        objects representing the entries in the directory
        """
        util.conditional_rm(self.testdb)
        Checkable.ex_nihilo(dbname=self.testdb)
        testdir='/home/tpb/hic_test'
        self.db_add_one(path=testdir, type='d')
        x = Checkable.get_list(dbname=self.testdb)
        
        self.expected(2, len(x))
        dirlist = x[1].check(1.0)
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
                self.expected(1, c.checksum)
            elif c.path == "%s/crawler.tar.idx" % testdir:
                self.expected(1, c.checksum)
            elif c.path == "%s/subdir1" % testdir:
                self.expected(0, c.checksum)
            elif c.path == "%s/subdir2" % testdir:
                self.expected(0, c.checksum)
                
    # -------------------------------------------------------------------------
    def test_check_file(self):
        """
        Calling .check() on a file should execute the check actions for that
        file and update the item's last_check value.
        """
        util.conditional_rm(self.testdb)
        Checkable.ex_nihilo(dbname=self.testdb)
        testdir='/home/tpb/hic_test'
        self.db_add_one(path=testdir, type='d')
        self.db_add_one(path=testdir + '/crawler.tar', type='f')
        self.db_add_one(path=testdir + '/crawler.tar.idx', type='f')

        x = Checkable.get_list(dbname=self.testdb)
        checked = []
        for item in [z for z in x if z.type == 'f']:
            self.expected(0, item.last_check)
            result = item.check(-1)
            if type(result) == str and result == "unavailable":
                return

        x = Checkable.get_list(dbname=self.testdb)
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
        self.expected(Checkable.dbname, x.dbname)
        self.expected('---', x.path)
        self.expected('-', x.type)
        # self.expected('', x.checksum)
        self.expected('', x.cos)
        self.expected(0, x.last_check)
        self.expected(None, x.rowid)
            
    # -------------------------------------------------------------------------
    def test_ctor_args(self):
        """
        Verify that the constructor accepts and sets rowid, path, type,
        cos, and last_check
        """
        x = Checkable(rowid=3, path='/one/two/three', type='f', cos='6002',
                      last_check=72)
        for method in self.methods:
            self.assertEqual(method in dir(x), True,
                         "Checkable object is missing %s method" % method)
        self.expected(3, x.rowid)
        self.expected('/one/two/three', x.path)
        self.expected('f', x.type)
        self.expected('6002', x.cos)
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
                             "Got the wrong StandardError: %s" %
                             util.line_quote(tb.format_exc()))
        except Exception, e:
            self.fail("Expected a StandardError but got this instead: %s" %
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
                      last_check=now)
        b = Checkable(rowid=97,
                      path='/foo/bar',
                      type='f',
                      cos='23743',
                      last_check=now + 23)
        c = Checkable(rowid=43,
                      path='/foo/bar',
                      type='d',
                      cos='2843',
                      last_check=now - 32)
        d = Checkable(rowid=18,
                      path='/foo/fiddle',
                      type='f',
                      cos='9222',
                      last_check=now + 10132)
        e = lambda: None
        setattr(e, 'path', '/foo/bar')
        setattr(e, 'type', 'f')
        f = Checkable(rowid=49,
                      path='/foo/fiddle',
                      type='d',
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
        util.conditional_rm(self.testdb)

        # this call should create it
        Checkable.ex_nihilo(dbname=self.testdb, dataroot="/home/somebody")

        # check that it exists
        self.assertEqual(os.path.exists(self.testdb), True,
                         "File '%s' should be created by ex_nihilo()" %
                         (self.testdb))

        # assuming it does, look inside and make sure the checkables table got
        # initialized correctly
        db = CrawlDBI.DBI(dbname=self.testdb)

        # there should be one row
        rows = db.select(table='checkables', fields=[])
        self.expected(1, len(rows))

        # the one row should reference the root directory
        [(max_id,)] = db.select(table='checkables', fields=['max(rowid)'])
        self.expected(1, max_id)                       # id
        self.expected('/home/somebody', rows[0][1])    # path
        self.expected('d', rows[0][2])                 # type
        self.expected('', rows[0][3])                  # cos
        self.expected(0, rows[0][4])                   # last_check

    # -------------------------------------------------------------------------
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

        # create a dummy .db file and set its mtime back by 500 seconds
        testhelp.touch(self.testdb)
        s = os.stat(self.testdb)
        newtime = s[stat.ST_MTIME] - 500
        os.utime(self.testdb, (s[stat.ST_ATIME], newtime))

        # create a dummy 'checkables' table in the test database
        Checkable.ex_nihilo(dbname=self.testdb)
        pre = os.stat(self.testdb)
        
        # call the test target routine
        time.sleep(1.0)
        Checkable.ex_nihilo(dbname=self.testdb)

        # verify that the file's mtime is unchanged and its size is unchanged
        post = os.stat(self.testdb)
        self.expected(self.ymdhms(pre[stat.ST_MTIME]),
                      self.ymdhms(post[stat.ST_MTIME]))
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

        # create a dummy .db file and set its mtime back by 500 seconds
        testhelp.touch(self.testdb)
        s = os.stat(self.testdb)
        newtime = s[stat.ST_MTIME] - 500
        os.utime(self.testdb, (s[stat.ST_ATIME], newtime))

        # call the test target routine
        Checkable.ex_nihilo(dbname=self.testdb)

        # verify that the file exists and the table does also
        self.assertTrue(os.path.exists(self.testdb),
                        "Expected %s to exist" % self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb)
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

        # this call should create it
        Checkable.ex_nihilo(dbname=self.testdb)

        # check that it exists
        self.assertEqual(os.path.exists(self.testdb), True,
                         "File '%s' should be created by ex_nihilo()" %
                         (self.testdb))

        # assuming it does, look inside and make sure the checkables table got
        # initialized correctly
        db = CrawlDBI.DBI(dbname=self.testdb)
        
        # there should be one row
        rows = db.select(table='checkables', fields=[])
        self.expected(1, len(rows))

        # the one row should reference the root directory
        [(max_id, )] = db.select(table='checkables', fields=['max(rowid)'])
        self.expected(1, max_id)           # id
        self.expected('/', rows[0][1])     # path
        self.expected('d', rows[0][2])     # type
        self.expected('', rows[0][3])      # cos
        self.expected(0, rows[0][4])       # last_check
        
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
        util.conditional_rm(self.testdb)

        try:
            Checkable.get_list(dbname=self.testdb)
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
        util.conditional_rm(self.testdb)

        # create some test data (path, type, cos, last_check)
        testdata = [('/', 'd', '', 0),
                    ('/abc', 'd', '', 17),
                    ('/xyz', 'f', '', 92),
                    ('/abc/foo', 'f', '', 5),
                    ('/abc/bar', 'f', '', time.time())]

        # testdata has to be sorted by last_check since that's the way get_list
        # will order the list it returns
        testdata.sort(key=lambda x : x[3])

        # create the .db file
        Checkable.ex_nihilo(dbname=self.testdb)

        # put the test data into the database
        db = CrawlDBI.DBI(dbname=self.testdb)
        db.insert(table='checkables',
                  fields=['path', 'type', 'cos', 'last_check'],
                  data=testdata[1:])
        db.close()
        
        # run the target routine
        x = Checkable.get_list(self.testdb)

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
    def test_persist_last_check(self):
        """
        Verify that last_check gets stored by persist().
        """
        util.conditional_rm(self.testdb)
        testpath = 'Checkable.py'
        Checkable.ex_nihilo(dbname=self.testdb, dataroot=testpath)

        when = time.time()
        
        x = Checkable.get_list(dbname=self.testdb)
        x[0].last_check = when
        x[0].persist()

        y = Checkable.get_list(dbname=self.testdb)
        self.expected(when, y[0].last_check)

    # -------------------------------------------------------------------------
    def test_persist_dir_duplicate(self):
        """
        Send in a new directory with path matching a duplicate in database
        (rowid == None, last_check == 0, type == 'd'). Exception should be
        thrown.
        """
        util.conditional_rm(self.testdb)
        Checkable.ex_nihilo(dbname=self.testdb)
        self.db_duplicates()
        x = Checkable.get_list(dbname=self.testdb)
        self.expected(3, len(x))

        foo = Checkable(path='/abc/def', type='d')
        try:
            foo.persist()
            self.fail("Expected an exception but didn't get one.")
        except AssertionError:
            raise
        except StandardError, e:
            self.assertEqual('There appears to be more than one' in str(e),
                             True,
                             "Got the wrong StandardError: %s" %
                             util.line_quote(tb.format_exc()))

        x = Checkable.get_list(dbname=self.testdb)
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
        Checkable.ex_nihilo(dbname=self.testdb)
        x = Checkable.get_list(dbname=self.testdb)
        self.expected(1, len(x))

        foo = Checkable(path='/abc/def', type='d')
        foo.persist()

        x = Checkable.get_list(dbname=self.testdb)
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
        Checkable.ex_nihilo(dbname=self.testdb)

        now = time.time()
        self.db_add_one(path=self.testpath, type='d', last_check=now)
        
        x = Checkable.get_list(dbname=self.testdb)
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected(now, x[1].last_check)
        self.expected('', x[1].cos)

        x[1].last_check = 0
        x[1].rowid = None
        x[1].persist()

        x = Checkable.get_list(dbname=self.testdb)
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
        Checkable.ex_nihilo(dbname=self.testdb)

        now = time.time()
        self.db_add_one(path=self.testpath, type='f',
                        cos='1234', last_check=now)
        
        x = Checkable.get_list(dbname=self.testdb)
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected(now, x[1].last_check)
        self.expected('1234', x[1].cos)
        
        x[1].last_check = 0
        x[1].cos= ''
        x[1].rowid = None
        x[1].type = 'd'
        x[1].persist()

        x = Checkable.get_list(dbname=self.testdb)
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected('d', x[1].type)
        self.expected(0, x[1].last_check)
        self.expected('', x[1].cos)
    
    # -------------------------------------------------------------------------
    def test_persist_dir_invalid(self):
        """
        Send in an invalid directory (rowid != None, last_check == 0, type ==
        'd'). Exception should be thrown.
        """
        util.conditional_rm(self.testdb)
        Checkable.ex_nihilo(dbname=self.testdb)
        now = time.time()
        self.db_add_one(path='/home', type='d', last_check=now)

        x = Checkable.get_list(dbname=self.testdb)

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
        except AssertionError:
            raise
        except StandardError, e:
            self.assertEqual("has rowid != None, last_check == 0.0" in str(e),
                             True,
                             "Got the wrong StandardError: %s" %
                             util.line_quote(tb.format_exc()))

        x = Checkable.get_list(dbname=self.testdb)
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
        util.conditional_rm(self.testdb)
        Checkable.ex_nihilo(dbname=self.testdb)

        x = Checkable.get_list(dbname=self.testdb)
        self.expected(1, len(x))
        self.expected(0, x[0].last_check)

        x[0].last_check = now = time.time()
        x[0].persist()

        x = Checkable.get_list(dbname=self.testdb)
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
        Checkable.ex_nihilo(dbname=self.testdb)
        self.db_add_one(path=self.testpath, type='f')
        self.db_add_one(path=self.testpath, type='f')

        x = Checkable.get_list(dbname=self.testdb)
        self.expected(3, len(x))
        self.assertEqual(x[1], x[2],
                         "There should be a duplicate entry in the database.")
        
        foo = Checkable(path=self.testpath, type='f')
        try:
            foo.persist()
            self.fail("Expected an exception but didn't get one.")
        except AssertionError:
            raise
        except StandardError, e:
            self.assertEqual('There appears to be more than one' in str(e),
                             True,
                             "Got the wrong StandardError: %s" %
                             util.line_quote(tb.format_exc()))

        x = Checkable.get_list(dbname=self.testdb)
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
        Checkable.ex_nihilo(dbname=self.testdb)

        x = Checkable.get_list(dbname=self.testdb)
        self.expected(1, len(x))
        self.expected("/", x[0].path)
        self.expected(0, x[0].last_check)

        foo = Checkable(path=self.testpath, type='f')
        foo.persist()

        x = Checkable.get_list(dbname=self.testdb)
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
        Checkable.ex_nihilo(dbname=self.testdb)
        now = time.time()
        self.db_add_one(last_check=now, type='d')
        x = Checkable.get_list(dbname=self.testdb)
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected(now, x[1].last_check)

        x[1].last_check = 0
        x[1].cos = '1234'
        x[1].checksum = 1
        x[1].rowid = None
        x[1].type = 'f'
        x[1].persist()

        x = Checkable.get_list(dbname=self.testdb)
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected('f', x[1].type)
        self.expected(0, x[1].checksum)
        self.expected(0, x[1].last_check)
        self.expected('', x[1].cos)
    
    # -------------------------------------------------------------------------
    def test_persist_file_exist_ff(self):
        """
        Send in a new file with matching path (rowid == None, last_check
        == 0, type == 'f'). Existing path should not be updated.
        """
        util.conditional_rm(self.testdb)
        Checkable.ex_nihilo(dbname=self.testdb)
        now = time.time()
        self.db_add_one(last_check=now, type='f', cos='1111')
        x = Checkable.get_list(dbname=self.testdb)
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected(now, x[1].last_check)

        x[1].last_check = 0
        x[1].cos = '2222'
        x[1].rowid = None
        x[1].persist()

        x = Checkable.get_list(dbname=self.testdb)
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected('f', x[1].type)
        self.expected(now, x[1].last_check)
        self.expected('2222', x[1].cos)
        
    # -------------------------------------------------------------------------
    def test_persist_file_invalid(self):
        """
        Send in an invalid file (rowid == None, last_check != 0, type == 'f')
        Exception should be thrown.
        """
        util.conditional_rm(self.testdb)
        Checkable.ex_nihilo(dbname=self.testdb)
        self.db_add_one()
        x = Checkable.get_list(dbname=self.testdb)
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected(0, x[1].last_check)

        now = time.time()
        x[1].last_check = now
        x[1].rowid = None
        try:
            x[1].persist()
            self.fail("Expected exception but didn't get one")
        except AssertionError:
            raise
        except StandardError, e:
            self.assertEqual("has rowid == None, last_check != 0.0" in str(e),
                             True,
                             "Got the wrong StandardError: %s" %
                             util.line_quote(tb.format_exc()))

        x = Checkable.get_list(dbname=self.testdb)
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
        Checkable.ex_nihilo(dbname=self.testdb)
        self.db_add_one()
        x = Checkable.get_list(dbname=self.testdb)
        self.expected(2, len(x))
        self.expected(self.testpath, x[1].path)
        self.expected(0, x[1].last_check)

        now = time.time()
        x[1].last_check = now
        x[1].persist()

        x = Checkable.get_list(dbname=self.testdb)
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
        db = CrawlDBI.DBI(dbname=self.testdb)
        db.insert(table='checkables',
                  fields=['path', 'type', 'cos', 'last_check'],
                  data=[(path, type, cos, last_check)])
        db.close()

    # -------------------------------------------------------------------------
    def db_duplicates(self):
        """
        Store a duplicate entry in the file table.
        """
        db = CrawlDBI.DBI(dbname=self.testdb)
        db.insert(table='checkables',
                  fields=['path', 'type', 'cos', 'last_check'],
                  data=[('/abc/def', 'd', '', 0)])
        db.insert(table='checkables',
                  fields=['path', 'type', 'cos', 'last_check'],
                  data=[('/abc/def', 'd', '', 0)])
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
    
