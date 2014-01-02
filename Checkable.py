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
import hpss
import pexpect
import random
import re
import testhelp
import time
import util

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
    # -------------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        """
        Initialize a Checkable object -- set the path, type, checksum, cos, and
        last_check to default values, then update them based on the arguments.
        """
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
        self.dim = {}
        self.dim['cos'] = Dimension.get_dim('cos')
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
    def add_to_sample(self, hsi, already_hashed=False):
        """
        Add the current Checkable to the sample. If already_hashed is True,
        this is a file for which a checksum has already been computed. We just
        need to record that fact by setting its checksum member to 1 and
        updating the sample count.

        If already_hashed is False, we need to carry out the following steps:

         1) run hashcreate on the file
         2) set checksum to non-zero to record that we have a checksum
         3) update the sample count in the Dimension object
        """
        if not already_hashed:
            l = util.get_logger()
            l.info("%s: starting hashcreate on %s", util.my_name(), self.path)
            hsi.hashcreate(self.path)
            l.info("%s: completed hashcreate on %s", util.my_name(), self.path)

        if self.checksum == 0:
            self.checksum = 1
            self.dim['cos'].update_category(self.cos,
                                            p_suminc=0,
                                            s_suminc=1)
        self.dim['cos'].persist()

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
        try:
            h = hpss.HSI(timeout=300)
        except hpss.HSIerror, e:
            return "unavailable"
        
        if self.type == 'd':
            rsp = h.chdir(self.path)
            if 'Not a directory' in rsp:
                # It's not a directory. Get the file info (ls -P) and decide
                # whether to add it to the sample.
                h.lsP(self.path)
                result = self.harvest(h)
                if 0 != self.checksum:
                    rval = "checksummed"
                else:
                    rval = "skipped"

            elif "Access denied" in rsp:
                # It's a directory we don't have access to. Carry on.
                rval = "access denied"
                
            else:
                # We have cd'd into a directory. We run "ls -P" for the
                # directory contents and run harvest on that.
                h.lsP("")
                rval = self.harvest(h)
                
        elif self.type == 'f':
            if self.checksum == 0:
                if self.addable(self.cos):
                    self.add_to_sample(h)
                    rval = "checksummed"
            else:
                rsp = h.hashverify(self.path)
                if "%s: (md5) OK" % self.path in rsp:
                    # hash was verified successfully
                    rval = "matched"
                elif "no valid checksum found" in rsp:
                    # no hash is available -- see if we want to add it
                    if self.addable(self.cos):
                        self.add_to_sample(h)
                        rval = "checksummed"
                else:
                    # hash verification failed
                    rval = Alert.Alert("Checksum mismatch: %s" % rsp)
        else:
            # we have an invalid Checkable type (not 'd' or 'f')
            raise StandardError("Invalid Checkable type: %s" % self.type)

        h.quit()

        self.last_check = time.time()
        self.persist()
        return rval

    # -------------------------------------------------------------------------
    def db_delete(self):
        db = CrawlDBI.DBI()
        db.delete(table='checkables',
                  where='path=?',
                  data=(self.path,))
        db.close()

    # -------------------------------------------------------------------------
    @classmethod
    def ex_nihilo(cls, dataroot='/'):
        """
        Start from scratch. Create the database if necessary. Create the
        table(s) if necessary. Bootstrap the queue by adding the root
        director(ies).
        """
        db = CrawlDBI.DBI()
        db.create(table='checkables',
                  fields=['rowid integer primary key autoincrement',
                          'path text',
                          'type text',
                          'cos text',
                          'checksum int',
                          'last_check int'])
        if type(dataroot) == str:
            dataroot = [ dataroot ]

        if type(dataroot) == list:
            for root in dataroot:
                r = Checkable(path=root, type='d')
                r.persist()

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
        db = CrawlDBI.DBI()
        rv = db.select(table='checkables',
                       fields=[],
                       where='path=?',
                       data=(self.path,))
        db.close()
        return rv

    # -------------------------------------------------------------------------
    @classmethod
    def get_list(cls):
        """
        Return the current list of Checkables from the database.
        """
        # Checkable.dbname = filename
        rval = []
        db = CrawlDBI.DBI()
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
    def harvest(self, hsi):
        """
        Given a list of files and directories from hsi, step through them and
        handle each one.

        For each file, we have to decide whether we're adding it to the sample
        or not. In any case, we persist it to the database. If we're adding it
        to the sample, we have to 1) issue hashcreate on it, 2) set its
        checksum to 1, and 3) update the sample count for it in the Dimension
        object.
        """
        rval = []
        data = hsi.before()
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
                        new.add_to_sample(hsi)
                    else:
                        # If it's not in the sample, but already has a
                        # checksum, it's already part of the sample and we need
                        # to reflect that.
                        rsp = hsi.hashlist(new.path)
                        if ((new.checksum == 0) and
                            (re.search("\n[0-9a-f]+\smd5\s%s" % new.path,
                                       rsp))):
                            new.add_to_sample(hsi, already_hashed=True)

                    new.persist()
                    rval.append(new)

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
        db = CrawlDBI.DBI()
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
                elif ((self.cos == rows[0][3]) and
                      (self.checksum == rows[0][4]) and
                      (self.last_check == rows[0][5])):
                    # everything matches -- no need to update it
                    flist = []
                    dlist = []
                else:
                    flist=['type', 'cos', 'checksum']
                    dlist=[(self.type, self.cos, self.checksum, rows[0][0])]

                # if an update is needed, do it
                if flist != []:
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
    
