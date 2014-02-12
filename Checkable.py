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
import CrawlConfig
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
        # where the item is in HPSS
        self.path = '---'
        # file ('f') or directory ('d')
        self.type = '-'
        # which COS the file is in (empty for directories)
        self.cos = ''
        # 1 if we have a checksum stored, else 0
        self.checksum = 0
        # how many times we've tried and failed to retrieve the file content
        self.fails = 0
        # whether we've reported that retrievals are failing for this file
        self.reported = 0
        # when was the last check of this file (epoch time)
        self.last_check = 0
        # this item's row id in the database
        self.rowid = None
        # how likely are we to add an item to the sample?
        self.probability = 0.1
        # whether this object is in the database
        self.in_db = False
        # whether this object has been changed
        self.dirty = False
        # non keyword arguments
        self.args = args

        for k in kwargs:
            if k not in ['rowid',
                         'path',
                         'type',
                         'checksum',
                         'cos',
                         'dim',
                         'fails',
                         'reported',
                         'last_check',
                         'probability',
                         'in_db',
                         'dirty']:
                raise StandardError("Attribute %s is invalid for Checkable" %
                                    k)
            setattr(self, k, kwargs[k])
        if self.checksum is None:
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
            util.log("starting hashcreate on %s", self.path)
            rsp = hsi.hashcreate(self.path)
            if "TIMEOUT" in rsp or "ERROR" in rsp:
                util.log("hashcreate transfer failed on %s", self.path)
                self.set('fails', self.fails + 1)
                return "skipped"
            elif "Access denied" in rsp:
                util.log("hashcreate failed with 'access denied' on %s",
                         self.path)
                return "access denied"
            else:
                util.log("completed hashcreate on %s", self.path)
            
        if self.checksum == 0:
            self.set('checksum', 1)
            return "checksummed"

    # -------------------------------------------------------------------------
    def addable(self, category):
        """
        Determine which Dimensions want this item added. Note that we want this
        routine to be general across dimensions so we don't want it to assume
        anything about the dimension it's checking (like that it's named 'cos'
        for example). That why calls to this pass in cos rather than looking at
        the value in the object.
        """
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
    def check(self):
        """
        For a directory:
         - get a list of its contents if possible,
         - create a Checkable object for each item and persist it to the
           database
         - return the list of Checkables found in the directory
        For a file:
         - if it already has a hash, add it to the sample if not already
           and verify it
         - if it does not have a hash, decide whether to add it or not

        The value of probability [0.0 .. 1.0] indicates the likelihood with
        which we should check files.

        potential outcomes            return
         read a directory             list of Checkable objects
         file checksum fail           Alert
         invalid Checkable type       raise StandardError
         access denied                "access denied"
         verified file checksum       "matched"
         checksum a file              "checksummed"
         skipped a file               "skipped"
         hpss unavailable             "unavailable"

        Here we examine a population member, count it as a member of the
        population, decide whether to add it to the sample, and if so, count it
        as a sample member.

        First, we have to make all the decisions and update the object
        accordingly.

        Then, we persist the object to the database.

        Finally, we load the dimension object to ensure it's up to date.
        """
        # fire up hsi
        # self.probability = probability
        rval = []
        cfg = CrawlConfig.get_config()
        # hsi_timeout = int(cfg.get_d('crawler', 'hsi_timeout', 300))
        try:
            # h = hpss.HSI(timeout=hsi_timeout, verbose=True)
            h = hpss.HSI(verbose=True)
            util.log("started hsi with pid %d" % h.pid())
        except hpss.HSIerror, e:
            return "unavailable"

        if self.type == 'd':
            rsp = h.lsP(self.path)
            if "Access denied" in rsp:
                rval = "access denied"
            else:
                for line in rsp.split("\n"):
                    new = Checkable.fdparse(line)
                    if None != new:
                        rval.append(new)
                        new.load()
                        new.persist()
                        # returning list of items found in the directory
        elif self.type == 'f':
            if self.checksum == 0:
                if self.has_hash(h):
                    self.add_to_sample(h, already_hashed=True)
                    rval = self.verify(h)
                    # returning "matched", "checksummed", "skipped", or Alert()
                elif self.addable(self.cos):
                    rval = self.add_to_sample(h)
                    # returning "access denied" or "checksummed"
                else:
                    rval = "skipped"
            else:
                rval = self.verify(h)
                # returning "matched", "checksummed", "skipped", or Alert()
        else:
            raise StandardError("Invalid Checkable type: %s" % self.type)

        if (3 < self.fails) and (0 == self.reported):
            self.fail_report(h.before())
            rval = "skipped"

        h.quit()

        self.set('last_check', time.time())
        self.persist()
        return rval

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
                  fields=['rowid      integer primary key autoincrement',
                          'path       text',
                          'type       text',
                          'cos        text',
                          'checksum   int',
                          'last_check int',
                          'fails      int',
                          'reported   int'])
        if type(dataroot) == str:
            dataroot = [ dataroot ]

        if type(dataroot) == list:
            for root in dataroot:
                r = Checkable(path=root, type='d', in_db=False, dirty=True)
                r.load()
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
            return Checkable(path=fname, type=cls.map[type], cos=cos)

        ltup = re.findall(cls.rgxl, value)
        if ltup:
            (type, ign1, ign2, ign3, ign4, fname) = ltup[0]
            return Checkable(path=fname, type=cls.map[type])

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
    def get_list(cls, prob=0.1):
        """
        Return the current list of Checkables from the database.
        """
        # Checkable.dbname = filename
        rval = []
        db = CrawlDBI.DBI()
        rows = db.select(table='checkables',
                         fields=['rowid', 'path', 'type',
                                 'cos', 'checksum', 'last_check',
                                 'fails', 'reported'],
                         orderby='last_check')
        for row in rows:
            if row[6] is None:
                fails = 0
            else:
                fails = row[6]
            new = Checkable(rowid=row[0],
                            path=row[1],
                            type=row[2],
                            cos=row[3],
                            checksum=row[4],
                            last_check=row[5],
                            fails=fails,
                            reported=row[7],
                            probability=prob,
                            in_db=True,
                            dirty=False)
            rval.append(new)
        db.close()
        return rval

    # -------------------------------------------------------------------------
    def fail_report(self, msg):
        try:
            f = self.fail_report_fh
        except AttributeError:
            cfg = CrawlConfig.get_config()
            filename = cfg.get('checksum-verifier', 'fail_report')
            self.fail_report_fh = open(filename, 'a')
            f = self.fail_report_fh
            
        f.write("Failure retrieving file %s: '%s'\n" % (self.path, msg))
        self.set('reported', 1)
        f.flush()

    # -------------------------------------------------------------------------
    def has_hash(self, h):
        """
        Return True if the current file has a hash, False otherwise.
        """
        rsp = h.hashlist(self.path)
        if re.search("\n[0-9a-f]+\smd5\s%s" % self.path, rsp):
            rval = True
        else:
            rval = False
        return rval

    # -------------------------------------------------------------------------
    def load(self):
        db = CrawlDBI.DBI()
        if self.rowid is not None:
            rows = db.select(table='checkables',
                             fields=['rowid', 'path', 'type', 'cos',
                                     'checksum', 'last_check', 'fails',
                                     'reported'],
                             where="rowid = ?",
                             data=(self.rowid,))
        else:
            rows = db.select(table='checkables',
                             fields=['rowid', 'path', 'type', 'cos',
                                     'checksum', 'last_check', 'fails',
                                     'reported'],
                             where="path = ?",
                             data=(self.path,))
        if 0 == len(rows):
            self.in_db = False
        elif 1 == len(rows):
            self.in_db = True
            self.rowid = rows[0][0]
            self.path = rows[0][1]
            self.type = rows[0][2]
            self.cos = rows[0][3]
            self.checksum = rows[0][4]
            self.last_check = rows[0][5]
            if rows[0][6] is None:
                self.fails = 0
            else:
                self.fails = int(rows[0][6])
            if rows[0][7] is None:
                self.reported = 0
            else:
                self.reported = int(rows[0][7])
            self.dirty = False
        else:
            raise StandardError("There appears to be more than one copy " +
                                "of %s in the database" % self)
            
        db.close()
        
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
        """
        if self.rowid == None and self.last_check != 0.0:
            raise StandardError("%s has rowid == None, last_check != 0.0" %
                                self)
        if self.path == '':
            raise StandardError("%s has an empty path" % self)
        if self.type == 'd' and self.cos != '':
            raise StandardError("%s has type 'd', non-empty cos" % self)
        if self.type != 'f' and self.type != 'd':
            raise StandardError("%s has invalid type" % self)

        db = CrawlDBI.DBI()
        if not self.in_db:
            # insert it
            db.insert(table='checkables',
                      fields=['path',
                              'type',
                              'cos',
                              'checksum',
                              'last_check',
                              'fails',
                              'reported'],
                      data=[(self.path,
                             self.type,
                             self.cos,
                             self.checksum,
                             self.last_check,
                             self.fails,
                             self.reported)])
            self.in_db = True
        elif self.dirty:
            # update it
            if self.rowid is not None:
                db.update(table='checkables',
                          fields=['path',
                                  'type',
                                  'cos',
                                  'checksum',
                                  'last_check',
                                  'fails',
                                  'reported'],
                          where="rowid = ?",
                          data=[(self.path,
                                 self.type,
                                 self.cos,
                                 self.checksum,
                                 self.last_check,
                                 self.fails,
                                 self.reported,
                                 self.rowid)])
            else:
                db.update(table='checkables',
                          fields=['type',
                                  'cos',
                                  'checksum',
                                  'last_check',
                                  'fails',
                                  'reported'],
                          where="path = ?",
                          data=[(self.type,
                                 self.cos,
                                 self.checksum,
                                 self.last_check,
                                 self.fails,
                                 self.reported,
                                 self.path)])
            self.dirty = False

        self.dim['cos'].load()
        db.close()
    
        # # assume it's already present
        # rval = False
        # db = CrawlDBI.DBI()
        # if self.rowid != None:
        #     # we have a rowid, so it should be in the database
        #     rows = db.select(table='checkables',
        #                      fields=[],
        #                      where='rowid=?',
        #                      data=(self.rowid,))
        #     # it wasn't there! raise an exception
        #     if 0 == len(rows):
        #         raise StandardError("%s has rowid, should be in database" %
        #                             self)
        #     # exactly one copy is in database. That's good.
        #     elif 1 == len(rows):
        #         # oops! the paths don't match. raise an exception
        #         if rows[0][1] != self.path:
        #             raise StandardError("Path value does not match for " +
        #                                 "%s and %s" % (self, rows[0][1]))
        #         # The type is changing. Let's set last_check to 0.0. We can
        #         # also reset checksum and cos. If it's going 'f' -> 'd',
        #         # checksum should be 0 and cos should be '' for a directory. If
        #         # it's going 'd' -> 'f', we assume we don't have a checksum for
        #         # the new file and we don't know what the cos is.
        #         if rows[0][2] != self.type:
        #             self.cos = ''
        #             self.checksum = 0
        #             self.last_check = 0.0
        #         # update it
        #         db.update(table='checkables',
        #                   fields=['type', 'cos', 'checksum', 'last_check'],
        #                   where='rowid=?',
        #                   data=[(self.type, self.cos, self.checksum,
        #                          self.last_check, self.rowid)])
        #     # hmm... we found more than one copy in the database. raise an
        #     # exception
        #     else:
        #         raise StandardError("There appears to be more than one copy " +
        #                             "of %s in the database" % self)
        # 
        # else:
        #     rows = self.find_by_path()
        #     # it's not there -- we need to insert it
        #     if 0 == len(rows):
        #         db.insert(table='checkables',
        #                   fields=['path', 'type', 'cos', 'checksum',
        #                           'last_check'],
        #                   data=[(self.path, self.type, self.cos, self.checksum,
        #                          self.last_check)])
        #         rval = True
        #     # it is in the database -- we need to update it
        #     elif 1 == len(rows):
        #         # if type is changing, last_check resets to 0.0, checksum to 0,
        #         # cos to ''
        #         if rows[0][2] != self.type:
        #             self.last_check = 0.0
        #             self.checksum = 0
        #             self.cos = ''
        #             flist=['type', 'cos', 'checksum', 'last_check']
        #             dlist=[(self.type, self.cos, self.checksum,
        #                     self.last_check, rows[0][0])]
        #         elif rows[0][5] < self.last_check:
        #             flist=['type', 'cos', 'checksum', 'last_check']
        #             dlist=[(self.type, self.cos, self.checksum,
        #                     self.last_check, rows[0][0])]
        #         elif ((self.cos == rows[0][3]) and
        #               (self.checksum == rows[0][4]) and
        #               (self.last_check == rows[0][5])):
        #             # everything matches -- no need to update it
        #             flist = []
        #             dlist = []
        #         else:
        #             flist=['type', 'cos', 'checksum']
        #             dlist=[(self.type, self.cos, self.checksum, rows[0][0])]
        # 
        #         # if an update is needed, do it
        #         if flist != []:
        #             db.update(table='checkables',
        #                       fields=flist,
        #                       where='rowid=?',
        #                       data=dlist)
        #     # more than a single copy in the database -- raise an exception
        #     else:
        #         raise StandardError("There appears to be more than one copy " +
        #                             "of %s in the database" % self)

    # -------------------------------------------------------------------------
    def set(self, attrname, value):
        setattr(self, attrname, value)
        self.dirty = True

    # -------------------------------------------------------------------------
    def verify(self, h):
        """
        Attempt to verify the current file.
        """
        util.log("hsi(%d) attempting to verify %s" % (h.pid(), self.path))
        rsp = h.hashverify(self.path)
            
        if "TIMEOUT" in rsp or "ERROR" in rsp:
            rval = "skipped"
            self.set('fails', self.fails + 1)
            util.log("hashverify transfer incomplete on %s" % self.path)
        elif "%s: (md5) OK" % self.path in rsp:
            rval = "matched"
            util.log("hashverify matched on %s" % self.path)
        elif "no valid checksum found" in rsp:
            if self.addable(self.cos):
                rval = self.add_to_sample(h)
            else:
                self.set('checksum', 0)
                rval = "skipped"
                util.log("hashverify skipped %s" % self.path)
        else:
            rval = Alert.Alert("Checksum mismatch: %s" % rsp)
            util.log("hashverify generated 'Checksum mismatch' " +
                     "alert on %s" % self.path)
        return rval
