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
import cv_lib
import dbschem
import Dimension
import glob
import hpss
import os
import pdb
import pexpect
import random
import re
import testhelp
import time
import util
import util as U


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
        # which tape cartridge(s) the file is stored on
        self.cart = None
        # the type of tape cartridge(s)
        self.ttypes = None
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
                         'cart',
                         'ttypes',
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
        for attr in ['checksum', 'fails', 'reported']:
            if getattr(self, attr) is None:
                setattr(self, attr, 0)

        # Set up dimensions based on configuration. If no dimensions option is
        # set in the configuration, we just leave the dimensions dict emtpy.
        # Since this class is only used by the cv_plugin, it makes no sense for
        # this code to be running if there is no cv section in the
        # configuration, so we'll let that exception get thrown up the stack.
        cfg = CrawlConfig.add_config()
        self.dim = {}
        try:
            dim_l = util.csv_list(cfg.get('cv', 'dimensions'))
            for dname in dim_l:
                self.dim[dname] = Dimension.get_dim(dname)
        except CrawlConfig.NoOptionError:
            pass

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
               "cart=%s, " % (self.cart if self.cart is None else "'%s'" %
                              self.cart) +
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
            CrawlConfig.log("starting hashcreate on %s", self.path)
            rsp = hsi.hashcreate(self.path)
            if "TIMEOUT" in rsp or "ERROR" in rsp:
                CrawlConfig.log("hashcreate transfer failed on %s", self.path)
                hsi.quit()
                self.set('fails', self.fails + 1)
                return "skipped"
            elif "Access denied" in rsp:
                CrawlConfig.log("hashcreate failed with 'access denied' on %s",
                                self.path)
                hsi.quit()
                return "access denied"
            else:
                CrawlConfig.log("completed hashcreate on %s", self.path)

        if self.checksum == 0:
            for dn in self.dim:
                cat = getattr(self, dn)
                self.dim[dn].addone(cat)
            self.set('checksum', 1)
            return "checksummed"

    # -------------------------------------------------------------------------
    def addable(self):
        """
        Determine which Dimensions want this item added. Note that we want this
        routine to be general across dimensions so we don't want it to assume
        anything about the dimension it's checking (like that it's named 'cos'
        for example). That why calls to this pass in cos rather than looking at
        the value in the object.
        """
        for dn in self.dim:
            cval = getattr(self, dn)
            if self.dim[dn].vote(cval) is False:
                CrawlConfig.log("%s votes against %s -- skipping" %
                                (dn, self.path))
                return False
        randval = random.random()
        if self.probability < randval:
            CrawlConfig.log("random votes against %s -- skipping (%g < %g)" %
                            (self.path, self.probability, randval))
            return False
        return True

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
        """
        # fire up hsi
        # self.probability = probability
        rval = []
        cfg = CrawlConfig.get_config()
        # hsi_timeout = int(cfg.get_d('crawler', 'hsi_timeout', 300))
        try:
            # h = hpss.HSI(timeout=hsi_timeout, verbose=True)
            h = hpss.HSI(verbose=True)
            CrawlConfig.log("started hsi with pid %d" % h.pid())
        except hpss.HSIerror as e:
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
            if self.cart is None:
                self.populate_cart(h)
            if self.checksum == 0:
                if self.has_hash(h):
                    self.add_to_sample(h, already_hashed=True)
                    rval = self.verify(h)
                    # returning "matched", "checksummed", "skipped", or Alert()
                elif self.addable():
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
        CrawlConfig.log("Persisting checkable '%s' with %s = %f, %s = %d" %
                        (self.path,
                         'last_check', self.last_check,
                         'fails', self.fails))
        self.persist()
        return rval

    # -------------------------------------------------------------------------
    @classmethod
    def ex_nihilo(cls, dataroot='/'):
        """
        Start from scratch. Create the database if necessary. Create the
        table(s) if necessary. Bootstrap the queue by adding the root
        director(ies).

        Field path is the location of the file or directory in the HPSS
        archive.

        Field type is 'f' for files or 'd' for directories.

        Field cos is the class of service for the file. For directories, cos is
        empty.

        Field cart starts with a null value. When populated from hsi, it may be
        set to the name of a tape cartridge or to ''. Empty files take up no
        space on any cartridge, so for them the field is empty.

        Field checksum is 0 if we have not computed or discoverd a checksum for
        the file. Once we know a checksum has been stored for the file, we set
        this to 1.

        Field last_check is the epoch time at which the file was last checked.

        Field fails is the number of times hashcreate and/or hashverify has
        failed on the file.

        Field reported is 0 or 1 indicating whether we've reported
        """
        dbschem.make_table("checkables")
        if type(dataroot) == str:
            dataroot = [dataroot]

        if type(dataroot) == list:
            for root in dataroot:
                r = Checkable(path=root, type='d', in_db=False, dirty=True)
                r.load()
                r.persist()

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

        If the file is empty, it looks like:

            FILE    /home/tpb/hic_test/empty     0       0            0
                                6056    0       1  07/10/2014       14:30:40
                07/10/2014      14:30:40

        The fields are separated by '\t', which is probably the easiest way to
        parse the line, especially when some values are missing.
        """
        try:
            q = cls.rgxl
        except AttributeError:
            cls.rgxl = re.compile("(.)([r-][w-][x-]){3}(\s+\S+){3}" +
                                  "(\s+\d+)(\s+\w{3}\s+\d+\s+[\d:]+)" +
                                  "\s+(\S+)")
            cls.map = {'DIRECTORY': 'd',
                       'd': 'd',
                       'FILE': 'f',
                       '-': 'f'}

        if any([value.startswith("FILE"),
                value.startswith("DIRECTORY")]):
            x = value.split('\t')
            ptype = cls.map[util.pop0(x)]
            pname = util.pop0(x).strip()
            util.pop0(x)
            util.pop0(x)
            util.pop0(x)
            cart = util.pop0(x)
            if cart is not None:
                cart = cart.strip()
            cos = util.pop0(x)
            if cos is not None:
                cos = cos.strip()
            else:
                cos = ''
            return Checkable(path=pname, type=ptype, cos=cos, cart=cart)
        else:
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
        db = CrawlDBI.DBI(dbtype='crawler')
        rv = db.select(table='checkables',
                       fields=['rowid',
                               'path',
                               'type',
                               'cos',
                               'cart',
                               'ttypes',
                               'checksum',
                               'last_check',
                               'fails',
                               'reported'
                               ],
                       where='path=?',
                       data=(self.path,))
        db.close()
        return rv

    # -------------------------------------------------------------------------
    @classmethod
    def get_list(cls, how_many=-1, prob=0.1, rootlist=[]):
        """
        Return the current list of Checkables from the database.
        """
        if how_many < 0:
            cfg = CrawlConfig.add_config()
            how_many = int(cfg.get_d('cv', 'operations', '30'))

        rval = Checkable.load_priority_list()
        if how_many <= len(rval):
            return rval

        rval.extend(Checkable.load_recheck_list(how_many))
        if how_many <= len(rval):
            return rval

        db = CrawlDBI.DBI(dbtype='crawler')
        kw = {'table': 'checkables',
              'fields': ['rowid',
                         'path',
                         'type',
                         'cos',
                         'cart',
                         'ttypes',
                         'checksum',
                         'last_check',
                         'fails',
                         'reported'],
              'orderby': 'last_check'}
        if 0 < how_many:
            kw['limit'] = how_many

        rows = db.select(**kw)

        # check whether any roots from rootlist are missing and if so, add them
        # to the table
        reselect = False
        pathlist = [x[1] for x in rows]
        for root in rootlist:
            if root not in pathlist:
                nr = Checkable(path=root, type='d')
                nr.load()
                nr.persist()
                reselect = True

        if reselect:
            rows = db.select(**kw)

        for row in rows:
            tmp = list(row)
            new = Checkable(rowid=tmp.pop(0),
                            path=tmp.pop(0),
                            type=tmp.pop(0),
                            cos=tmp.pop(0),
                            cart=tmp.pop(0),
                            ttypes=tmp.pop(0),
                            checksum=tmp.pop(0),
                            last_check=tmp.pop(0),
                            fails=tmp.pop(0),
                            reported=tmp.pop(0),
                            probability=prob,
                            in_db=True,
                            dirty=False)
            if new not in rval:
                rval.append(new)
            if how_many <= len(rval):
                break

        db.close()
        CrawlConfig.log("returning %d items" % len(rval))
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
        db = CrawlDBI.DBI(dbtype='crawler')
        if self.rowid is not None:
            rows = db.select(table='checkables',
                             fields=['rowid',
                                     'path',
                                     'type',
                                     'cos',
                                     'cart',
                                     'ttypes',
                                     'checksum',
                                     'last_check',
                                     'fails',
                                     'reported'],
                             where="rowid = ?",
                             data=(self.rowid,))
        else:
            rows = db.select(table='checkables',
                             fields=['rowid',
                                     'path',
                                     'type',
                                     'cos',
                                     'cart',
                                     'ttypes',
                                     'checksum',
                                     'last_check',
                                     'fails',
                                     'reported'],
                             where="path = ?",
                             data=(self.path,))
        if 0 == len(rows):
            self.in_db = False
        elif 1 == len(rows):
            self.in_db = True
            rz = list(rows[0])
            self.rowid = rz.pop(0)
            self.path = rz.pop(0)
            self.type = rz.pop(0)
            self.cos = rz.pop(0)
            self.cart = rz.pop(0)
            self.ttypes = rz.pop(0)
            self.checksum = rz.pop(0)
            self.last_check = rz.pop(0)
            try:
                self.fails = rz.pop(0)
            except IndexError:
                self.fails = 0
            try:
                self.reported = rz.pop(0)
            except IndexError:
                self.reported = 0
            self.dirty = False
        else:
            raise StandardError("There appears to be more than one copy " +
                                "of %s in the database" % self)

        db.close()

    # -------------------------------------------------------------------------
    @classmethod
    def load_recheck_list(cls, how_many):
        """
        Look to see whether any of the already checksummed items in the
        database have a last check time over the threshold for rechecking. If
        so, we'll shove some of them to the front of the list based on the
        configuration.
        """
        cfg = CrawlConfig.add_config()
        r_fraction = float(cfg.get_d('cv', 'recheck_fraction', '0.0'))
        r_age = cfg.get_time('cv', 'recheck_age', 365*24*3600)
        threshold = int(time.time() - r_age)
        CrawlConfig.log("threshold = %s (%d)", U.ymdhms(threshold), threshold)
        if r_fraction == 0.0:
            return []

        limit = round(r_fraction * how_many)

        db = CrawlDBI.DBI(dbtype='crawler')
        kw = {'table': 'checkables',
              'fields': ['rowid',
                         'path',
                         'type',
                         'cos',
                         'cart',
                         'ttypes',
                         'checksum',
                         'last_check',
                         'fails',
                         'reported'],
              'where': 'checksum <> 0 and last_check < %d' % threshold,
              'orderby': 'last_check',
              'limit': limit}

        rows = db.select(**kw)
        db.close()

        rval = []
        for row in rows:
            tmp = list(row)
            new = Checkable(rowid=tmp.pop(0),
                            path=tmp.pop(0),
                            type=tmp.pop(0),
                            cos=tmp.pop(0),
                            cart=tmp.pop(0),
                            ttypes=tmp.pop(0),
                            checksum=tmp.pop(0),
                            last_check=tmp.pop(0),
                            fails=tmp.pop(0),
                            reported=tmp.pop(0),
                            in_db=True,
                            dirty=False)
            rval.append(new)
        return rval

    # -------------------------------------------------------------------------
    @classmethod
    def load_priority_list(cls):
        """
        If one or more priority list files are configured, read them and put
        their contents first in the list of Checkables to be processed
        """
        rval = []
        cfg = CrawlConfig.get_config()
        priglob = cfg.get_d('cv', 'priority', '')
        if priglob == '':
            return rval

        pricomp = cfg.get_d('cv',
                            'completed',
                            U.pathjoin(U.dirname(priglob), 'completed'))

        for pripath in U.foldsort(glob.glob(priglob)):
            with open(pripath, 'r') as f:
                for line in f.readlines():
                    path = line.strip()
                    rval.append(Checkable(path=path, type='f'))
            os.rename(pripath, U.pathjoin(pricomp, U.basename(pripath)))

        return rval

    # -------------------------------------------------------------------------
    def persist(self, dirty=False):
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
        if self.rowid is None and self.last_check != 0.0:
            raise StandardError("%s has rowid == None, last_check != 0.0" %
                                self)
        if self.path == '':
            raise StandardError("%s has an empty path" % self)
        if self.type == 'd' and self.cos != '':
            raise StandardError("%s has type 'd', non-empty cos" % self)
        if self.type != 'f' and self.type != 'd':
            raise StandardError("%s has invalid type" % self)

        db = CrawlDBI.DBI(dbtype='crawler')

        # Populate ttypes if appropriate
        if self.type == 'f' and self.ttypes is None and self.cart is not None:
            media = cv_lib.ttype_lookup(self.path, self.cart)
            if media is not None:
                self.ttypes = ','.join([x[1] for x in media])

        if not self.in_db:
            # insert it
            db.insert(table='checkables',
                      fields=['path',
                              'type',
                              'cos',
                              'cart',
                              'ttypes',
                              'checksum',
                              'last_check',
                              'fails',
                              'reported'],
                      data=[(self.path,
                             self.type,
                             self.cos,
                             self.cart,
                             self.ttypes,
                             self.checksum,
                             self.last_check,
                             self.fails,
                             self.reported)])
            self.in_db = True
        elif self.dirty or dirty:
            # update it
            if self.rowid is not None:
                db.update(table='checkables',
                          fields=['path',
                                  'type',
                                  'cos',
                                  'cart',
                                  'ttypes',
                                  'checksum',
                                  'last_check',
                                  'fails',
                                  'reported'],
                          where="rowid = ?",
                          data=[(self.path,
                                 self.type,
                                 self.cos,
                                 self.cart,
                                 self.ttypes,
                                 self.checksum,
                                 self.last_check,
                                 self.fails,
                                 self.reported,
                                 self.rowid)])
            else:
                db.update(table='checkables',
                          fields=['type',
                                  'cos',
                                  'cart',
                                  'ttypes',
                                  'checksum',
                                  'last_check',
                                  'fails',
                                  'reported'],
                          where="path = ?",
                          data=[(self.type,
                                 self.cos,
                                 self.cart,
                                 self.ttypes,
                                 self.checksum,
                                 self.last_check,
                                 self.fails,
                                 self.reported,
                                 self.path)])
            self.dirty = False

        for d in self.dim:
            self.dim[d].load()
        db.close()

    # -------------------------------------------------------------------------
    def populate_cart(self, h):
        rsp = h.lsP(self.path)
        tmp = Checkable.fdparse(rsp.split("\n")[1])
        try:
            self.cart = tmp.cart
        except AttributeError:
            self.cart = ''
            CrawlConfig.log("%s <- Checkable.fdparse('%s')" %
                            (tmp, rsp.split("\n")[1]))

    # -------------------------------------------------------------------------
    def set(self, attrname, value):
        setattr(self, attrname, value)
        self.dirty = True

    # -------------------------------------------------------------------------
    def verify(self, h):
        """
        Attempt to verify the current file.
        """
        CrawlConfig.log("hsi(%d) attempting to verify %s" % (h.pid(),
                                                             self.path))
        rsp = h.hashverify(self.path)

        if "TIMEOUT" in rsp or "ERROR" in rsp:
            rval = "skipped"
            self.set('fails', self.fails + 1)
            CrawlConfig.log("hashverify transfer incomplete on %s -- skipping"
                            % self.path)
            h.quit()
        elif "%s: (md5) OK" % self.path in rsp:
            rval = "matched"
            CrawlConfig.log("hashverify matched on %s" % self.path)
        elif "no valid checksum found" in rsp:
            if self.addable(self.cos):
                rval = self.add_to_sample(h)
            else:
                self.set('checksum', 0)
                rval = "skipped"
                CrawlConfig.log("hashverify skipped %s" % self.path)
        else:
            rval = Alert.Alert("Checksum mismatch: %s" % rsp)
            CrawlConfig.log("hashverify generated 'Checksum mismatch' " +
                            "alert on %s" % self.path)
        return rval
