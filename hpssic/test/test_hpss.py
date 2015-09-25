"""
Tests for hpss.py
"""
import copy
from hpssic import CrawlConfig
import distutils
from hpssic import hpss
from hpssic import messages as MSG
import os
import pdb
import pytest
import re
import sys
from hpssic import testhelp
import time
import traceback as tb
from hpssic import util


# -----------------------------------------------------------------------------
class hpssBaseTest(testhelp.HelpedTestCase):
    """
    Common stuff for hpss.HSI tests
    """
    hdir = "/home/tpb/hic_test"
    stem = "hashable"
    plist = ["%s/%s%d" % (hdir, stem, x) for x in range(1, 4)]
    paths = " ".join(plist)
    cfg_d = {'crawler': {'plugins': 'cv'},
             'cv':      {'fire': 'no',
                         'reset_atime': 'yes',
                         'hash_algorithm': 'md5'},
             }


# -----------------------------------------------------------------------------
def test_hsi_location():
    """
    Get the directories for 'crawl' and 'hsi'. They should match
    """
    cloc = distutils.spawn.find_executable('crawl')
    assert cloc is not None, "crawl not found"
    c = util.dirname(cloc)
    hloc = distutils.spawn.find_executable('hsi')
    assert hloc is not None, "hsi not found"
    h = util.dirname(hloc)
    assert c == h, "location of hsi does not match location of crawl"


# -----------------------------------------------------------------------------
@pytest.mark.jenkins_fail
@pytest.mark.slow
class hpssCtorTest(hpssBaseTest):
    """
    Tests specifically for the constructor of the hpss.HSI class
    """
    # -------------------------------------------------------------------------
    def test_ctor_attrs(self):
        """
        Verify that a newly created HSI object has the right attributes
        """
        a = hpss.HSI(connect=False)

        for attr in ['prompt', 'verbose', 'xobj', 'lscos', 'quit', 'connect',
                     'chdir', 'lsP', 'hashcreate', 'hashlist', 'hashverify',
                     'hashdelete']:
            self.assertTrue(hasattr(a, attr),
                            "Expected %s to have attribute '%s'" %
                            (a, attr))

    # -------------------------------------------------------------------------
    def test_ctor_reset_atime_default(self):
        """
        If reset_atime is not specified in the config or argument list, it
        should default to False
        """
        cf_name = self.tmpdir(util.my_name() + ".cfg")

        # write out a config file with no reset_atime spec
        cd = copy.deepcopy(self.cfg_d)
        del cd['cv']['reset_atime']
        self.write_cfg_file(cf_name, cd)

        # make the test config the default
        CrawlConfig.get_config(cfname=cf_name, reset=True)

        # get an hpss.HSI object and check its reset_atime attribute
        h = hpss.HSI(connect=False)
        self.expected(False, h.reset_atime)

        CrawlConfig.get_config(reset=True, soft=True)

    # -------------------------------------------------------------------------
    def test_ctor_reset_atime_cfg_true(self):
        """
        If reset_atime is specified in the config as True, it should be True
        """
        cf_name = self.tmpdir(util.my_name() + ".cfg")

        # write out a config file with no reset_atime spec
        self.write_cfg_file(cf_name, self.cfg_d)

        # make the test config the default
        CrawlConfig.get_config(cfname=cf_name, reset=True)

        # get an hpss.HSI object and check its reset_atime attribute
        h = hpss.HSI(connect=False)
        self.expected(True, h.reset_atime)

        CrawlConfig.get_config(reset=True, soft=True)

    # -------------------------------------------------------------------------
    def test_ctor_reset_atime_cfg_false(self):
        """
        If reset_atime is specified in the config as False, it should be False
        """
        cf_name = self.tmpdir(util.my_name() + ".cfg")

        # write out a config file with no reset_atime spec
        cfg = copy.deepcopy(self.cfg_d)
        cfg['cv']['reset_atime'] = 'no'
        self.write_cfg_file(cf_name, cfg)

        # make the test config the default
        CrawlConfig.get_config(cfname=cf_name, reset=True)

        # get an hpss.HSI object and check its reset_atime attribute
        h = hpss.HSI(connect=False)
        self.expected(False, h.reset_atime)

        CrawlConfig.get_config(reset=True, soft=True)

    # -------------------------------------------------------------------------
    def test_ctor_reset_atime_call_true(self):
        """
        If reset_atime is specified in the call as True, it should be True,
        even if it's specified as False in the config
        """
        cf_name = self.tmpdir(util.my_name() + ".cfg")

        # write out a config file with no reset_atime spec
        cfg = copy.deepcopy(self.cfg_d)
        cfg['cv']['reset_atime'] = 'no'
        self.write_cfg_file(cf_name, cfg)

        # make the test config the default
        CrawlConfig.get_config(cfname=cf_name, reset=True)

        # get an hpss.HSI object and check its reset_atime attribute
        h = hpss.HSI(connect=False, reset_atime=True)
        self.expected(True, h.reset_atime)

        CrawlConfig.get_config(reset=True, soft=True)

    # -------------------------------------------------------------------------
    def test_ctor_reset_atime_call_false(self):
        """
        If reset_atime is specified in the call as False, it should be False,
        even if the config has it as True
        """
        cf_name = self.tmpdir(util.my_name() + ".cfg")

        # write out a config file with no reset_atime spec
        self.write_cfg_file(cf_name, self.cfg_d)

        # make the test config the default
        CrawlConfig.get_config(cfname=cf_name, reset=True)

        # get an hpss.HSI object and check its reset_atime attribute
        h = hpss.HSI(connect=False, reset_atime=False)
        self.expected(False, h.reset_atime)

        CrawlConfig.get_config(reset=True, soft=True)

    # -------------------------------------------------------------------------
    def test_ctor_no_cv_section(self):
        """
        If there is no cv section in the config, reset_atime and hash_algorithm
        should take on their default values.
        """
        self.dbgfunc()
        cfg = copy.deepcopy(self.cfg_d)
        del cfg['cv']
        zcfg = CrawlConfig.add_config(close=True, dct=cfg)
        self.assertFalse(zcfg.has_section('cv'))
        h = hpss.HSI(connect=False)
        self.expected(False, h.reset_atime)
        self.expected(None, h.hash_algorithm)


# -----------------------------------------------------------------------------
@pytest.mark.jenkins_fail
@pytest.mark.slow
class hpssTest(hpssBaseTest):
    """
    Tests for the hpss.HSI class
    """
    # -------------------------------------------------------------------------
    def test_chdir_noarg(self):
        """
        Change dir in HPSS to a non directory
        """
        try:
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            self.assertRaisesMsg(TypeError,
                                 "chdir() takes exactly 2 arguments",
                                 h.chdir)
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_chdir_notdir(self):
        """
        Change dir in HPSS to a non directory
        """
        try:
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            result = h.chdir("hic_test/crawler.tar")
            h.quit()

            exp = "Not a directory"
            self.assertTrue(exp in result,
                            "Expected '%s' in %s" %
                            (exp, util.line_quote(result)))
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_chdir_ok(self):
        """
        Successful change dir in HPSS
        """
        self.dbgfunc()
        try:
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            result = h.chdir("hic_test")
            h.quit()

            exp = "/home/tpb/hic_test"
            self.expected_in(exp, result)
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_chdir_perm(self):
        """
        Change dir in HPSS to a non-accessible directory
        """
        try:
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            result = h.chdir("cli_test/unreadable")
            h.quit()

            exp = "hpss_Chdir: Access denied"
            self.assertTrue(exp in result,
                            "Expected '%s' in %s" %
                            (exp, util.line_quote(result)))
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_chdir_unicode(self):
        """
        Unicode argument to chdir should work
        """
        try:
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            ucdir = unicode("hic_test")
            result = h.chdir(ucdir)
            exp = "/home/tpb/hic_test"
            self.expected_in(exp, result)
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashcreate_argbad(self):
        """
        If hashcreate gets an invalid argument (not a str or list), it should
        throw an exception
        """
        try:
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            self.assertRaisesMsg(hpss.HSIerror,
                                 "hashcreate: Invalid argument",
                                 h.hashcreate,
                                 32)
            h.quit()
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashcreate_atime_reset(self):
        """
        1) use file /home/tpb/hic_test/hashable1
        2) if it has a hash, delete it
        3) Set the access time into the past (touch -a t yyyymmddHHMM.SS ...)
        4) hashcreate with atime reset turned on (set when opening HSI)
        5) Get the access time -- it should be the same as was set in step 2
        """
        try:
            filename = self.plist[0]
            h = hpss.HSI(reset_atime=True)

            # delete the file's hash if it has one
            hash = h.hashlist(filename)
            if "(none)" not in hash:
                h.hashdelete(filename)

            # set the atime into the past
            past = util.epoch("2001.0203 04:05:06")
            h.touch(filename, when=past)

            # create a hash
            h.hashcreate(filename)

            # check the atime -- it should be way in the past
            atime = h.access_time(filename)
            self.expected(util.ymdhms(past), util.ymdhms(atime))

        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashcreate_atime_no_reset(self):
        """
        1) use file /home/tpb/hic_test/hashable1
        2) delete the file's hash if it has one
        3) Set the access time into the past (touch -a t yyyymmddHHMM.SS ...)
        4) hashcreate with atime reset turned OFF
        5) Get the access time -- it should be near the present
        """
        try:
            filename = self.plist[0]
            h = hpss.HSI(reset_atime=False)

            # delete the file's hash if it has one
            hash = h.hashlist(filename)
            if "(none)" not in hash:
                h.hashdelete(filename)

            # set the file's atime into the past
            past = util.epoch("2001.0203 04:05:06")
            h.touch(filename, when=past)

            # give the file a hash
            h.hashcreate(filename)

            # check the atime -- it should be recent
            atime = h.access_time(filename)
            delta = time.time() - atime
            self.assertTrue(delta < 10,
                            "Expected a recent time, got '%s'" %
                            util.ymdhms(atime))
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashverify_atime_reset(self):
        """
        1) Use file /home/tpb/hic_test/hashable1
        2) hashcreate on it
        3) Set the access time into the past (touch -a t yyyymmddHHMM.SS ...)
        4) hashverify with atime reset turned on
        5) Get the access time -- it should be the same as was set in step 3
        """
        try:
            filename = self.plist[0]
            h = hpss.HSI(reset_atime=True)

            # give it a hash
            h.hashcreate(filename)

            # set the access time into the past
            past = util.epoch("2001.0203 04:05:06")
            h.touch(filename, when=past)

            # hashverify
            h.hashverify(filename)

            # check the atime -- it should be old
            atime = h.access_time(filename)
            self.expected(util.ymdhms(past), util.ymdhms(atime))
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashverify_atime_no_reset(self):
        """
        1) Create a file
        2) hashcreate on it
        3) Set the access time into the past (touch -a t yyyymmddHHMM.SS ...)
        4) hashverify with atime reset turned OFF
        5) Get the access time -- it should be near the present
        6) remove the file
        """
        try:
            filename = self.plist[0]
            h = hpss.HSI(reset_atime=False)
            h.hashcreate(filename)

            # set the access time into the past
            past = util.epoch("2001.0203 04:05:06")
            h.touch(filename, when=past)

            # hashverify
            h.hashverify(filename)

            # check the atime -- it should be recent
            atime = h.access_time(filename)
            delta = time.time() - atime
            self.assertTrue(delta < 60,
                            "Expected a recent time, got '%s'" %
                            util.ymdhms(atime))
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashcreate_argnone(self):
        """
        If hashcreate gets no argument, it should throw an exception
        """
        try:
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            self.assertRaisesMsg(TypeError,
                                 "hashcreate() takes exactly 2 arguments",
                                 h.hashcreate)
            h.quit()
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashcreate_ok_glob(self):
        """
        Issue "hashcreate" in hsi with a wildcard argument, return results
        """
        glop = "%s/%s*" % (self.hdir, self.stem)

        try:
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            result = h.hashcreate(glop)
            h.quit()
            self.expected_in("hashcreate", result)
            for path in self.paths.split():
                exp = "\(?md5\)? %s" % path
                self.expected_in(exp, result)
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashcreate_ok_list(self):
        """
        Issue "hashcreate" in hsi with a list argument, return results
        """
        try:
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            result = h.hashcreate(self.plist)
            h.quit()
            self.expected_in("hashcreate", result)
            for path in self.plist:
                exp = "\(?md5\)? %s" % path
                self.expected_in(exp, result)
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashcreate_ok_str(self):
        """
        Issue "hashcreate" in hsi with a string argument, return results
        """
        try:
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            result = h.hashcreate(self.paths)
            h.quit()
            self.expected_in("hashcreate", result)
            for path in self.paths.split():
                exp = "\(?md5\)? %s" % path
                self.expected_in(exp, result)
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashdelete_argbad(self):
        """
        If hashdelete gets an invalid argument (not a str or list), it should
        throw an exception
        """
        try:
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            self.assertRaisesMsg(hpss.HSIerror,
                                 "hashdelete: Invalid argument",
                                 h.hashdelete,
                                 32)
            h.quit()
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashdelete_argnone(self):
        """
        If hashdelete gets no argument, it should throw an exception
        """
        try:
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            self.assertRaisesMsg(TypeError,
                                 "hashdelete() takes exactly 2 arguments" +
                                 " (1 given)",
                                 h.hashdelete)
            h.quit()
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashdelete_ok_glob(self):
        """
        If hashdelete gets a wildcard argument, it should work
        """
        try:
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            # make sure the hashables all have a checksum stored
            x = h.hashlist(self.plist)
            for path in self.plist:
                if util.rgxin("\(?none\)?  %s" % path, x):
                    h.hashcreate(path)

            # run hashdelete on the glob path
            result = h.hashdelete("%s/hash*" % self.hdir)
            h.quit()

            # verify the results
            self.expected_in("hashdelete", result)
            for path in self.plist:
                self.expected_in("hash deleted: \(?md5\)? %s" % path, result)

            exp = "\(?none\)?  %s/hashnot" % self.hdir
            self.assertTrue(exp not in result,
                            "'%s' not expected in %s" %
                            (exp,
                             util.line_quote(result)))
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashdelete_ok_list(self):
        """
        If hashdelete get a list argument, it should work
        """
        try:
            plist = self.plist + [self.hdir + "/hashnot"]
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            # make sure the hashables all have a checksum stored
            x = h.hashlist(self.plist)
            for path in self.plist:
                if util.rgxin("\(?none\)?  %s" % path, x):
                    h.hashcreate(path)

            # run hashdelete on the list
            result = h.hashdelete(plist)
            h.quit()

            # verify the results
            self.expected_in("hashdelete", result)
            for path in self.plist:
                self.expected_in("hash deleted: md5 %s" % path, result)
            exp = "\(?none\)?  %s/hashnot" % self.hdir
            self.assertTrue(exp not in result,
                            "'%s' not expected in %s" %
                            (exp,
                             util.line_quote(result)))
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashdelete_ok_str(self):
        """
        If hashdelete gets a string argument, it should work
        """
        try:
            paths = self.paths + " %s/hashnot" % self.hdir
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            # make sure the hashables all have a checksum stored
            x = h.hashlist(self.plist)
            for path in self.plist:
                if util.rgxin("\(?none\)?  %s" % path, x):
                    h.hashcreate(path)

            # run hashdelete on the string
            result = h.hashdelete(paths)
            h.quit()

            # verify the results
            self.expected_in("hashdelete", result)
            for path in self.paths.split():
                exp = "hash deleted: \(?md5\)? %s" % path
                self.expected_in(exp, result)
            exp = "hash deleted: \(?md5\)? %s/hashnot" % self.hdir
            self.assertFalse(util.rgxin(exp, result),
                             "'%s' not expected in %s" %
                             (exp, util.line_quote(result)))
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashlist_argbad(self):
        """
        If hashlist gets an invalid argument (not a str or list), it should
        throw an exception
        """
        try:
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            self.assertRaisesMsg(hpss.HSIerror,
                                 "hashlist: Invalid argument",
                                 h.hashlist,
                                 32)
            h.quit()
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashlist_argnone(self):
        """
        If hashlist gets no argument, it should throw an exception
        """
        try:
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            self.assertRaisesMsg(TypeError,
                                 "hashlist() takes exactly 2 arguments",
                                 h.hashlist)
            h.quit()
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashlist_ok_glob(self):
        """
        If hashlist gets a wildcard argument, it should work
        """
        try:
            vbval = ("verbose" in testhelp.testargs())
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            # make sure the hashables all have a checksum stored
            x = h.hashlist(self.plist)
            for path in self.plist:
                if util.rgxin("\(?none\)?  %s" % path, x):
                    h.hashcreate(path)

            # run the test payload
            result = h.hashlist("%s/hash*" % self.hdir)
            h.quit()
            self.expected_in("hashlist", result)
            for path in self.plist:
                exp = "\(?md5\)? %s" % path
                self.expected_in(exp, result)
            exp = "\(?none\)?  %s/hashnot" % self.hdir
            self.expected_in(exp, result)
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashlist_ok_list(self):
        """
        If hashlist get a list argument, it should work
        """
        try:
            plist = self.plist + [self.hdir + "/hashnot"]
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            # make sure the hashables all have a checksum stored
            x = h.hashlist(self.plist)
            for path in self.plist:
                if util.rgxin("\(?none\)?  %s" % path, x):
                    h.hashcreate(path)

            # run the test payload
            result = h.hashlist(plist)
            h.quit()
            self.expected_in("hashlist", result)
            for path in self.plist:
                exp = "\(?md5\)? %s" % path
                self.expected_in(exp, result)
            exp = "\(?none\)?  %s/hashnot" % self.hdir
            self.expected_in(exp, result)
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashlist_ok_str(self):
        """
        If hashlist gets a string argument, it should work
        """
        try:
            paths = self.paths + " %s/hashnot" % self.hdir
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            # make sure the hashables all have a checksum stored
            x = h.hashlist(self.plist)
            for path in self.plist:
                if util.rgxin("\(?none\)?  %s" % path, x):
                    h.hashcreate(path)

            # run the test payload
            result = h.hashlist(paths)
            h.quit()
            self.expected_in("hashlist", result)
            for path in self.paths.split():
                exp = "\(?md5\)? %s" % path
                self.expected_in(exp, result)
            exp = "\(?none\)?  %s/hashnot" % self.hdir
            self.expected_in(exp, result)
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashverify_argbad(self):
        """
        If hashverify gets an invalid argument (not a str or list), it should
        throw an exception
        """
        try:
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            self.assertRaisesMsg(hpss.HSIerror,
                                 "hashverify: Invalid argument",
                                 h.hashverify,
                                 32)
            h.quit()
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashverify_argnone(self):
        """
        Issue "hashverify" in hsi, return results
        """
        try:
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            self.assertRaisesMsg(TypeError,
                                 "hashverify() takes exactly 2 arguments",
                                 h.hashverify)
            h.quit()
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashverify_ok_glob(self):
        """
        Issue "hashverify" in hsi on a wildcard, return results
        """
        self.dbgfunc()
        try:
            glop = "/home/tpb/hic_test/hash*"

            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            # make sure the hashables all have a checksum stored
            x = h.hashlist(self.plist)
            for path in self.plist:
                if "\(?none\)?  %s" % path in x:
                    h.hashcreate(path)

            # run hashverify on the glob path
            result = h.hashverify(glop)
            h.quit()

            # verify the results: we should see "OK" for the hashables and an
            # error for hashnot
            self.expected_in("hashverify", result)
            for path in self.plist:
                exp = "%s: \(?md5\)? OK" % path
                self.expected_in(exp, result)
            exp = "hashnot failed: no valid checksum found"
            self.expected_in(exp, result)
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashverify_ok_list(self):
        """
        Issue "hashverify" in hsi, return results
        """
        self.dbgfunc()
        try:
            plist = self.plist + ["%s/hashnot" % self.hdir]

            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))

            # make sure the hashables all have a checksum stored
            x = h.hashlist(self.plist)
            for path in self.plist:
                if "\(?none\)?  %s" % path in x:
                    h.hashcreate(path)

            # run hashverify on the list
            result = h.hashverify(plist)
            h.quit()

            # verify the results: we should see "OK" for the hashables and an
            # error for hashnot
            self.expected_in("hashverify", result)
            for path in self.plist:
                self.expected_in("%s: \(?md5\)? OK" % path, result)
            self.expected_in("hashnot failed: no valid checksum found",
                             result)
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashverify_ok_str(self):
        """
        Issue "hashverify" in hsi, return results
        """
        self.dbgfunc()
        try:
            paths = self.paths + " %s/hashnot" % self.hdir
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            result = h.hashverify(paths)
            h.quit()
            self.expected_in("hashverify", result)
            for path in self.plist:
                self.expected_in("%s: \(?md5\)? OK" % path, result)
            self.expected_in("hashnot failed: no valid checksum found",
                             result)
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_hashverify_ok_unicode(self):
        """
        Issue "hashverify" in hsi with a unicode arg, return results
        """
        self.dbgfunc()
        try:
            paths = unicode(self.paths + " %s/hashnot" % self.hdir)
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            result = h.hashverify(paths)
            h.quit()
            self.expected_in("hashverify", result)
            for path in self.plist:
                self.expected_in("%s: \(?md5\)? OK" % path, result)
            self.expected_in("hashnot failed: no valid checksum found",
                             result)
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_lscos(self):
        """
        Issue "lscos", check result
        """
        try:
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            result = h.lscos()
            h.quit()
            self.expected_in("3003 Disk Big Backups", result)
            self.expected_in("5081 Disk X-Small", result)
            self.expected_in("6001 Disk Small", result)
            self.expected_in("6054 Disk Large_T", result)
            self.expected_in("6057 Disk X-Large_T 2-Copy", result)
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_lsP_argbad(self):
        """
        Issue "ls -P" with non-string, non-list arg, expect exception
        """
        try:
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            self.assertRaisesMsg(hpss.HSIerror,
                                 "lsP: Invalid argument",
                                 h.lsP,
                                 19)
            h.quit()
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_lsP_argnone(self):
        """
        Issue "ls -P" in /home/tpb/hic_test with no arg, validate result
        """
        try:
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            h.chdir("/home/tpb/hic_test")
            result = h.lsP()
            for path in self.plist:
                self.expected_in("FILE\s+%s" % util.basename(path), result)
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_lsP_glob(self):
        """
        Issue "ls -P hash*" in hic_test, validate results
        """
        try:
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            h.chdir("/home/tpb/hic_test")
            result = h.lsP("hash*")
            for path in self.plist:
                self.expected_in("FILE\s+%s" % util.basename(path), result)
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_lsP_list(self):
        """
        Issue "ls -P [foo, bar]" in hic_test, validate results
        """
        try:
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            result = h.lsP(self.plist)
            for path in self.plist:
                self.expected_in("FILE\s+%s" % util.basename(path), result)
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_lsP_str(self):
        """
        Issue "ls -P foo bar" in hic_test, validate results
        """
        try:
            h = hpss.HSI(verbose=("verbose" in testhelp.testargs()))
            result = h.lsP(self.paths)
            for path in self.plist:
                self.expected_in("FILE\s+%s" % util.basename(path), result)
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_touch_ls_access(self):
        """
        Test hpss.touch() and hpss.ls_access(). Use dates 120 days ahead and
        120 days behind and one close to the current date to ensure we look at
        dates both inside and outside daylight saving time.
        """
        def one_round(h, filename, when):
            h.touch(filename, when)
            atime = h.access_time(filename)
            self.expected(when, atime)

        try:
            filename = self.plist[0]
            h = hpss.HSI(reset_atime=True)
            now = int(time.time())

            one_round(h, filename, now + (120*24*3600))
            one_round(h, filename, now + (2*24*3600))
            one_round(h, filename, now - (120*24*3600))

            h.quit()
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

    # -------------------------------------------------------------------------
    def test_unavailable(self):
        """
        If HPSS is down, the HSI constructor should throw an exception. And in
        this case, we don't need to call h.quit() since the connection never
        got completed.
        """
        h = hpss.HSI(connect=False, unavailable=True)
        self.assertRaisesMsg(hpss.HSIerror,
                             MSG.hpss_unavailable,
                             h.connect)


# -----------------------------------------------------------------------------
@pytest.mark.jenkins_fail
@pytest.mark.slow
class hpssHashAlgTest(hpssBaseTest):
    """
    Tests for the hash_algorithm config item in hpss.HSI class
    """
    # -------------------------------------------------------------------------
    def test_hashalg_default(self):
        """
        With no hash_algorithm in config, the default should be 'md5'
        """
        self.check_hash_algorithm(util.my_name(), '(none)', 'md5')

    # -------------------------------------------------------------------------
    def test_hashalg_md5(self):
        """
        With hash_algorithm = md5 in config
        """
        self.check_hash_algorithm(util.my_name(), 'md5')

    # -------------------------------------------------------------------------
    def test_hashalg_sha1(self):
        """
        With hash_algorithm = sha1 in config
        """
        self.check_hash_algorithm(util.my_name(), 'sha1')

    # -------------------------------------------------------------------------
    def test_hashalg_sha224(self):
        """
        With hash_algorithm = sha224 in config
        """
        self.check_hash_algorithm(util.my_name(), 'sha224')

    # -------------------------------------------------------------------------
    def test_hashalg_sha256(self):
        """
        With hash_algorithm = sha1 in config
        """
        self.check_hash_algorithm(util.my_name(), 'sha256')

    # -------------------------------------------------------------------------
    def test_hashalg_sha384(self):
        """
        With hash_algorithm = sha1 in config
        """
        self.check_hash_algorithm(util.my_name(), 'sha384')

    # -------------------------------------------------------------------------
    def test_hashalg_sha512(self):
        """
        With hash_algorithm = sha1 in config
        """
        self.dbgfunc()
        self.check_hash_algorithm(util.my_name(), 'sha512')

    # -------------------------------------------------------------------------
    def test_hashalg_crc32(self):
        """
        With hash_algorithm = sha1 in config
        """
        self.check_hash_algorithm(util.my_name(), 'crc32')

    # -------------------------------------------------------------------------
    def test_hashalg_adler32(self):
        """
        With hash_algorithm = sha1 in config
        """
        self.check_hash_algorithm(util.my_name(), 'adler32')

    # -------------------------------------------------------------------------
    def check_hash_algorithm(self, cf_stem, alg, checkfor=None):
        """
        With hash_algorithm = *alg* in config
        """
        if checkfor is None:
            checkfor = alg

        # generate a config file and make it the default config
        cf_name = self.tmpdir(cf_stem + ".cfg")
        cd = copy.deepcopy(self.cfg_d)
        if alg == '(none)':
            del cd['cv']['hash_algorithm']
        else:
            cd['cv']['hash_algorithm'] = alg
        self.write_cfg_file(cf_name, cd)
        CrawlConfig.get_config(cfname=cf_name, reset=True)

        # Get an hsi object
        testfile = self.plist[1]
        try:
            h = hpss.HSI()
        except hpss.HSIerror as e:
            if MSG.hpss_unavailable in str(e):
                pytest.skip(str(e))

        # if necessary, delete any hash on the test file
        result = h.hashlist(testfile)
        if "(none)" not in result:
            h.hashdelete(testfile)

        # generate a hash on the test file
        h.hashcreate(testfile)

        # verify that the hash created is of the proper type
        result = h.hashlist(testfile)
        self.expected_in(checkfor, result)
