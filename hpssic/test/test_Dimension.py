#!/usr/bin/env python
"""
Track stratum proportions in a sample against a population
"""
import copy
from hpssic import Checkable
from hpssic import CrawlConfig
from hpssic import CrawlDBI
from hpssic.Dimension import Dimension
import os
import pdb
import sys
from hpssic import testhelp
from hpssic import toolframe
import traceback as tb
from hpssic import util as U

mself = sys.modules[__name__]
logfile = "%s/crawl_test.log" % os.path.dirname(mself.__file__)


M = sys.modules['__main__']
if 'py.test' in M.__file__:
    import pytest
    attr = pytest.mark.attr
else:
    from nose.plugins.attrib import attr


# -----------------------------------------------------------------------------
class DimensionTest(testhelp.HelpedTestCase):
    """
    Tests for the Dimension class
    """
    # -------------------------------------------------------------------------
    def cfg_dict(self, tname='test_Dimension'):
        """
        Return the basic config for these tests
        """
        cfg_d = {'dbi-crawler': {'dbtype': 'sqlite',
                                 'dbname': self.tmpdir('test.db'),
                                 'tbl_prefix': 'test'},
                 'crawler': {'logpath': self.tmpdir('%s.log' % (tname))},
                 'cv': {'fire': 'no'}
                 }
        return cfg_d

    # -------------------------------------------------------------------------
    def test_addone(self):
        """
        Given a set of values in a Dimension object, verify that addone does
        the right thing.
        """
        self.dbgfunc()
        a = Dimension(name='addone', load=False)
        a.p_sum = {'6001': {'count': 10, 'pct': 50.0},
                   '5081': {'count': 10, 'pct': 50.0}
                   }
        a.s_sum = {'6001': {'count': 2, 'pct': 40.0},
                   '5081': {'count': 3, 'pct': 60.0}
                   }
        a.addone('6001')
        self.expected(3, a.s_sum['6001']['count'])
        self.expected(50.0, a.s_sum['6001']['pct'])

        a.addone('6001')
        self.expected(4, a.s_sum['6001']['count'])
        exp = 100.0 * 4.0 / 7.0
        self.expected(exp, a.s_sum['6001']['pct'])

        self.expected(10, a.p_sum['6001']['count'])
        self.expected(10, a.p_sum['5081']['count'])

    # -------------------------------------------------------------------------
    def test_ctor_attrs(self):
        """
        Verify that a newly created Dimension object has the following
        attributes:
         - name (string)
         - sampsize (small float value, e.g., 0.005)
         - p_sum (empty dict)
         - s_sum (empty dict)
         - methods
            > sum_total
            > load
        """
        dimname = 'cos'
        CrawlConfig.add_config(close=True, dct=self.cfg_dict(U.my_name()))
        Checkable.Checkable.ex_nihilo()
        a = Dimension(name=dimname,
                      sampsize=0.005)
        for attr in ['name',
                     'sampsize',
                     'p_sum',
                     's_sum',
                     'sum_total',
                     'load',
                     ]:
            self.assertTrue(hasattr(a, attr),
                            "Object %s does not have expected attribute %s" %
                            (a, attr))

    # -------------------------------------------------------------------------
    def test_ctor_bad_attr(self):
        """
        Attempting to create a Dimension with attrs that are not in the
        settable list should get an exception.
        """
        dimname = 'bad_attr'
        CrawlConfig.add_config(close=True, dct=self.cfg_dict(U.my_name()))
        got_exception = False
        self.assertRaisesMsg(StandardError,
                             "Attribute 'catl' is not valid",
                             Dimension, name=dimname, catl=[1, 2, 3])

        self.assertRaisesMsg(StandardError,
                             "Attribute 'aardvark' is not valid",
                             Dimension, name=dimname, aardvark='Fanny Brice')

    # -------------------------------------------------------------------------
    def test_ctor_defaults(self):
        """
        A new Dimension with only the name specified should have the right
        defaults.
        """
        dimname = 'cos'
        CrawlConfig.add_config(close=True, dct=self.cfg_dict(U.my_name()))
        a = Dimension(name=dimname)
        self.expected(dimname, a.name)
        self.expected(0.01, a.sampsize)
        self.expected({}, a.p_sum)
        self.expected({}, a.s_sum)

    # -------------------------------------------------------------------------
    def test_ctor_no_name(self):
        """
        Attempting to create a Dimension without providing a name should get an
        exception.
        """
        self.assertRaisesMsg(StandardError,
                             "Caller must set attribute 'name'",
                             Dimension)

    # -------------------------------------------------------------------------
    def test_load_already(self):
        """
        With the database and a checkables table in place and records in the
        table, calling load() on a Dimension should load the information from
        the table into the object. However, it should only count records where
        last_check <> 0.
        """
        self.dbgfunc()
        U.conditional_rm(self.dbname())
        CrawlConfig.add_config(close=True, dct=self.cfg_dict(U.my_name()))
        Checkable.Checkable.ex_nihilo()
        chk = Checkable.Checkable
        testdata = [
            chk(rowid=1, path="/abc/001", type='f', cos='6001', checksum=0,
                last_check=0),
            chk(rowid=2, path="/abc/002", type='f', cos='6002', checksum=0,
                last_check=5),
            chk(rowid=3, path="/abc/003", type='f', cos='6003', checksum=1,
                last_check=0),
            chk(rowid=4, path="/abc/004", type='f', cos='6001', checksum=1,
                last_check=17),
            chk(rowid=5, path="/abc/005", type='f', cos='6002', checksum=0,
                last_check=0),
            chk(rowid=6, path="/abc/006", type='f', cos='6003', checksum=0,
                last_check=8),
            chk(rowid=7, path="/abc/007", type='f', cos='6001', checksum=0,
                last_check=0),
            chk(rowid=8, path="/abc/008", type='f', cos='6002', checksum=0,
                last_check=19),
            chk(rowid=9, path="/abc/009", type='f', cos='6003', checksum=0,
                last_check=0),
            ]

        # insert some test data into the table
        for t in testdata:
            t.persist()

        # get a default Dimension with the same name as the data in the table
        q = Dimension(name='cos')
        # this should load the data from the table into the object
        q.load()

        # verify the loaded data in the object
        self.expected('cos', q.name)
        self.assertTrue('6001' in q.p_sum.keys(),
                        "Expected '6001' in p_sum.keys()")
        self.assertTrue('6002' in q.p_sum.keys(),
                        "Expected '6001' in p_sum.keys()")
        self.assertTrue('6003' in q.p_sum.keys(),
                        "Expected '6003' in p_sum.keys()")
        self.assertTrue('6001' in q.s_sum.keys(),
                        "Expected '6001' in s_sum.keys()")
        self.assertTrue('6002' in q.s_sum.keys(),
                        "Expected '6002' in s_sum.keys()")
        self.assertTrue('6003' in q.s_sum.keys(),
                        "Expected '6003' in s_sum.keys()")

    # -------------------------------------------------------------------------
    def test_load_new(self):
        """
        With the database and checkables table in place, create a new Dimension
        that is not in the table. Calling load() on it should be a no-op -- the
        object should not be stored to the database and its contents should not
        be changed.
        """
        # reboot the database and call persist() to create the table without
        # adding any data
        U.conditional_rm(self.dbname())
        CrawlConfig.add_config(close=True, dct=self.cfg_dict(U.my_name()))
        Checkable.Checkable.ex_nihilo()

        ignore = Dimension(name='foobar')

        # get a Dimension object that is not in the table
        test = Dimension(name='notindb')
        # make a copy of the object for reference (not just a handle to the
        # same ojbect)
        ref = copy.deepcopy(test)

        # call load(), which should be a no op
        test.load()

        # verify that the object didn't change
        self.expected(ref.name, test.name)
        self.expected(ref.sampsize, test.sampsize)
        self.expected(ref.p_sum, test.p_sum)
        self.expected(ref.s_sum, test.s_sum)

    # -------------------------------------------------------------------------
    def test_repr(self):
        """
        Method __repr__ should return <Dimension(name='foo')>.
        """

        CrawlConfig.add_config(close=True, dct=self.cfg_dict(U.my_name()))
        exp = "Dimension(name='foo')"
        a = eval(exp)
        self.expected(exp, a.__repr__())

    # -------------------------------------------------------------------------
    def test_sum_total(self):
        """
        Return the sum of all the 'count' values in either the p_sum or s_sum
        dictionary.
        """
        CrawlConfig.add_config(close=True, dct=self.cfg_dict(U.my_name()))
        Checkable.Checkable.ex_nihilo()
        a = Dimension(name='sum_total')
        a.p_sum = {'6001': {'count': 2, 'pct': 50.0},
                   '5081': {'count': 2, 'pct': 50.0}
                   }
        a.s_sum = {'6001': {'count': 2, 'pct': 40.0},
                   '5081': {'count': 3, 'pct': 60.0}
                   }
        self.expected(4, a.sum_total())
        self.expected(4, a.sum_total(dict=a.p_sum))
        self.expected(5, a.sum_total(which='s'))
        self.expected(5, a.sum_total(dict=a.s_sum))
