#!/usr/bin/env python
"""
Track stratum proportions in a sample against a population
"""
from hpssic import Checkable
import copy
from hpssic import CrawlDBI
from hpssic.Dimension import Dimension
import os
import pdb
import sys
from hpssic import testhelp
from hpssic import toolframe
import traceback as tb
from hpssic import util

mself = sys.modules[__name__]
logfile = "%s/crawl_test.log" % os.path.dirname(mself.__file__)

# -----------------------------------------------------------------------------
def setUpModule():
    """
    Set up for testing
    """
    testhelp.module_test_setup(DimensionTest.testdir)
    
# -----------------------------------------------------------------------------
def tearDownModule():
    """
    Clean up after testing
    """
    testhelp.module_test_teardown(DimensionTest.testdir)

# -----------------------------------------------------------------------------
class DimensionTest(testhelp.HelpedTestCase):
    """
    Tests for the Dimension class
    """
    testdir = '%s/test.d' % os.path.dirname(mself.__file__)
    testdb = '%s/test.db' % testdir
    
    # -------------------------------------------------------------------------
    def test_ctor_attrs(self):
        """
        Verify that a newly created Dimension object has the following attributes:
         - name (string)
         - sampsize (small float value, e.g., 0.005)
         - p_sum (empty dict)
         - s_sum (empty dict)
         - methods
            > sum_total
            > load
        """
        dimname = 'ctor_attrs'
        testhelp.db_config(self.testdir, util.my_name())
        Checkable.Checkable.ex_nihilo()
        a = Dimension(name=dimname,
                      sampsize=0.005)
        for attr in ['name',
                     'sampsize',
                     'p_sum',
                     's_sum',
                     'sum_total',
                     'load',]:
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
        testhelp.db_config(self.testdir, util.my_name())
        got_exception = False
        try:
            a = Dimension(name=dimname,
                          catl=[1,2,3])
        except StandardError, e:
            got_exception = True
            self.assertTrue("Attribute 'catl' is not valid" in tb.format_exc(),
                            "Got the wrong StandardError: " +
                            '\n"""\n%s\n"""' % str(e))
        except:
            self.fail("Got unexpected exception: " +
                      '"""\n%s\n"""' % tb.format_exc())
        self.assertTrue(got_exception,
                        "Expected a StandardError but didn't " +
                        "get one for attr 'catl'")

        got_exception = False    
        try:
            a = Dimension(name=dimname,
                          aardvark='Fanny Brice')
        except StandardError, e:
            got_exception = True
            self.assertTrue("Attribute 'aardvark' is not valid" in
                            tb.format_exc(),
                            "Got the wrong StandardError: " +
                            '\n"""\n%s\n"""' % tb.format_exc())
        except:
            self.fail("Got unexpected exception: " +
                      '"""\n%s\n"""' % tb.format_exc())
        self.assertTrue(got_exception,
                        "Expected an StandardError but didn't get " +
                        "one for attr 'aardvark'")
            
    # -------------------------------------------------------------------------
    def test_ctor_defaults(self):
        """
        A new Dimension with only the name specified should have the right
        defaults.
        """
        dimname = 'defaults'
        testhelp.db_config(self.testdir, util.my_name())
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
        got_exception = False
        testhelp.db_config(self.testdir, util.my_name())
        try:
            a = Dimension()
        except StandardError, e:
            got_exception = True
            self.assertTrue("Caller must set attribute 'name'" in
                            tb.format_exc(),
                            "Got the wrong StandardError: " +
                            '\n"""\n%s\n"""' % tb.format_exc())
        except:
            self.fail("Got unexpected exception: " +
                      '"""\n%s\n"""' % tb.format_exc())
        self.assertTrue(got_exception,
                        "Expected an exception but didn't get one")
        
    # -------------------------------------------------------------------------
    # def test_db_already_no_table(self):
    #     """
    #     Creating a Dimension object should initialize the dimension table in
    #     the existing database if the db exists but the table does not.
    #     """
    #     util.conditional_rm(self.testdb)
    #     testhelp.db_config(self.testdir, util.my_name())
    #     db = CrawlDBI.DBI()
    #     self.assertFalse(db.table_exists(table='dimension'),
    #                     'Did not expect table \'dimension\' in database')
    #     self.assertTrue(os.path.exists(self.testdb),
    #                     "Expected to find database file '%s'" % self.testdb)
    # 
    #     a = Dimension(name='already_nt')
    #     a.persist()
    #     self.assertTrue(os.path.exists(self.testdb),
    #                     "Expected to find database file '%s'" % self.testdb)
    # 
    #     self.assertTrue(db.table_exists(table='dimension'),
    #                     'Expected table \'dimension\' in database')
    #     db.close()
        
    # -------------------------------------------------------------------------
    # def test_ex_nihilo_nofile(self):
    #     """
    #     Creating and persisting a Dimension object should initialize the
    #     database and table dimension if they do not exist.
    #     """
    #     util.conditional_rm(self.testdb)
    #     testhelp.db_config(self.testdir, util.my_name())
    # 
    #     a = Dimension(name='ex_nihilo')
    #     a.persist()
    #     self.assertTrue(os.path.exists(self.testdb),
    #                     "Expected to find database file '%s'" % self.testdb)
    # 
    #     db = CrawlDBI.DBI()
    #     self.assertTrue(db.table_exists(table='dimension'),
    #                     "Expected table 'dimension' in database")
    #     db.close()
        
    # -------------------------------------------------------------------------
    # def test_ex_nihilo_notable(self):
    #     """
    #     If the db file exists but the table does not, creating and persisting a
    #     Dimension object should create the table 'dimension'.
    #     """
    #     util.conditional_rm(self.testdb)
    #     testhelp.db_config(self.testdir, util.my_name())
    #     testhelp.touch(self.testdb)
    #     
    #     a = Dimension(name='ex_nihilo')
    #     a.persist()
    #     self.assertTrue(os.path.exists(self.testdb),
    #                     "Expected to find database file '%s'" % self.testdb)
    # 
    #     db = CrawlDBI.DBI()
    #     self.assertTrue(db.table_exists(table='dimension'),
    #                     "Expected table 'dimension' in database")
    #     db.close()
        
    # -------------------------------------------------------------------------
    def test_load_already(self):
        """
        With the database and a checkables table in place and records in the
        table, calling load() on a Dimension should load the information from
        the table into the object. However, it should only count records where
        last_check <> 0.
        """
        util.conditional_rm(self.testdb)
        testhelp.db_config(self.testdir, util.my_name())
        Checkable.Checkable.ex_nihilo()
        chk = Checkable.Checkable
        testdata = [
            chk(rowid=1, path="/abc/001", type='f', cos='6001', checksum=0, last_check=0),
            chk(rowid=2, path="/abc/002", type='f', cos='6002', checksum=0, last_check=5),
            chk(rowid=3, path="/abc/003", type='f', cos='6003', checksum=1, last_check=0),
            chk(rowid=4, path="/abc/004", type='f', cos='6001', checksum=1, last_check=17),
            chk(rowid=5, path="/abc/005", type='f', cos='6002', checksum=0, last_check=0),
            chk(rowid=6, path="/abc/006", type='f', cos='6003', checksum=0, last_check=8),
            chk(rowid=7, path="/abc/007", type='f', cos='6001', checksum=0, last_check=0),
            chk(rowid=8, path="/abc/008", type='f', cos='6002', checksum=0, last_check=19),
            chk(rowid=9, path="/abc/009", type='f', cos='6003', checksum=0, last_check=0),
            ]

        # create the dimension table without putting anything in it
        # z = Dimension(name='foobar')
        # z.persist()
    
        # insert some test data into the table
        for t in testdata:
            t.persist()
    
        # get a default Dimension with the same name as the data in the table
        q = Dimension(name='cos')
        # this should load the data from the table into the object
        q.load()
    
        # verify the loaded data in the object
        # pdb.set_trace()
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
        With the database and dimension table in place, create a new Dimension
        that is not in the table. Calling load() on it should be a no-op -- the
        object should not be stored to the database and its contents should not
        be changed.
        """
        # reboot the database and call persist() to create the table without
        # adding any data
        util.conditional_rm(self.testdb)
        testhelp.db_config(self.testdir, util.my_name())
        Checkable.Checkable.ex_nihilo()

        ignore = Dimension(name='foobar')
        # ignore.persist()

        # get a Dimension object that is not in the table
        test = Dimension(name='notindb')
        # make a copy of the object for reference (not just a handle to the
        # same ojbect)
        ref = copy.deepcopy(test)

        # call load(), which should be a no op
        test.load()

        # verify that the object didn't change
        # self.expected(ref.dbname, test.dbname)
        self.expected(ref.name, test.name)
        self.expected(ref.sampsize, test.sampsize)
        self.expected(ref.p_sum, test.p_sum)
        self.expected(ref.s_sum, test.s_sum)

        # TODO: verify that the object still is not in the table
        
    # -------------------------------------------------------------------------
    # def test_persist_already(self):
    #     """
    #     With the database and dimension table in place and a named Dimension in
    #     place in the table, updating and persisting a Dimension with the same
    #     name should update the database record.
    #     """
    #     util.conditional_rm(self.testdb)
    #     testhelp.db_config(self.testdir, util.my_name())
    # 
    #     # first, we need to stick some records in the table
    #     test = Dimension(name='foobar')
    #     test.update_category('<1M')
    #     test.update_category('<1G')
    #     test.persist()
    # 
    #     # verify that the records are in the table as expected
    #     db = CrawlDBI.DBI()
    #     rows = db.select(table='dimension',
    #                      fields=['name', 'category', 'p_count', 'p_pct',
    #                              's_count', 's_pct'])
    #     self.expected(2, len(rows))
    #     self.expected(('foobar', '<1M', 1, 50.0, 0, 0.0), rows[0])
    #     self.expected(('foobar', '<1G', 1, 50.0, 0, 0.0), rows[1])
    # 
    #     # update the Dimension values
    #     test.update_category('<1M', s_suminc=1)
    #     test.update_category('<1G', s_suminc=2)
    #     test.update_category('<1T', s_suminc=1)
    #     test.persist()
    #     
    #     # verify that the records in the database got updated
    #     rows = db.select(table='dimension',
    #                      fields=['name', 'category', 'p_count', 'p_pct',
    #                              's_count', 's_pct'])
    #     db.close()
    #     self.expected(3, len(rows))
    #     self.expected(('foobar', '<1M', 2, 40.0, 1, 25.0), rows[0])
    #     self.expected(('foobar', '<1G', 2, 40.0, 2, 50.0), rows[1])
    #     self.expected(('foobar', '<1T', 1, 20.0, 1, 25.0), rows[2])
        
    # -------------------------------------------------------------------------
    # def test_persist_new(self):
        """
        With the database and dimension table in place, create a new Dimension
        that is not in the table. Calling persist() on it should store it in
        the dimension table in the database.

        We no longer persist the Dimension object -- this test is obsolete
        """
        # util.conditional_rm(self.testdb)
        # testhelp.db_config(self.testdir, util.my_name())
        # Checkable.Checkable.ex_nihilo()
        # 
        # # instantiating the object initializes the database
        # new = Dimension(name='notintable')
        # new.update_category('5081', s_suminc=1)
        # new.update_category('6001')
        # new.update_category('6002', s_suminc=1)
        # new.update_category('6003')
        # new.persist()
        # 
        # # verify that the data is in the table
        # db = CrawlDBI.DBI()
        # rows = db.select(table='dimension',
        #                  fields=['name', 'category', 'p_count', 'p_pct',
        #                          's_count', 's_pct'])
        # db.close()
        # self.expected(4, len(rows))
        # testdata = [('notintable', '5081', 1, 25.0, 1, 50.0),
        #             ('notintable', '6001', 1, 25.0, 0, 0.0),
        #             ('notintable', '6002', 1, 25.0, 1, 50.0),
        #             ('notintable', '6003', 1, 25.0, 0, 0.0)]
        # for row in testdata:
        #     self.assertTrue(row in rows,
        #                     "Expected '%s' to be in rows (%s)" % (row, rows))
        
    # -------------------------------------------------------------------------
    def test_repr(self):
        """
        Method __repr__ should return <Dimension(name='foo')> if the dbname is
        the default. If the dbname is something else, __repr__ should show it.
        Like so: <Dimension(name='baz', dbname='./test.d/test.db')>
        """
        
        testhelp.db_config(self.testdir, util.my_name())
        Checkable.Checkable.ex_nihilo()
        exp = "Dimension(name='foo')"
        a = eval(exp)
        self.expected(exp, a.__repr__())

        exp = "Dimension(name='baz')"
        b = eval(exp)
        self.expected(exp, b.__repr__())
        
    # -------------------------------------------------------------------------
    def test_sum_total(self):
        """
        Return the sum of all the 'count' values in either the p_sum or s_sum
        dictionary.
        """
        testhelp.db_config(self.testdir, util.my_name())
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
        
    # # -------------------------------------------------------------------------
    # def test_update_category_already(self):
    #     """
    #     If the category exists, the psum and ssum counts and percentages should
    #     be updated appropriately. Call sum_total and sum_pct to check the
    #     summary counts and percentages.
    #     """
    #     testhelp.db_config(self.testdir, util.my_name())
    #     a = Dimension(
    #                   name='xcat',
    #                   sampsize=0.05)
    #     a.update_category('6001', p_suminc=1, s_suminc=1)
    #     a.update_category('5081')
    #     self.expected('xcat', a.name)
    #     self.expected(0.05, a.sampsize)
    #     self.expected({'count': 1, 'pct': 50.0}, a.p_sum['5081'])
    #     self.expected({'count': 1, 'pct': 50.0}, a.p_sum['6001'])
    #     self.expected({'count': 1, 'pct': 100.0}, a.s_sum['6001'])
    #     self.expected({'count': 0, 'pct': 0.0}, a.s_sum['5081'])
    # 
    # # -------------------------------------------------------------------------
    # def test_update_category_new(self):
    #     """
    #     If the category does not exist, it should be added to psum and ssum as
    #     dictionary keys and the counts and percentages should be updated
    #     appropriately. Call sum_total and sum_pct to check the summary counts
    #     and percentages.
    #     """
    #     testhelp.db_config(self.testdir, util.my_name())
    #     a = Dimension(
    #                   name='newcat',
    #                   sampsize=0.01)
    #     a.update_category('5081', p_suminc=1, s_suminc=1)
    #     self.expected('newcat', a.name)
    #     self.expected(0.01, a.sampsize)
    #     self.expected({'5081': {'count': 1, 'pct': 100.0}}, a.p_sum)
    #     self.expected({'5081': {'count': 1, 'pct': 100.0}}, a.s_sum)

# -----------------------------------------------------------------------------
if __name__ == '__main__':
    toolframe.ez_launch(test='DimensionTest',
                        logfile=logfile)
        
