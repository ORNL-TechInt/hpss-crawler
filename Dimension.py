#!/usr/bin/env python
"""
Track stratum proportions in a sample against a population
"""
import copy
import CrawlDBI
import os
import testhelp
import toolframe
import traceback as tb
import util

# -----------------------------------------------------------------------------
class Dimension(object):
    """
    The Dimension object has a name, for example, 'cos', indicating what the
    dimension represents. Within the object, the actual data are tracked in
    dictionaries p_sum (population summary) and s_sum (sample summary).

    The struct of both dictionaries looks like this:

        {'5081': {'count': 1700, 'pct': 25.432},
         '6002': {'count':  900, 'pct': 12.785},
         ...}

    The keys for the top level dict are the categories for the dimension.

    The count field is the number of items found in the category for the
    population or the sample.

    The pct field is the percent of the total represented by the category of
    the total, either in the population or the sample.

    In the database, this data is stored in records like

        name      category    p_count    p_pct   s_count    s_pct
        'cos'     '5081'         1700   25.432        50   24.987
        'cos'     '6002'          900   12.785        28   11.938
        ...
        
    """
    # The default database to use
    dbname = 'drill.db'

    # attributes that can be set through the constructor
    settable_attrl = ['dbname', 'name', 'sampsize']

    # -------------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        """
        Initialize/create a Dimension object - we expect a name, to be used as
        a database key, and a sample size parameter relative to the population
        size, given as a fraction that can be used directly as a multiplier
        (e.g., 0.05 for 5%, 0.005 for .5%, etc.).

        Field 'name' in the object corresponds with field 'category' in the db.
        """
        self.dbname = Dimension.dbname
        self.name = ''          # name must be set by caller
        self.sampsize = 0.01    # default sample size is 1%
        self.p_sum = {}         # population summary starts empty
        self.s_sum = {}         # sample summary starts empty
        for attr in kwargs:
            if attr in self.settable_attrl:
                setattr(self, attr, kwargs[attr])
            else:
                raise StandardError("Attribute '%s'" % attr+
                                    " is not valid for Dimension" )
        if self.name == '':
            raise StandardError("Caller must set attribute 'name'")
        self.db_init()
        self.load()

    # -------------------------------------------------------------------------
    def __repr__(self):
        """
        A human readable representation of a Dimension object
        """
        rv = "Dimension(name='%s'" % self.name
        if self.dbname != Dimension.dbname:
            rv += ", dbname='%s'" % self.dbname
        rv += ")"
        return rv

    # -------------------------------------------------------------------------
    def db(self):
        """
        Get our database connection (initialize it if necessary)
        """
        try:
            return self.dbh
        except AttributeError:
            self.dbh = CrawlDBI.DBI(dbname=self.dbname)
            return self.dbh
    
    # -------------------------------------------------------------------------
    def db_init(self):
        """
        Get a database handle. If our table has not yet been created, do so.
        """
        if hasattr(self,'dbh'):
            return
        db = self.db()
        if not db.table_exists(table='dimension'):
            db.create(table='dimension',
                      fields=['id int primary key',
                              'name text',
                              'category text',
                              'p_count int',
                              'p_pct real',
                              's_count int',
                              's_pct real'])
        
    # -------------------------------------------------------------------------
    def load(self):
        """
        Load this object with data from the database
        """
        db = self.db()
        rows = db.select(table='dimension',
                         fields=['category', 'p_count', 'p_pct', 's_count',
                                 's_pct'],
                         where='name = ?',
                         data=(self.name,))
        for row in rows:
            (cval, p_count, p_pct, s_count, s_pct) = row
            self.p_sum.setdefault(cval, {'count': p_count,
                                         'pct': p_pct})
            self.s_sum.setdefault(cval, {'count': s_count,
                                         'pct': s_pct})
        pass
    
    # -------------------------------------------------------------------------
    def persist(self):
        """
        Save this object to the database.

        First, we have to find out which rows are already in the database.
        Then, comparing the rows from the database with the object, we
        construct two lists: rows to be updated, and rows to be inserted.
        There's no way for categories to be removed from a summary dictionary,
        so we don't have to worry about deleting entries from the database.
        """
        if self.p_sum == {}:
            return
        
        i_list = []
        i_data = []
        u_list = []
        u_data = []
        
        # Find out which rows are already in the database.
        db = self.db()
        rows = db.select(table='dimension',
                         fields=['name', 'category'],
                         where='name = ?',
                         data=(self.name,))
        
        # Based on the population data, sort the database entries into items to
        # be updated versus those to be inserted.
        for k in self.p_sum:
            if (self.name, k) in rows:
                u_list.append((self.name, k))
            else:
                i_list.append((self.name, k))

        # For each item in the update list, build the data tuple and add it to
        # the update list
        for (name, cat) in u_list:
            u_data.append((self.p_sum[cat]['count'],
                           self.p_sum[cat]['pct'],
                           self.s_sum[cat]['count'],
                           self.s_sum[cat]['pct'],
                           self.name,
                           cat))
        # If the data tuple list is not empty, issue an update against the
        # database
        if u_data != []:
            db.update(table='dimension',
                      fields=['p_count', 'p_pct', 's_count', 's_pct'],
                      where='name=? and category=?',
                      data=u_data)
            
        # For each item in the insert list, build the data tuple and add it to
        # the insert list
        for (name, cat) in i_list:
            i_data.append((self.name,
                           cat,
                           self.p_sum[cat]['count'],
                           self.p_sum[cat]['pct'],
                           self.s_sum[cat]['count'],
                           self.s_sum[cat]['pct']))
        # If the insert data tuple list is not empty, issue the insert
        if i_data != []:
            db.insert(table='dimension',
                      fields=['name', 'category', 'p_count',
                              'p_pct', 's_count', 's_pct'],
                      data=i_data)
                          
        pass
    
    # -------------------------------------------------------------------------
    def report(self):
        """
        Generate a string reflecting the current contents of the dimension
        """
        rval = ("\n%-8s     %17s   %17s" % (self.name,
                                            "Population",
                                            "Sample"))

        rval += ("\n%8s     %17s   %17s" % (8 * '-',
                                            17 * '-',
                                            17 * '-'))
        for cval in self.p_sum:
            rval += ("\n%-8s  "   % cval +
                     "   %8d"   % self.p_sum[cval]['count'] +
                     "   %6.2f" % self.p_sum[cval]['pct'] +
                     "   %8d"   % self.s_sum[cval]['count'] +
                     "   %6.2f" % self.s_sum[cval]['pct'])
        rval += ("\n%-8s     %8d   %6.2f   %8d   %6.2f" %
                 ("Total",
                  self.sum_total(which='p'),
                  sum(map(lambda x: x['pct'], self.p_sum.values())),
                  self.sum_total(which='s'),
                  sum(map(lambda x: x['pct'], self.s_sum.values()))))
        rval += "\n"
        return rval
        
    # -------------------------------------------------------------------------
    def sum_total(self, dict=None, which='p'):
        """
        Return the total of all the 'count' entries in one of the dictionaries.
        The dictionary to sum up can be indicated by passing an argument to
        dict, or by passing abbreviations of 'population' or 'sample' to which.
        """
        if dict is not None:
            rv = sum(map(lambda x: x['count'], dict.values()))
        elif which.startswith('s'):
            rv = sum(map(lambda x: x['count'], self.s_sum.values()))
        else:
            rv = sum(map(lambda x: x['count'], self.p_sum.values()))
        return rv
    
    # -------------------------------------------------------------------------
    def update_category(self, catval, p_suminc=1, s_suminc=0):
        """
        Update the population (and optionally the sample) count for a category.
        If the category is new, it is added to the population and sample
        dictionaries. Once the counts are updated, we call _update_pct() to
        update the percent values as well.
        """
        if catval not in self.p_sum:
            self.p_sum[catval] = {'count': p_suminc, 'pct': 0.0}
        else:
            self.p_sum[catval]['count'] += p_suminc

        if catval not in self.s_sum:
            self.s_sum[catval] = {'count': s_suminc, 'pct': 0.0}
        else:
            self.s_sum[catval]['count'] += s_suminc

        self._update_pct()
    
    # -------------------------------------------------------------------------
    def _update_pct(self):
        """
        Update the 'pct' values in the population and sample dictionaries. This
        is called by update_category and is tested as part of the tests for it.
        This routine should not normally be called from outside the class.
        """
        for d in [self.p_sum, self.s_sum]:
            total = sum(map(lambda x: x['count'], d.values()))
            if 0 < total:
                for key in d:
                    d[key]['pct'] = 100.0 * d[key]['count'] / total
            else:
                for key in d:
                    d[key]['pct'] = 0.0
        pass

    # -------------------------------------------------------------------------
    def vote(self, category):
        if ((category in self.s_sum) and
            (self.s_sum[category]['pct'] < self.p_sum[category]['pct'])):
            return 1
        else:
            return 0

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
    testdir = './test.d'
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
            > update_category
            > update_pct
            > sum_total
            > persist
            > load
        """
        dimname = 'ctor_attrs'
        a = Dimension(dbname=self.testdb,
                      name=dimname,
                      sampsize=0.005)
        for attr in ['name',
                     'sampsize',
                     'p_sum',
                     's_sum',
                     'update_category',
                     '_update_pct',
                     'sum_total',
                     'persist',
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
        got_exception = False
        try:
            a = Dimension(dbname=self.testdb,
                          name=dimname,
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
            a = Dimension(dbname=self.testdb,
                          name=dimname,
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
        a = Dimension(dbname=self.testdb, name=dimname)
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
        try:
            a = Dimension(dbname=self.testdb)
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
    def test_db_already_no_table(self):
        """
        Creating a Dimension object should initialize the dimension table in
        the existing database if the db exists but the table does not.
        """
        util.conditional_rm(self.testdb)
        db = CrawlDBI.DBI(dbname=self.testdb)
        self.assertFalse(db.table_exists(table='dimension'),
                        'Did not expect table \'dimension\' in database')
        self.assertTrue(os.path.exists(self.testdb),
                        "Expected to find database file '%s'" % self.testdb)

        a = Dimension(dbname=self.testdb, name='already_nt')
        a.persist()
        self.assertTrue(os.path.exists(self.testdb),
                        "Expected to find database file '%s'" % self.testdb)

        self.assertTrue(db.table_exists(table='dimension'),
                        'Expected table \'dimension\' in database')
        db.close()
        
    # -------------------------------------------------------------------------
    def test_ex_nihilo_nofile(self):
        """
        Creating and persisting a Dimension object should initialize the
        database and table dimension if they do not exist.
        """
        util.conditional_rm(self.testdb)

        a = Dimension(dbname=self.testdb, name='ex_nihilo')
        a.persist()
        self.assertTrue(os.path.exists(self.testdb),
                        "Expected to find database file '%s'" % self.testdb)

        db = CrawlDBI.DBI(dbname=self.testdb)
        self.assertTrue(db.table_exists(table='dimension'),
                        "Expected table 'dimension' in database")
        db.close()
        
    # -------------------------------------------------------------------------
    def test_ex_nihilo_notable(self):
        """
        If the db file exists but the table does not, creating and persisting a
        Dimension object should create the table 'dimension'.
        """
        util.conditional_rm(self.testdb)
        testhelp.touch(self.testdb)
        
        a = Dimension(dbname=self.testdb, name='ex_nihilo')
        a.persist()
        self.assertTrue(os.path.exists(self.testdb),
                        "Expected to find database file '%s'" % self.testdb)

        db = CrawlDBI.DBI(dbname=self.testdb)
        self.assertTrue(db.table_exists(table='dimension'),
                        "Expected table 'dimension' in database")
        db.close()
        
    # -------------------------------------------------------------------------
    def test_load_already(self):
        """
        With the database and dimension table in place and a named Dimension in
        place in the table, calling load() on a Dimension with the same name as
        the one in the table should load the information from the table into
        the object.
        """
        util.conditional_rm(self.testdb)
        testdata = [('cos', '6002', 24053, 17.2563, 190, 15.2343),
                    ('cos', '5081', 14834, 98.753,  105, 28.4385)]
        # create the dimension table without putting anything in it
        z = Dimension(dbname=self.testdb, name='foobar')
        z.persist()

        # insert some test data into the table
        db = CrawlDBI.DBI(dbname=self.testdb)
        db.insert(table='dimension',
                  fields=['name', 'category',
                          'p_count', 'p_pct', 's_count', 's_pct'],
                  data=testdata)
        db.close()

        # get a default Dimension with the same name as the data in the table
        q = Dimension(dbname=self.testdb, name='cos')
        # this should load the data from the table into the object
        q.load()

        # verify the loaded data in the object
        self.expected('cos', q.name)
        self.assertTrue(testdata[0][1] in q.p_sum.keys(),
                        "Expected '%s' in p_sum.keys()" % testdata[0][1])
        self.assertTrue(testdata[0][1] in q.s_sum.keys(),
                        "Expected '%s' in s_sum.keys()" % testdata[0][1])
        self.assertTrue(testdata[1][1] in q.p_sum.keys(),
                        "Expected '%s' in p_sum.keys()" % testdata[1][1])
        self.assertTrue(testdata[1][1] in q.s_sum.keys(),
                        "Expected '%s' in s_sum.keys()" % testdata[1][1])

        self.expected(testdata[0][2], q.p_sum[testdata[0][1]]['count'])
        self.expected(testdata[0][3], q.p_sum[testdata[0][1]]['pct'])
        self.expected(testdata[0][4], q.s_sum[testdata[0][1]]['count'])
        self.expected(testdata[0][5], q.s_sum[testdata[0][1]]['pct'])

        self.expected(testdata[1][2], q.p_sum[testdata[1][1]]['count'])
        self.expected(testdata[1][3], q.p_sum[testdata[1][1]]['pct'])
        self.expected(testdata[1][4], q.s_sum[testdata[1][1]]['count'])
        self.expected(testdata[1][5], q.s_sum[testdata[1][1]]['pct'])

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
        ignore = Dimension(dbname=self.testdb, name='foobar')
        ignore.persist()

        # get a Dimension object that is not in the table
        test = Dimension(dbname=self.testdb, name='notindb')
        # make a copy of the object for reference (not just a handle to the
        # same ojbect)
        ref = copy.deepcopy(test)

        # call load(), which should be a no op
        test.load()

        # verify that the object didn't change
        self.expected(ref.dbname, test.dbname)
        self.expected(ref.name, test.name)
        self.expected(ref.sampsize, test.sampsize)
        self.expected(ref.p_sum, test.p_sum)
        self.expected(ref.s_sum, test.s_sum)

        # TODO: verify that the object still is not in the table
        
    # -------------------------------------------------------------------------
    def test_persist_already(self):
        """
        With the database and dimension table in place and a named Dimension in
        place in the table, updating and persisting a Dimension with the same
        name should update the database record.
        """
        util.conditional_rm(self.testdb)

        # first, we need to stick some records in the table
        test = Dimension(dbname=self.testdb, name='foobar')
        test.update_category('<1M')
        test.update_category('<1G')
        test.persist()

        # verify that the records are in the table as expected
        db = CrawlDBI.DBI(dbname=self.testdb)
        rows = db.select(table='dimension',
                         fields=['name', 'category', 'p_count', 'p_pct',
                                 's_count', 's_pct'])
        self.expected(2, len(rows))
        self.expected(('foobar', '<1M', 1, 50.0, 0, 0.0), rows[0])
        self.expected(('foobar', '<1G', 1, 50.0, 0, 0.0), rows[1])

        # update the Dimension values
        test.update_category('<1M', s_suminc=1)
        test.update_category('<1G', s_suminc=2)
        test.update_category('<1T', s_suminc=1)
        test.persist()
        
        # verify that the records in the database got updated
        rows = db.select(table='dimension',
                         fields=['name', 'category', 'p_count', 'p_pct',
                                 's_count', 's_pct'])
        db.close()
        self.expected(3, len(rows))
        self.expected(('foobar', '<1M', 2, 40.0, 1, 25.0), rows[0])
        self.expected(('foobar', '<1G', 2, 40.0, 2, 50.0), rows[1])
        self.expected(('foobar', '<1T', 1, 20.0, 1, 25.0), rows[2])
        
    # -------------------------------------------------------------------------
    def test_persist_new(self):
        """
        With the database and dimension table in place, create a new Dimension
        that is not in the table. Calling persist() on it should store it in
        the dimension table in the database.
        """
        util.conditional_rm(self.testdb)
        
        # instantiating the object initializes the database
        new = Dimension(dbname=self.testdb, name='notintable')
        new.update_category('5081', s_suminc=1)
        new.update_category('6001')
        new.update_category('6002', s_suminc=1)
        new.update_category('6003')
        new.persist()

        # verify that the data is in the table
        db = CrawlDBI.DBI(dbname=self.testdb)
        rows = db.select(table='dimension',
                         fields=['name', 'category', 'p_count', 'p_pct',
                                 's_count', 's_pct'])
        db.close()
        self.expected(4, len(rows))
        testdata = [('notintable', '5081', 1, 25.0, 1, 50.0),
                    ('notintable', '6001', 1, 25.0, 0, 0.0),
                    ('notintable', '6002', 1, 25.0, 1, 50.0),
                    ('notintable', '6003', 1, 25.0, 0, 0.0)]
        for row in testdata:
            self.assertTrue(row in rows,
                            "Expected '%s' to be in rows (%s)" % (row, rows))
        
    # -------------------------------------------------------------------------
    def test_repr(self):
        """
        Method __repr__ should return <Dimension(name='foo')> if the dbname is
        the default. If the dbname is something else, __repr__ should show it.
        Like so: <Dimension(name='baz', dbname='./test.d/test.db')>
        """
        
        exp = "Dimension(name='foo')"
        a = eval(exp)
        self.expected(exp, a.__repr__())

        exp = "Dimension(name='baz', dbname='%s')" % self.testdb
        b = eval(exp)
        self.expected(exp, b.__repr__())
        
    # -------------------------------------------------------------------------
    def test_sum_total(self):
        """
        Return the sum of all the 'count' values in either the p_sum or s_sum
        dictionary.
        """
        a = Dimension(dbname=self.testdb, name='sum_total')
        a.update_category('6001')
        a.update_category('6001', s_suminc=2)
        a.update_category('5081')
        a.update_category('5081', s_suminc=3)
        self.expected(4, a.sum_total())
        self.expected(4, a.sum_total(dict=a.p_sum))
        self.expected(5, a.sum_total(which='s'))
        self.expected(5, a.sum_total(dict=a.s_sum))
        
    # -------------------------------------------------------------------------
    def test_update_category_already(self):
        """
        If the category exists, the psum and ssum counts and percentages should
        be updated appropriately. Call sum_total and sum_pct to check the
        summary counts and percentages.
        """
        a = Dimension(dbname=self.testdb,
                      name='xcat',
                      sampsize=0.05)
        a.update_category('6001', p_suminc=1, s_suminc=1)
        a.update_category('5081')
        self.expected('xcat', a.name)
        self.expected(0.05, a.sampsize)
        self.expected({'count': 1, 'pct': 50.0}, a.p_sum['5081'])
        self.expected({'count': 1, 'pct': 50.0}, a.p_sum['6001'])
        self.expected({'count': 1, 'pct': 100.0}, a.s_sum['6001'])
        self.expected({'count': 0, 'pct': 0.0}, a.s_sum['5081'])

    # -------------------------------------------------------------------------
    def test_update_category_new(self):
        """
        If the category does not exist, it should be added to psum and ssum as
        dictionary keys and the counts and percentages should be updated
        appropriately. Call sum_total and sum_pct to check the summary counts
        and percentages.
        """
        a = Dimension(dbname=self.testdb,
                      name='newcat',
                      sampsize=0.01)
        a.update_category('5081', p_suminc=1, s_suminc=1)
        self.expected('newcat', a.name)
        self.expected(0.01, a.sampsize)
        self.expected({'5081': {'count': 1, 'pct': 100.0}}, a.p_sum)
        self.expected({'5081': {'count': 1, 'pct': 100.0}}, a.s_sum)

# -----------------------------------------------------------------------------
if __name__ == '__main__':
    toolframe.ez_launch(test='DimensionTest',
                        logfile='crawl_test.log')
        
