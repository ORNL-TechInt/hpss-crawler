#!/usr/bin/env python
"""
Track stratum proportions in a sample against a population
"""
import CrawlDBI

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
    # dbname = 'drill.db'

    # attributes that can be set through the constructor
    settable_attrl = ['name', 'sampsize']

    # -------------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        """
        Initialize/create a Dimension object - we expect a name, to be used as
        a database key, and a sample size parameter relative to the population
        size, given as a fraction that can be used directly as a multiplier
        (e.g., 0.05 for 5%, 0.005 for .5%, etc.).

        Field 'name' in the object corresponds with field 'category' in the db.
        """
        # self.dbname = Dimension.dbname
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
        rv = "Dimension(name='%s')" % self.name
        return rv

    # -------------------------------------------------------------------------
    def db(self):
        """
        Get our database connection (initialize it if necessary)
        """
        try:
            return self.dbh
        except AttributeError:
            self.dbh = CrawlDBI.DBI()
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

