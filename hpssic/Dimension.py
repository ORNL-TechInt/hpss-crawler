#!/usr/bin/env python
"""
Track stratum proportions in a sample against a population
"""
import CrawlDBI
import pdb
import rpt_sublib


# -----------------------------------------------------------------------------
def get_dim(dname, reset=False):
    """
    Ensure that each named dimension is a singleton object.
    """
    if reset:
        if hasattr(get_dim, '_dims'):
            del get_dim._dims
        return None

    try:
        rval = get_dim._dims[dname]
    except AttributeError:
        get_dim._dims = {}
        get_dim._dims[dname] = Dimension(name=dname)
        rval = get_dim._dims[dname]
    except KeyError:
        get_dim._dims[dname] = Dimension(name=dname)
        rval = get_dim._dims[dname]
    return rval


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
        self.name = ''          # name must be set by caller
        self.sampsize = 0.01    # default sample size is 1%
        self.p_sum = {}         # population summary starts empty
        self.s_sum = {}         # sample summary starts empty
        for attr in kwargs:
            if attr in self.settable_attrl:
                setattr(self, attr, kwargs[attr])
            else:
                raise StandardError("Attribute '%s'" % attr +
                                    " is not valid for Dimension")
        if self.name == '':
            raise StandardError("Caller must set attribute 'name'")
        self.load()

    # -------------------------------------------------------------------------
    def __repr__(self):
        """
        A human readable representation of a Dimension object
        """
        rv = "Dimension(name='%s')" % self.name
        return rv

    # -------------------------------------------------------------------------
    def _compute_dict(self, rows):
        d = {}
        for (pcount, cat) in rows:
            if cat is not None and cat != '':
                d[cat] = {'count': pcount}
        total = sum(map(lambda x: x['count'], d.values()))
        for cat in d:
            d[cat]['pct'] = 100.0 * d[cat]['count'] / total
        return d

    # -------------------------------------------------------------------------
    def addone(self, cat):
        if cat not in self.s_sum:
            self.s_sum[cat] = {'count': 0, 'pct': 0}
        self.s_sum[cat]['count'] += 1
        tot = self.sum_total(which='s')
        for cval in self.s_sum:
            self.s_sum[cval]['pct'] = 100.0 * self.s_sum[cval]['count'] / tot

    # -------------------------------------------------------------------------
    def load(self, already_open=False):
        """
        Load this object with data from the database
        """
        if not already_open:
            self.db = CrawlDBI.DBI(dbtype='crawler')

        dimname = self.name
        try:
            # populate the p_sum structure
            rows = self.db.select(table='checkables',
                                  fields=["count(path)", dimname],
                                  where='type="f" and last_check <> 0',
                                  groupby=dimname)
            self.p_sum = self._compute_dict(rows)

            # populate the s_sum structure
            rows = self.db.select(table='checkables',
                                  fields=["count(path)", dimname],
                                  where='type = "f" and checksum = 1',
                                  groupby=dimname)
            self.s_sum = self._compute_dict(rows)
        except CrawlDBI.DBIerror:
            pass

        for cval in self.p_sum:
            if cval not in self.s_sum:
                self.s_sum[cval] = {'count': 0, 'pct': 0}

        if not already_open:
            self.db.close()

    # -------------------------------------------------------------------------
    def report(self):
        """
        Generate a string reflecting the current contents of the dimension
        """
        rval = ("\n   %-30s     %17s   %17s" % (self.name,
                                                "Population",
                                                "Sample"))

        rval += ("\n   %30s     %17s   %17s" % (30 * '-',
                                                17 * '-',
                                                17 * '-'))
        for cval in self.p_sum:
            show = cval + " - " + rpt_sublib.cos_description(cval)
            rval += ("\n   %-30.30s  " % show +
                     "   %8d" % self.p_sum[cval]['count'] +
                     "   %6.2f" % self.p_sum[cval]['pct'] +
                     "   %8d" % self.s_sum[cval]['count'] +
                     "   %6.2f" % self.s_sum[cval]['pct'])
        rval += ("\n   %-30.30s     %8d   %6.2f   %8d   %6.2f" %
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
    def vote(self, category):
        """
        If the category value does not yet appear in the sample, we want to
        vote for it.

        If the category value is in the sample, we want to vote for it if the
        percentage of the sample it represents is smaller than the percentage
        of the population it represents.

        Even if we vote for a category here, it only has a probability of
        getting intothe sample. It's not a sure thing.
        """
        if category is None or category == '':
            return 0
        elif category not in self.s_sum:
            return 1
        elif self.s_sum[category]['pct'] < self.p_sum[category]['pct']:
            return 1
        else:
            return 0
