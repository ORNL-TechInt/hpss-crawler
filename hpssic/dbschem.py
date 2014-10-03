"""
This file defines the crawler database layout and contains routines that create
tables, drop tables, and alter tables.
"""
import CrawlConfig
import CrawlDBI
import messages as MSG
import re
import warnings

tdefs = {
    'checkables': {'fields': ['rowid       integer primary key autoincrement',
                              'path        text',
                              'type        text',
                              'cos         text',
                              'cart        text',
                              'ttypes      text',
                              'checksum    int',
                              'last_check  int',
                              'fails       int',
                              'reported    int'
                              ]
                   },
    'mpra':       {'fields': ['rowid       integer primary key autoincrement',
                              'type        text',
                              'scan_time   integer',
                              'start_time  integer',
                              'end_time    integer',
                              'hits        integer',
                              ]
                   },
    'report':     {'fields': ['rowid       integer primary key autoincrement',
                              'report_time integer',
                              ]
                   },
    'tape_types': {'fields': ['rowid       integer primary key autoincrement',
                              "type        int",
                              "subtype     int",
                              "name        text"
                              ]
                   },
    'tcc_data':   {'fields': ['rowid       integer primary key autoincrement',
                              'check_time  integer',
                              'low_nsobj_id  integer',
                              'high_nsobj_id integer',
                              'correct     integer',
                              'error       integer'
                              ]
                   }
    }


# -----------------------------------------------------------------------------
def alter_table(table=None, addcol=None, dropcol=None, pos=None, cfg=None):
    """
    Alter a table, either adding a column (*addcol*) in position *pos*, or
    dropping a column (*dropcol*). This function should be idempotent, so we
    need to check for the column before adding it.
    """
    if cfg:
        db = CrawlDBI.DBI(dbtype="crawler", cfg=cfg)
    else:
        db = CrawlDBI.DBI(dbtype="crawler")

    if addcol and dropcol:
        raise CrawlDBI.DBIerror("addcol and dropcol are mutually exclusive")
    elif addcol:
        fieldname = addcol.split()[0]
    elif dropcol:
        fieldname = dropcol

    try:
        db.alter(table=table, addcol=addcol, dropcol=dropcol, pos=pos)
        rval = "Successful"
    except CrawlDBI.DBIerror as e:
        if (dropcol and
            "Can't DROP '%s'; check that column/key exists" % fieldname
                in str(e)):
            # edit the error number off the front of the message
            rval = re.sub("\s*\d+:\s*", "", e.value)
        elif (addcol and
              "Duplicate column name '%s'" % fieldname
              in str(e)):
            # edit the error number off the front of the message
            rval = re.sub("\s*\d+:\s*", "", e.value)
        else:
            raise

    db.close()
    return rval


# -----------------------------------------------------------------------------
def drop_table(cfg=None, prefix=None, table=None):
    """
    This wraps the table dropping operation.
    """
    if table is None:
        return(MSG.nothing_to_drop)

    if cfg is None:
        cfg = CrawlConfig.get_config()

    if prefix is None:
        prefix = cfg.get('dbi-crawler', 'tbl_prefix')
    else:
        cfg.set('dbi-crawler', 'tbl_prefix', prefix)

    db = CrawlDBI.DBI(dbtype="crawler", cfg=cfg)
    if not db.table_exists(table=table):
        rval = ("Table '%s_%s' does not exist" % (prefix, table))
    else:
        db.drop(table=table)
        if db.table_exists(table=table):
            rval = ("Attempt to drop table '%s_%s' failed" % (prefix, table))
        else:
            rval = ("Attempt to drop table '%s_%s' was successful" %
                    (prefix, table))

    db.close()
    return rval


# -----------------------------------------------------------------------------
def drop_tables_matching(tablike):
    """
    Drop tables with names matching the *tablike* expression. At the time of
    writing, this is only used for drop test tables ('test_%')
    """
    if CrawlDBI.mysql_available:
        tcfg = CrawlConfig.get_config()
        tcfg.set('dbi-crawler', 'tbl_prefix', '')

        db = CrawlDBI.DBI(cfg=tcfg, dbtype='crawler')
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore",
                                    "Can't read dir of .*")
            tlist = db.select(table="information_schema.tables",
                              fields=['table_name'],
                              where="table_name like '%s'" % tablike)
            for (tname,) in tlist:
                if db.table_exists(table=tname):
                    db.drop(table=tname)
        db.close()


# -----------------------------------------------------------------------------
def make_table(tabname, cfg=None):
    """
    Make the indicated table if it does not exist
    """
    db = CrawlDBI.DBI(dbtype='crawler', cfg=cfg)
    if db.table_exists(table=tabname):
        rval = "Already"
    else:
        db.create(table=tabname, fields=tdefs[tabname]['fields'])
        rval = "Created"
    db.close()
    return rval
