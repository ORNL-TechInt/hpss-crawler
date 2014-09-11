import CrawlDBI

tdefs = {
    'checkables': {'fields': ['rowid       integer primary key autoincrement',
                              'path        text',
                              'type        text',
                              'cos         text',
                              'cart        text',
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
def make_table(tabname, cfg=None):
    """
    Make the indicated table if it does not exist
    !@! test this
    """
    db = CrawlDBI.DBI(dbtype='crawler', cfg=cfg)
    if not db.table_exists(table=tabname):
        db.create(table=tabname, fields=tdefs[tabname]['fields'])
    db.close()


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
