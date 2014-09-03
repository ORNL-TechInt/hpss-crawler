all_mpra_data_lost = """
   All MPRA data will be lost. Unless you have a backup, this data is not
   recoverable.

   Are you sure? > """

db_closed = "Cannot operate on a closed database"

dbname_not_allowed = "dbname may not be specified here"

no_cfg_found = """
            No configuration found. Please do one of the following:
             - cd to a directory with an appropriate crawl.cfg file,
             - create crawl.cfg in the current working directory,
             - set $CRAWL_CONF to the path of a valid crawler configuration, or
             - use --cfg to specify a configuration file on the command line.
            """

nothing_to_drop = "table is None, nothing to drop"

valid_dbtype = "dbtype must be 'hpss' or 'crawler'"

wildcard_selects = ("Wildcard selects are not supported. " +
                    "Please supply a list of fields.")
