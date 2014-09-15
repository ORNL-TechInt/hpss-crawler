cov_no_data = "Coverage.py warning: No data was collected.\r\n"

all_mpra_data_lost = """
   All MPRA data will be lost. Unless you have a backup, this data is not
   recoverable.

   Are you sure? > """

db_closed = "Cannot operate on a closed database"

dbname_not_allowed = "dbname may not be specified here"

invalid_recip_list = ("The To: list should be a comma-separated list of " +
                      "e-mail addresses in a string")

more_than_one_ss = "More than one record found in %s for table %s"

no_cfg_found = """
            No configuration found. Please do one of the following:
             - cd to a directory with an appropriate crawl.cfg file,
             - create crawl.cfg in the current working directory,
             - set $CRAWL_CONF to the path of a valid crawler configuration, or
             - use --cfg to specify a configuration file on the command line.
            """

no_recip_list = "No recipient list specified or in configuration"

nothing_to_drop = "table is None, nothing to drop"

valid_dbtype = "dbtype must be 'hpss' or 'crawler'"

wildcard_selects = ("Wildcard selects are not supported. " +
                    "Please supply a list of fields.")
