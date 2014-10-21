cov_no_data = "Coverage.py warning: No data was collected.\r\n"

all_mpra_data_lost = """
   All MPRA data will be lost. Unless you have a backup, this data is not
   recoverable.

   Are you sure? > """

db_closed = "Cannot operate on a closed database"

dbname_not_allowed = "dbname may not be specified here"

default_mail_subject = ("HPSS Integrity Crawler ALERT")

empty_message = ("Empty message")

hpss_unavailable = ("HPSS Unavailable")

invalid_msg_body = ("The message body must be a string")

invalid_recip_list = ("The To: list should be a comma-separated list of " +
                      "e-mail addresses in a string")

invalid_sender_S = ("From address (%s) should be a string containing a " +
                    "valid e-mail address")

invalid_subject_S = ("Subject (%s) is not a string")

more_than_one_ss = "More than one record found in %s for table %s"

multiple_objects_S = ("Multiple objects found for bitfile $s")

no_bitfile_found_S = ("No bitfile found with id %s")

no_cfg_found = """
            No configuration found. Please do one of the following:
             - cd to a directory with an appropriate crawl.cfg file,
             - create crawl.cfg in the current working directory,
             - set $CRAWL_CONF to the path of a valid crawler configuration, or
             - use --cfg to specify a configuration file on the command line.
            """

no_recip_list = "No recipient list specified or in configuration"

no_such_path_component_SD = ("No match for object '%s' with parent %d")

not_in_bftapeseg_S = ("Bitfile ID %s\n   not found in table BFTAPESEG")

not_in_bitfile_S = ("Bitfile ID %s\n   not found in table BITFILE")

not_in_nsobject_D = ("Object ID %d not found in table NSOBJECT")

nothing_to_drop = "table is None, nothing to drop"

valid_dbtype = "dbtype must be 'hpss' or 'crawler'"

wildcard_selects = ("Wildcard selects are not supported. " +
                    "Please supply a list of fields.")
