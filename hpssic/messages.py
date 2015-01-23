cov_no_data = "Coverage.py warning: No data was collected.\r\n"

all_mpra_data_lost = """
   All MPRA data will be lost. Unless you have a backup, this data is not
   recoverable.

   Are you sure? > """

alter_table_string = ("On alter(), table name must be a string")

create_table_string = ("On create(), table name must be a string")

db_closed = ("Cannot operate on a closed database")

db_closed_already_rgx = ("(closing a closed connection|" +
                         "Connection is not active)")

db2_unsupported_S = ("%s not supported for DB2")

dbname_not_allowed = ("dbname may not be specified here")

dbname_required = ("A database name is required")

default_int_float = ("CrawlConfig.get_time: default must be int or float")

default_mail_subject = ("HPSS Integrity Crawler ALERT")

default_piddir = ("/tmp/crawler")

drop_table_string = ("On drop(), table name must be a string")

drop_table_empty = ("On drop(), table name must not be empty")

empty_message = ("Empty message")

history_cv_not_loaded = ("Can't load cv data without --read-log FILENAME")

history_filename_ignored = ("'cv' not specified for --load. " +
                            "--read-log file ignored.")

history_invalid_format_S = ("'%s' is not a valid history report format")

history_load_dryrun_SSSS = """
     Would attempt to load table '%s'
                     in database '%s'
                         on host '%s'
                   (cv from file '%s')
                   """

history_options = ("--read_log, --show, and --reset are mutually exclusive")

history_reset_dryrun_SSS = """
     Would attempt to drop table '%s'
                     in database '%s'
                         on host '%s'
                            """

hpss_unavailable = ("HPSS Unavailable")

insert_ignore_bool = ("On insert(), ignore must be boolean")

invalid_attr_SS = ("Attribute '%s' is not valid for %s")

invalid_attr_rgx = ("Attribute '.*' is not valid for .*")

invalid_msg_body = ("The message body must be a string")

invalid_recip_list = ("The To: list should be a comma-separated list of " +
                      "e-mail addresses in a string")

invalid_sender_S = ("From address (%s) should be a string containing a " +
                    "valid e-mail address")

invalid_subject_S = ("Subject (%s) is not a string")

invalid_time_unit_S = ("invalid time unit '%s'")

invalid_time_mag_S = ("invalid time magnitude '%s'")

list_expected_S = ("Expected a list, but got a '%s'")

lsp_invalid_file_type = ("Invalid file type in 'ls -P' output")

lsp_output_not_found = ("ls -P output not found in lsp_parse input")

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

no_recip_list = ("No recipient list specified or in configuration")

no_such_path_component_SD = ("No match for object '%s' with parent %d")

no_such_table_del_rgx = ("(\\(1146, \"Table '.*?' doesn't exist\"\\)|" +
                         "delete from .*? where name='.*?': " +
                         "no such table: .*? \\(dbname=.*?\\))")

no_such_table_drop_rgx = ("(1051: Unknown table '.*' \\(dbname=.*\\)|" +
                          "no such table: .* \\(dbname=.*\\))")

no_such_table_upd_rgx = ("(\\(1146, \"Table '.*?' doesn't exist\"\\)|" +
                         "no such table: .*? \\(dbname=.*?\\))")

no_such_table_desc_rgx = ("(Unknown table '.*'|" +
                          "DESCRIBE not supported for DB2)")

no_such_table_S = ("Unknown table '%s'")

not_in_bftapeseg_S = ("Bitfile ID %s\n   not found in table BFTAPESEG")

not_in_bitfile_S = ("Bitfile ID %s\n   not found in table BITFILE")

not_in_nsobject_D = ("Object ID %d not found in table NSOBJECT")

nothing_to_drop = ("table is None, nothing to drop")

only_one = ("Only one of --show, --drop, or --pop is allowed")

password_missing_rgx = ('Security processing failed with reason "\d" ' +
                        '\("PASSWORD MISSING"\)')

table_already_mysql = ("1050: Table 'test_create_already' already exists")

table_already_sqlite = ("table test_create_already already exists")

table_created_S = ("Table '%s' created")

table_already_S = ("Table '%s' already exists")

tblpfx_required = ("A table prefix is required")

too_few_val = ("need more than 0 values to unpack")

too_many_val = ("too many values to unpack")

unknown_dbtype = ("Unknown database type")

unrecognized_arg_S = ("Unrecognized argument to %s. " +
                      "Only 'cfg=<config>' is accepted")

unrecognized_plugin_S = ("Plugin '%s' not found in the configured list")

valid_dbtype = ("dbtype must be 'hpss' or 'crawler'")

wildcard_selects = ("Wildcard selects are not supported. " +
                    "Please supply a list of fields.")
