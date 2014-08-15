<head><title>HPSSIC Development Notes</title></head>

# HPSS Integrity Crawler
## Development Notes

#### 2014-07-30

* A database best practice is to always specify the fields of interest in a
  SELECT statement, particularly in SELECT statements embedded in code. It's
  not so bad when you're spelunking through the database in discovery mode.
  Rather, a SELECT * statement can be problematic when

   1. it is embedded in software, and
   2. the structure of the table changes

  When this happens, if fields are added to the end of the row, you'll
  probably be okay. Your software will keep looking for the data it wants at
  the correct indexes in the row.

  However, if columns swap places or a new column is added early in the row,
  your software will suddenly be looking at the wrong positions in the row.

  If you specify the fields you're interested in, this won't be a problem.

  Clearly, we need to ensure that all our calls to DBI.select() specify a
  field list. Optionally, we could also update DBI.select() to throw a
  DBIerror when it gets an empty fields list.
  

#### 2014-08-13

* The crawler needs read/select access to the following DB2 tables:

      CFG           COS
      CFG           HIER
      CFG           PVLPV
      SUBSYS        BFMIGRREC
      SUBSYS        BFPURGEREC
      SUBSYS        BFTAPESEG
      SUBSYS        BITFILE
      SUBSYS        NSOBJECT


#### 2014-08-15

* The crawler can be started while sitting in another directory somewhere as
  long as

   1 the crawl command is in the $PATH,

   2 it can find the configuration file from the command line, $CRAWL_CONF, or
     as ./crawl.cfg, and

   3 all the paths in the configuration are absolute.

* Keep database access (i.e., calls to CrawlDBI.DBI()) in the _lib.py files,
  wrapped in well-encapsulated routines that can be 1) easily called from the
  interactive tools and plugins and 2) easily tested.

* A lot of the routines currently in the system are missing doc strings. It
  would be helpful to have a test that hunts for such and reports them.
