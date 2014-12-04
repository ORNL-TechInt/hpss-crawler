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


#### 2014.0822

Modes of testing:

 - Full install:

       $ sudo pip install {$TARBALL|$GIT_REPO}

   This will put command line entry points (CLEPs) in $PATH (/usr/bin)

 - Virtual environment install:

       $ . $VENV/<env>/activate
       $ pip install {$TARBALL|$GIT_REPO}

   This will put CLEPs in $PATH (~/venv/<environment>/bin)

 - Not installed:

       $ . env-setup
           $ export PATH=$GIT_REPO/bin:$PATH
           $ export PYTHONPATH=$GIT_REPO

   Puts CLEPs in $PATH ($GIT_REPO/bin)


#### 2014.0903

Decided how to handle strings defined in messages.py that involve
formatter expressions. For example,


    more_than_one_ss = "More than one record found in %s for table %s"

The "_ss" at the end of the name indicates that the string contains
two occurrences of "%s" that need matching data to follow. A variable
name like

    my_special_string_dfs

would indicate a string with "%d", "%f", and "%s" embedded.


#### 2014.1013

Current software layers:

                                         test/test_*.py
                                         fakesmtp

   crawl   cv   rpt   tcc     mpra      html

   crawl_lib  cv_lib   rpt_lib  tcc_lib   mpra_lib   html_lib

         Checkable                              toolframe

            Alert     dbschem    Dimension     testhelp(?)

               hpss      CrawlDBI   CrawlMail   CrawlPlugin

       CrawlConfig

   util    version     messages    daemon

Source files may only import files below them in the hierarchy. We
need to add files to the layer below the _lib files. For example,
rpt_lib and html_lib should use the same titles for the CV report, the
TCC report, etc. Those titles should be defined in cv_sublib,
tcc_sublib, mpra_sublib. Both rpt_lib and html_lib need to retrieve
the last report time from the database, so we need rpt_sublib that can
be imported by both rpt_lib and html_lib. Etc.


0                                         test/test_*.py
0.1                                         fakesmtp

1   crawl   cv   rpt   tcc     mpra      html

2   crawl_lib  cv_lib   rpt_lib  tcc_lib   mpra_lib   html_lib

3         Checkable    {cv,tcc,mpra,rpt}_sublib      toolframe

4            Alert     dbschem    Dimension     testhelp(?)

5               hpss      CrawlDBI   CrawlMail   CrawlPlugin

6       CrawlConfig

7   util    version    daemon

8          messages

#### 2014-11-15

Updating software stack -- moving Dimension from layer 4 up to layer 3
so it can call routines in cv_lib(3). The stack rule:

 * A file may only import files at its own and lower levels in the
   stack, never files at a higher level than itself.

0                                         test/test_*.py
0.1                                         fakesmtp

 1   crawl   cv   rpt   tcc     mpra      html

 2   crawl_lib  cv_lib   tcc_lib   mpra_lib   html_lib

 3   Checkable         toolframe    rpt_lib

 4   Alert       Dimension     testhelp(?)

 5       {tcc,mpra,rpt}_sublib

 6               cv_sublib

 7               dbschem

 8               hpss      CrawlDBI   CrawlMail   CrawlPlugin

 9       CrawlConfig

10          util

11          messages    version     daemon


#### 2014-11-18  Running py.test -- options

   --all    run all the tests; without this, many will be skipped. The
            full set of tests takes about three minutes to run. The subset
            run without --all takes less than a minute.

   -x       exit on the first failing test.

   --nolog  don't log results. Without this, test results are written to
            hpssic/test/hpssic_test.log

   -rs      report the reason that each skipped test was skipped

   --dbg TEST|all
            drop into the debugger when running any test whose name contains
            TEST. If all is specified, drop into the debugger for each test
            run.

   -k EXP   run only tests whose names match EXP, which can be a python
            expression

   -v       report test names as they are run


#### 2014-12-02  Test Skipping

There are a few situations in which tests should be skipped:

 * Running on jenkins, several pieces of infrastructure are not available
   (hsi/hpss, db2, mysql), so tests that depend on those should be skipped.

 * With option --fast, tests marked 'slow' should be skipped.

 * When a particular piece of infrastructure (HPSS/hsi, db2, mysql) is not
   available in the development environment, we'd like to skip tests that
   depend on that infrastructure.

I learned yesterday that using pytest.mark.foo to mark tests and control
skipping does not work well for classes that inherit tests from parent
classes. The marks are assigned to the method, not the class, so the marks
leak over to sibling classes and skip or run tests that were supposed to go
the other way.

So, the (sub-optimal) strategy I have formulated for now is the following:

 * Some non-inherited tests will be marked with
   '@pytest.mark.jenkins_fail'. Those will not be run if 1) 'jenkins'
   appears in os.getcwd() or 2) the file 'jenkins' exists in the current
   directory when tests are run.

 * Tests can be skipped by putting some piece of the class name in the
   --skip option. For example, 'py.test --skip db2' will skip db2 tests,
   including those inherited from DBI_in_Base, without skipping sqlite or
   mysql tests inherited from DBI_in_Base. 'py.test --skip "db2 mysql"' will
   skip both db2 and mysql tests. This is based on looking at the test's
   class name and deciding whether the skip tag appears in it.

 * Slow tests can be skipped by using the option --fast on the py.test
   command line.


#### 2014-12-03

Updating software stack -- moving CrawlPlugin from layer 8 up to layer
3 so it can call routines in crawl_sublib. Adding crawl_sublib.py on
level 5. The stack rule:

 * To prevent dependency cycles, A file may only import files at a
   lower level in the stack than itself, never files at its own level
   or a higher level than itself.

0                                         test/test_*.py
0.1                                         fakesmtp

 1   crawl   cv   rpt   tcc     mpra      html

 2   crawl_lib  cv_lib   tcc_lib   mpra_lib   html_lib

 3   Checkable         toolframe    rpt_lib    CrawlPlugin

 4   Alert       Dimension     testhelp(?)

 5       {crawl,tcc,mpra,rpt}_sublib

 6               cv_sublib

 7               dbschem

 8               hpss      CrawlDBI   CrawlMail

 9       CrawlConfig

10          util

11          messages    version     daemon
