<head><title>HPSSIC Reference Manual</title></head>

# HPSS Integrity Crawler Reference Manual

## Installation

### Prerequisites

NOTE: Some of these steps may not be necessary if your system already
has components installed.

* Python 2.6.6 or higher

            $ yum install python
            $ yum install python-devel.x86_64

* MySQL

            $ yum install mysql

* MySQL/Python database interface 1.2.3

            $ yum install MySQL-python

* DB2 client

  [Set up whatever tunnels](#tunnel) are required to reach barstow,
  then fetch the DB2 package

            $ scp [-P port] barstow:/hpss_prereq/db2_10.5/DB2_Svr_V10.5_Linux_x86-64.tar.gz . 
        
  Unpack DB2

            $ tar zxf DB2_Svr_V10.5_Linux_x86-64.tar.gz

  Install the DB2 Client

            $ cd server
            $ ./db2_install
            [specify "CLIENT" at product prompt]
            [confirm default install location when prompted]

* Set up DB2 client support on the server instance. Wait to do this
  step until you are ready to stop and restart DB2. On DB2 server:

            Add "db2c_hpssdb   50000/tcp" to /etc/services.
            $ db2 update database manager configuration using svcename db2c_hpssdb
            $ db2set DB2COMM=tcpip
            $ db2stop
            $ db2start

    to verify:

            $ db2 get dbm config | grep db2c_hpssdb - /etc/services
            (standard input): TCP/IP Service name                          (SVCENAME) = db2c_hpssdb
            /etc/services:db2c_hpssdb       50000/tcp
            $ db2set -all DB2COMM
            [i] TCPIP

  Note: The name 'db2c_hpssdb' can be whatever you like, but the name
  in /etc/services must match the SVCENAME in the database manager
  configuration.
        
* Optionally, create a read-only user on the server instance:

            $ groupadd -g 9903 hpssic
            $ useradd -u 9903 -g hpssic -m -d /home/hpssic hpssic
            $ passwd hpssic

  For each database (HCFG, HSUBSYS1, etc.), do the following to
  grant select access on all the HPSS tables.

            $ db2 connect to DATABASE
            $ db2 -x "select 'grant select on table ' || rtrim(tabschema) || '.' ||
                rtrim(tabname) || ' to user hpssic' from syscat.tables 
                where tabschema = 'HPSS'"

  To execute the output of the above command, append '| db2 +p -tv' 
  or pipe the output to a temp file and then pipe that to 
  'db2 +p -tv'
    
            $ db2 -x "select 'grant select on table ' || rtrim(tabschema) || '.' ||
                rtrim(tabname) || ' to user hpssic' from syscat.tables 
                where tabschema = 'HPSS'" | db2 +p -tv
        
  If you would prefer to only grant the user access to the
  tables the crawler actually accesses, those tables are show in
  the following list. You would need to issue the following
  command on each of these tables.

             $ GRANT SELECT ON TABLE _tablename_ TO USER hpssic

                 Table                Database
                 cartridge            cfg
                 cos                  cfg
                 hier                 cfg
                 pvlpv                cfg

                 bfmigrrec            subsys
                 bfpurgerec           subsys
                 bftapeseg            subsys
                 bitfile              subsys
                 nsobject             subsys

                 $ db2 connect reset

* DB2 client support on the client machine

  * As root:

            $ groupadd -g 9903 hpssic
            $ useradd  -u 9903 -g hpssic -m -d /home/hpssic hpssic
            $ passwd hpssic
            $ db2icrt -s client hpssic

  * As user hpssic:

            $ . /home/hpssic/sqllib/db2profile

* ibm-db

            $ yum install ibm_db

  If ibm-db is not available in your yum repositories, install the ibm-db python module using pip:

            $ export IBM_DB_HOME=/opt/ibm/db2/V10.5
            $ pip install ibm-db

  Then tell the link loader where to find the DB2 libraries:

            $ echo $IBM_DB_HOME/lib64 > /etc/ld.so.conf.d/db2-x86_64.conf
            $ ldconfig

  Note: The connector in ibm-db/ibm_db goes back and forth between hyphen and
  underscore depending on the context. Pip only recognizes the
  hyphenated name while yum expects the underscore.

## Install hpssic

            $ yum install hpssic

  If hpssic is not available in your yum repositories,

            $ git clone https://github.com/ORNL-TechInt/hpss-crawler.git
            $ cd hpssic
            $ pip install .

## The 'crawl' command

The crawl program contains a collection of subfunctions for managing
the crawler and its data.

        $ crawl help
           cfgdump - load a config file and dump its contents
           cleanup - remove any test directories left behind
           cvreport - show the database status
           dbdrop - drop a database table
           fire - run a plugin
           log - write a message to the indicated log file
           start - if the crawler is not already running as a daemon, start it
           status - report whether the crawler is running or not
           stop - shut down the crawler daemon if it is running
           help - show this list

The commands most commonly used, for starting, stopping, and checking
the status of the crawler, are 'start', 'stop', and 'status'.

The other subfunctions are documented in more detail later in this
document.

## Configuration

<a name="ConfigPrecedence">
### Configuration Precedence

Configuration values are set based on (in order of precedence)

1. arguments from the command line, 
1. values defined in the environment, 
1. values in the configuration file itself (when this makes sense), and 
1. default values encoded in the program.

So, values specified on the command line override everything else.
Values specified in the environment override values from the
configuration file or defaults. Values specified in the configuration
file override program defaults.

Setting the configuration file name in the configuration file itself
would create a bootstrapping issue -- the crawler would have to know
the configuration file name in order to look up the configuration file
name. So the name of the configuration file must be specified in the
environment or on the command line, or the default will be used.

<a name="ConfigDefault">
### Default Configuration File

The default configuration filename is 'crawl.cfg' in the current
working directory. Non-default configuration files can be used by
specifying environment variable CRAWL_CONF or by specifying the file
path on the crawl command line. For example,

        $ cd /somewhere/else
        $ export CRAWL_CONF=/var/hpssic/crawl.cfg
        $ crawl start

Or

        $ crawl start --cfg /var/hpssic/crawl.cfg

If the configuration file is changed while the crawler is running, the
crawler will detect this and reload its configuration, so
configuration changes usually will not require restarting the crawler
(unless a configuration update happens to tickle a bug that causes the
crawler to crash, for example).

<a name="ConfigSyntax">
### Configuration Syntax

The configuration file follows the format defined by the [Python
ConfigParser](https://docs.python.org/2/library/configparser.html)
module, which is similar to the ancient .ini file format
(http://en.wikipedia.org/wiki/INI_file). The file is divided into
sections. Each section begins with a section name in square brackets.
For example,

        [crawler]

The 'crawler' section is required and contains the following items:

        archive_dir = <path>
            Full log files will be copied to archive_dir for long term
            storage. These are not removed when logmax is reached.
            (string)

        context = PROD
            Name of the crawler instance, may be 'PROD', 'TEST',
            'DEV', etc. (string)

        exitpath = <path name>
            If this file exists, the crawler will remove it and exit.
            (string)

        heartbeat = <time interval>
            The crawler will write heartbeat records to the log file
            this frequently so there will be an indication whether it
            is still making progress. (time interval)

        hsi_timeout = <time interval>
            The maximum amount of time to wait for hsi commands to
            complete. (time interval)

        logmax = <integer>
            The number of log files to be kept around. Once this many
            log files have accumulated, when a new one is created, the
            oldest will be deleted. (integer)

        logsize = <size>
            The maximum size of the log file before it will be closed
            and renamed and a new one will be opened. (size)

        logpath = <file path>
            Where log records will be written. (string)

        notify-e-mail = <email-address>
            Default target addresses if not otherwise set.
            (comma-separated list of addresses)

        plugin-dir = <dirname>
            The location of the plugin directory. (string)

        plugins = <plugin list>
            List of plugins to be run, selected from 'cv', 'tcc',
            'mpra', 'rpt'. (comma-separated list of strings)

        sleep_time = <seconds>
            How long 'crawl stop' will sleep between peeks at the exit
            path to see whether the crawler has shut down. (time
            interval)

        stopwait_timeout = <seconds>
            Number of seconds 'crawl stop' will wait for crawler to
            shut down before giving up and throwing an exception.
            (time interval)

        verbose = <boolean>
            If this is true, more information will be logged.
            (boolean)

        xlim_count = <integer>
            If this many exceptions are encountered within a short
            time window, the crawler will shut down. (integer)

        xlim_ident = <integer>
            If this many identical exceptions are encountered over any
            length of time, the crawler will shut down. (integer)

        xlim_time = <time interval>
            This is the time window for xlim_count. (time interval)

        xlim_total = <integer>
            If this many total excpetions are encountered over any
            length of time, the crawler will shut down. (integer)

Here's an example of what the crawler section of a configuration file
might look like:

        [crawler]
        context    = PROD
        exitpath   = /tmp/hpssic/PROD.exit
        sleep_time = 0.25
        stop_timeout = 5.0

        heartbeat = 10s

        plugin-dir = ./plugins
        plugins = cv, tcc, mpra

        logpath    = hpss_crawl.log
        logsize    = 5mb
        logmax     = 5
        archive_dir = ./history

        notify-e-mail = admin@somewhere.org

        verbose = false
        hsi_timeout = 150

        xlim_count  = 3
        xlim_time   = 7.0
        xlim_ident  = 5
        xlim_total  = 10

### Configuration Data Types

#### Boolean

The value True can be indicated by the strings "1", "yes", "true", or
"on". Other values will be interpreted as False.

#### Email Address

A valid e-mail address.

#### Size

A size value consists of an integer followed by an option size unit.
Recognized size units are b(yte), k(ilobyte), m(egabyte), g(igabyte),
t(erabyte), p(etabyte), e(xabyte), z(ettabyte), y(ottabyte). If no
unit is specified, the default is byte.

#### String

A string is an ordered list of alphanumeric characters plus '/', '-',
'_', '.'. Strings in a configuration file should not be quoted. 

#### Time Interval

Time intervals are specified using a number (integer or float) and an
optional unit. The default unit if none is specified is one second.
The following units are recognized:

* seconds: s, sec, second, seconds
* minutes: m, min, minute, minutes
* hours:   h, hr, hour, hours
* days:    d, day, days
* weeks:   w, week, weeks
* months:  month, months
* years:   y, year, years

### Plugin Sections

For each plugin named in the "plugins" option in the "crawler"
section, there must be a section with the same name which describes
the configuration for the plugin.

Two standard options are defined which apply to all plugins. These are

        fire: <boolean>
        frequency: <time interval>

If a plugin does not specify these, fire defaults to False (so the
plugin will not run unless its configuration section contains a "fire
= yes" line) and frequency defaults to 1 hour.

The plugin may define any other options it requires in its
configuration section.

### Alerts Section

The alerts section defines the alerts that will be raised when a
notable condition occurs during crawler operation. Currently, three
types of alerts are supported:

        [alerts]
        log           = %s
        email         = tom.barron, tusculum@gmail.com
        shell         = touch foobar

A _log_ alert is written to the log file.

An _email_ alert is sent to the list of addresses.

A _shell_ alert causes the command line on the right side of the equal
sign to be run.


#### Log File Alerts

Log file alerts are written to the log file. The string after "log = "
can include information in addition to the message generated by the
program. For example, since each plugin can specify its own alerts
section, we might define a special alerts section for the Checksum
Verifier:

        [alerts]
        log           = checksum verifier: %s

The alert information generated by the program will replace the '%s'.

#### E-mail Alerts

If e-mail alerts are configured, the configuration file contains a
comma-separated list of e-mail addresses to receive the alert e-mails
as shown in the example above.

#### Shell Alerts

Shell alerts trigger an arbitrary program to be run. Such a program
might send a text message, write a log message to syslog, etc.


### dbi-crawler Section

The dbi-crawler section tells the crawler which database (MySQL or
SQLite) to use for storing information gathered by the various
plugins. It contains the following options:

        [dbi-crawler]
        dbtype = sqlite
        dbname = <filename>
        tbl_prefix = <table-prefix>

        [dbi-crawler]
        dbtype = mysql
        hostname = <mysql server host>
        dbname = <database name>
        username = <database user name>
        password = <base64-encoded password>
        tbl_prefix = <table-prefix>

If an sqlite database is in use, it will reside in a local file,
specified as option 'dbname'. Option 'tbl_prefix' will be used to
identify tables belonging to the current instance of HPSSIC.

If a mysql database is in use, it will typically run on another
machine. The 'hostname' option is used to tell HPSSIC where to find
the database server. Option 'dbname' indicates the name of the
database to use on the remote server. Options 'username' and
'password' provide login information for accessing the database.
Option 'tbl_prefix' is prepended to the table names to identify the
tables as belonging to the current instance of HPSSIC.

### dbi-hpss Section

The dbi-hpss section tells the crawler how to access HPSS' DB2
databases. It contains the following options:

        [dbi-hpss]
        dbtype = db2
        cfg = <name of the HPSS cfg database>
        sub = <name of the HPSS subsys database>
        hostname = <DB2 server host>
        port = <port number>
        username = <username>
        password = <base64-encoded password>
        tbl_prefix = <DB2 schema name>

HPSS is supported by two DB2 databases, one normally named 'cfg' or
'hcfg', the other typically names 'subsys1' or 'hsubsys1'. Support for
multiple subsystems will be added to HPSSIC in a future release.

Option 'hostname' tells HPSSIC where to find the database. Option
'port' is the Ethernet port number DB2 listens on for incoming client
connections. This is the port number added to /etc/services when
setting up DB2 client support above. Options 'username' and 'password'
provide login information for accessing the database. These options
will typically match those of the read-only user set up during DB2
setup. If no read-only user was set up, any hpss-related account can
be used. All operations against the database through the DB2 client
interface are strictly read only.

Option 'tbl_prefix' should match the schema in which the HPSS tables
are defined. This is usually 'hpss' but may be set differently when
creating the HPSS databases.

## Operation

### Starting, Stopping, Checking Status

To start the crawler

        crawl start [--cfg <config file>] [--log <log file>]

To check the status of the crawler

        crawl status

To shut down the crawler

        crawl stop


### Log Files

HPSSIC reports events to a log file. The default log file is either
/var/log/crawl.log (if the Crawler runs as root) or /tmp/crawl.log (if
the Crawler runs as a non-root user). The log file can be placed
somewhere else either by setting environment variable CRAWL_LOG or by
specifying the file path on the crawl command line. For example,

        export CRAWL_LOG=/var/hpssic/crawl.log
        crawl start

Or

        crawl start --log /var/hpssic/crawl.log

## Other Subfunctions

Besides start, stop, and status, the other subfunctions of crawl are:

### cfgdump

> Load the configuration file and dump its contents to stdout. This is
> an easy way to verify that there are no syntax errors in the
> configuration file.

### cleanup

> Running the unit tests for the software will normally remove any test
> data created for the test. If some is left behind, this subfunction
> will remove it.

### dbdrop

> This subfunction can be used to drop tables from the MySQL or sqlite
> database. This subfunction does not operate on DB2 databases and
> therefore cannot be used to drop tables from the HPSS database.

> Dropping database tables is an easy way to reset the various plugins
> and have them start from scratch.

### fire

> This subfunction can be used to test a specific plugin by firing it
> one time.

### help

> With no arguments, this subfunction reports a list of all of the
> crawler's subfunctions. If the name of a subfunction is passed as an
> argument, the usage notes for that subfunction will be displayed.

### history

> This subfunction can be used to manage and access the history table,
> which records the time and result of each plugin run.

### log

> This subfunction can be used to write a message to the log file.

### pw_decode

> This subfunction accepts a base64-encoded password, decodes, and
> displays it. Be careful with this. If someone is looking over your
> shoulder, they will be able to see the plain text version of the
> password.

### pw_encode

> This subfunction accepts a plain text password and displays the
> base64-encoded version.

### syspath

> This subfunction reports Python's sys.path variable, which is a list
> of directories to be searched for Python modules.

### version

> This subfunction reports the HPSSIC version.

## Support Programs

> Each plugin provides several files:
> 
>   1. A plugin file loaded and run by the HPSSIC daemon,
> 
>   2. An interactive command line program for managing tables related
>      to the plugin, testing, simulating a plugin run, etc.,
> 
>   3. A library file containing code common to the plugin and the
>      command line program, and
> 
>   4. A sub-library file providing low level support for the plugin,
>      the command line program, and the higher level library file.

<blockquote><blockquote>
<table style="border: 1px solid black; border-collapse: collapse" cellpadding="5"><tr>
    <th style="border: 1px solid black">Plugin Name
    <th style="border: 1px solid black">Plugin File
    <th style="border: 1px solid black">Command Line
    <th style="border: 1px solid black">Library File
    <th style="border: 1px solid black">Sub-library File
  </tr><tr>
    <td style="border: 1px solid black">cv
    <td style="border: 1px solid black">cv_plugin.py
    <td style="border: 1px solid black">cv.py
    <td style="border: 1px solid black">cv_lib.py
    <td style="border: 1px solid black">cv_sublib.py
  </tr><tr>
    <td style="border: 1px solid black">mpra
    <td style="border: 1px solid black">mpra_plugin.py
    <td style="border: 1px solid black">mpra.py
    <td style="border: 1px solid black">mpra_lib.py
    <td style="border: 1px solid black">mpra_sublib.py
  </tr><tr>
    <td style="border: 1px solid black">rpt
    <td style="border: 1px solid black">rpt_plugin.py
    <td style="border: 1px solid black">rpt.py
    <td style="border: 1px solid black">rpt_lib.py
    <td style="border: 1px solid black">rpt_sublib.py
  </tr><tr>
    <td style="border: 1px solid black">tcc
    <td style="border: 1px solid black">tcc_plugin.py
    <td style="border: 1px solid black">tcc.py
    <td style="border: 1px solid black">tcc_lib.py
    <td style="border: 1px solid black">tcc_sublib.py
</tr></table>
</blockquote></blockquote>

### cv

#### addcart

> Upgrade the HPSSIC database schema by adding the cart field to the
> checkable table.

#### dropcart

> Drop field cart from the checkable table.

#### fail_reset

> Reset a failing path so it can be checked again

#### lscos

> Manage the lscos table

#### nulltest

> Show database rows containing NULL values to see what they look like.

#### popcart

> Populate field cart in the checkable table.

#### report

> Report the current state of the checksum verifier. This produces a
> report showing the size of the total population and the size of the
> representative sample based on COS and media type.
> 
>         $ cv report
>         cos                                       Population              Sample
>         ------------------------------     -----------------   -----------------
>         6002 - ???                               14     3.10          6     7.79
>         6003 - ???                                5     1.11          3     3.90
>         6001 - ???                              326    72.28         31    40.26
>         5081 - ???                               86    19.07         28    36.36
>         6054 - ???                               12     2.66          5     6.49
>         6057 - ???                                6     1.33          3     3.90
>         6056 - ???                                2     0.44          1     1.30
>         Total                                   451   100.00         77   100.00
> 
> 
>         ttypes                                    Population              Sample
>         ------------------------------     -----------------   -----------------
>         STK T10000/T10000B(1000GB),STK            2     0.45          1     1.30
>         STK T10000/T10000C(5000GB),STK           18     4.04          8    10.39
>         STK T10000/T10000A(500GB) - ??           86    19.33         28    36.36
>         STK T10000/T10000B(1000GB) - ?          336    75.51         40    51.95
>         STK T10000/T10000C(5000GB) - ?            3     0.67          0     0.00
>         Total                                   445   100.00         77   100.00

#### show_next

> Report rows in the checkable table in the order that they will be checked.

#### simplug

> Fire the cv plugin one time.

#### test_check

> Run a check on a specific entry by path or rowid.

#### ttype_add

> Add the ttypes field to table checkables.

#### ttype_drop

> Drop the ttypes field from table checkables.

#### ttype_lookup

> Look up the tape type for a specified path.

#### ttype_missing

> Report records missing media type information

#### ttype_populate

> Populate the ttypes field.

#### ttype_table

> Create or drop the tape_types table.


### mpra

### rpt


dashboard_interval -- how frequently to update the dashboard page

email_interval -- how frequently to send e-mail reports

receipients -- list of e-mail addresses to receive e-mail reports

sender -- source address for e-mail reports

subject -- subject line for e-mail reports

alerts -- which section from this file to use for alert definitions


### tcc

## Tests

All unit tests can be run by issuing the following command while sitting
in the git repository:

        $ tests/all

Unit tests for specific components can be run with commands like the following:

        $ tests/UtilTest.py
        $ tests/CrawlConfigTest.py

<a name="wget-config">
## How To Tell `wget` to Use a Proxy Server

* Edit `$HOME/.wgetrc` and add the following lines:

        http_proxy = http://proxy.ccs.ornl.gov:3128
        https_proxy = http://proxy.ccs.ornl.gov:3128
        use_proxy = on

<a name="tunnel-proxy">
## Tunneling

Here's an example tunnel set up for reaching barstow from inside a
firewall:

        Host barstow-tunnel
            User ...
            Hostname ...IP address...
            LocalForward 21717 localhost:22
            ProxyCommand ssh gateway nc %h 22
