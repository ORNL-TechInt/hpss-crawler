<head><title>HPSSIC Reference Manual</title></head>

# HPSS Integrity Crawler Reference Manual

## Installation

### Prerequisites

NOTE: Some of these steps may not be necessary if your system already
has components installed.

* git 1.7.1 or higher

        yum install git.x86_64

* Python 2.6.6 or higher

        yum install python
        yum install python-devel.x86_64

* Python packaging tools 

   1. If the current host does not have Internet access, you will
      need to [tell wget to use a proxy server](#wget-config).

   2. `ez_setup.py` and `get-pip.py` use curl under the covers. That's why
      the `https_proxy` environment variable is needed -- to tell curl
      to use the proxy. We use wget for fetching `ez_setup.py` and
      `get-pip.py` because wget has the --no-check-certificate option,
      which is needed for talking to raw.github.com. I could not get
      curl's corresponding option to work.

        wget https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py
        wget --no-check-certificate https://raw.github.com/pypa/pip/master/contrib/get-pip.py

        export https_proxy=https://proxy.ccs.ornl.gov:3128
        python ez_setup.py
        python get-pip.py

* MySQL

        yum install mysql

* MySQL/Python database interface 1.2.3

        yum install MySQL-python

* DB2 client

        [Set up whatever tunnels](#tunnel) are required to reach
        barstow, then fetch the DB2 package

                scp [-P port] barstow:/hpss_prereq/db2_10.5/DB2_Svr_V10.5_Linux_x86-64.tar.gz . 
        
        Unpack

                tar zxf DB2_Svr_V10.5_Linux_x86-64.tar.gz

        Install

                cd server
                ./db2_install
                [specify "CLIENT" at product prompt]
                [confirm default install location when prompted]

* DB2 client support on the server instance. On DB2 server:

        Added "db2c_hpssdb   50000/tcp" to /etc/services.
        db2 update database manager configuration using svcename db2c_hpssdb
        db2set DB2COMM=tcpip
        db2stop
        db2start

    to verify:

        db2 get dbm config | grep SVCENAME
        db2set -all DB2COMM
        
* Optionally, create a read-only user on the server instance:

        groupadd -g 9903 hpssic
        useradd -u 9903 -g hpssic -m -d /home/hpssic hpssic
        passwd hpssic

        For each database (HCFG, HSUBSYS1, etc.), do the following to
        grant select access on all the HPSS tables.

        db2 connect to DATABASE
        db2 -x "select 'grant select on table ' || rtrim(tabschema) || '.' ||
            rtrim(tabname) || ' to user hpssic' from syscat.tables 
            where tabschema = 'HPSS'"

        To execute the output of the above command, append '| db2 +p -tv' 
        or pipe the output to a temp file and then pipe that to 
        'db2 +p -tv'
    
        db2 connect reset

        If you would prefer to only grant the user access to the
        tables the crawler actually accesses, those tables are show in
        the following list. You would need to issue the following
        command on each of these tables.

          GRANT SELECT ON TABLE _tablename_ TO USER hpssic

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


* DB2 client support on the client machine

        root: groupadd -g 9903 hpssic
        root: useradd  -u 9903 -g hpssic -m -d /home/hpssic hpssic
        root: passwd hpssic
        root: db2icrt -s client hpssic
        hpssc: . /home/hpssic/sqllib/db2profile

* ibm-db

        # install the ibm_db python module
        export IBM_DB_HOME=/opt/ibm/db2/V10.5
        pip install ibm_db

        # tell the link loader where to find DB2 libraries
        echo $IBM_DB_HOME/lib64 > /etc/ld.so.conf.d/db2-x86_64.conf
        ldconfig

## Install hpssic

        $ git clone ssh://USERNAME@gerrit.ccs.ornl.gov:29418/hpss/hpssic
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

        export CRAWL_CONF=/var/hpssic/crawl.cfg
        crawl start

Or

        crawl start --cfg /var/hpssic/crawl.cfg

If the configuration file is changed while the crawler is running, the
crawler will detect this and reload its configuration, so
configuration changes usually will not require restarting the crawler
(unless a configuration update happens to tickle a bug that causes the
crawler to crash, for example).

<a name="ConfigSyntax">
### Configuration Syntax

The configuration file follows the format defined by the Python
ConfigParser module, which is similar to the ancient .ini file format
(http://en.wikipedia.org/wiki/INI_file). The file is divided into
sections. Each section begins with a section name in square brackets.
For example,

        [crawler]

The 'crawler' section is required and contains the following items:

        plugin-dir = <dirname>
            Indicates the location of the plugin directory

        logpath = <file path>
            Where log records will be written

        logsize = <number>
            The maximum size of the log file before it will be closed
            and renamed and a new one will be opened.

        logmax = <number>
            The number of log files to be kept around. Once this many
            log files have accumulated, when a new one is created, the
            oldest will be deleted.

        heartbeat = <time interval>
            The crawler will write heartbeat records to the log file
            this frequently so there will be an indication whether it
            is still making progress.

        verbose = <boolean>
            If this is true, more information will be logged

        plugins = <space separated list>

        hsi_timout = <time interval>

Here's an example of what the crawler section of a configuration file
might look like:

        [crawler]
        plugin-dir = ./plugins
        logpath    = hpss_crawl.log
        logsize    = 5mb
        logmax     = 5
        heartbeat = 10s
        verbose = false
        plugins = checksum-verifier
        hsi_timeout = 150


#### Time Intervals

Time intervals are specified using a number and an optional unit. If
no unit is specified, the interval is taken to be the number of
seconds indicated by the number. The following units are recognized:

* seconds: s, sec, second, seconds
* minutes: m, min, minute, minutes
* hours:   h, hr, hour, hours
* days:    d, day, days
* weeks:   w, week, weeks
* months:  month, months
* years:   y, year, years

#### Booleans

The value True can be indicated by the strings "1", "yes", "true", or
"on". Other values will be translated as False.

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
notable condition is discovered in the archive. Currently, three types
of alerts are supported:

        [alerts]
        log           = %s
        email         = tom.barron, tusculum@gmail.com
        shell         = touch foobar

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


### dbi Section

The dbi section tells the crawler which database (MySQL or SQLite) to
use for storing information gathered by the various plugins. It
contains the following options:

        [dbi]
        dbtype = sqlite
        dbname = <filename>

        [dbi]
        dbtype = mysql
        host = <mysql server host>
        dbname = <database name>
        username = <database user name>
        password = <base64-encoded password>

## Operation

### Starting, Stopping, Checking Status

To start the crawler

        crawl start [--cfg <config file>] [--log <log file>]

To check the status of the crawler

        crawl status

To shut down the crawler

        crawl stop


### Log Files

The Integrity Crawler reports events to a log file. The default log
file is either /var/log/crawl.log (if the Crawler runs as root) or
/tmp/crawl.log (if the Crawler runs as a non-root user). The log file can be placed somewhere else either by setting environment variable CRAWL_LOG or by specifying the file path on the crawl command line. For example,

        export CRAWL_LOG=/var/hpssic/crawl.log
        crawl start

Or

        crawl start --log /var/hpssic/crawl.log

## Other Subfunctions

Besides start, stop, and status, the other subfunctions of crawl are:

### cfgdump

Load the configuration file and dump its contents to stdout. This is
an easy way to verify that there are no syntax errors in the
configuration file.

### cleanup

Running the unit tests for the software will normally remove any test
data created for the test. If some is left behind, this subfunction
will remove it.

### cvreport

Report the current state of the checksum verifier. This produces a
report showing the size of the total population and the size of the
representative sample based on COS in the checkables and dimension
tables. These values should agree.

        $ crawl cvreport
                        Population      Sample
             checkables        354         120
              dimension        354         120 
                cvstats                    120 
        -----
            Name Category ==Population=== ====Sample=====
             cos     5081     157   44.35      61   50.83
             cos     6001      80   22.60      26   21.67
             cos     6002      54   15.25      14   11.67
             cos     6003      29    8.19       8    6.67
             cos     6004       2    0.56       1    0.83
             cos     6054      23    6.50       6    5.00
             cos     6056       3    0.85       1    0.83
             cos     6057       6    1.69       3    2.50
                    Total     354             120


The second part of the report shows the population and sample size by
COS value. The totals should agree with the numbers from the first
part of the report, and the percentage that each category value
represents of the population and sample should approximately agree.

### dbdrop

This subfunction drops can be used to drop tables from the MySQL or
sqlite database used by the Checksum Verifier. This subfunction does
not operate on DB2 databases and therefore cannot be used to drop
tables from the HPSS database.

Dropping the Checksum Verifier tables is an easy way to reset the
Checksum Verifier and have it start from scratch building its
representative sample of the file population in the archive.

### fire

This subfunction can be used to test a specific plugin by firing it
one time.

### log

This subfunction can be used to write a message to the log file.

### help

With no arguments, this subfunction reports a list of all of the
crawler's subfunctions. If the name of a subfunction is passed as an
argument, the usage notes for that subfunction will be displayed.

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
