
# HPSS Integrity Crawler Reference Manual

## Installation

### Prerequisites

* Python 2.6 

* MySQL databse interface
    yum install MySQL-python

* ibm-db

        yum install python-devel.x86_64
        curl https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py -O ez_setup.py
        python ez_setup.py

        curl https://raw.github.com/pypa/pip/master/contrib/get-pip.py -O get-pip.py
        python get-pip.py

        export IBM_DB_HOME=/opt/ibm/db2/V10.5
        pip install ibm_db

        echo $IBM_DB_HOME/lib64 > /etc/ld.so.conf.d/db2-x86_64.conf
        ldconfig

## Configuration

The default configuration filename is 'crawl.cfg' in the current
working directory. Non-default configuration files can be used by
specifying environment variable CRAWL_CONF or by specifying the file
path on the crawl command line. For example,

        export CRAWL_CONF=/var/hpssic/crawl.cfg
        crawl start

Or

        crawl start --cfg /var/hpssic/crawl.cfg

<a name="ConfigSyntax">
### Configuration Syntax

## Operation

### Alerts

When the Crawler finds an issue to report, it does so through an
alert. The following types of alerts are defined:

* E-mail Alerts
* Log File Alerts
* Shell Alerts

#### E-mail Alerts

If e-mail alerts are configured, the configuration file contains a
list of e-mail addresses to receive the alert e-mails. See the
[Configuration Syntax](#ConfigSyntax) section for more detail.

For an e-mail alert, the addresses to receive the e-mail are specified
in the configuration file. See the Configuration Syntax for details.

#### Log File Alerts

For log file alerts, 

#### Shell Alerts

### Log Files

The Integrity Crawler reports events to a log file. The default log
file is either /var/log/crawl.log (if the Crawler runs as root) or
/tmp/crawl.log (if the Crawler runs as a non-root user). The log file can be placed somewhere else either by setting environment variable CRAWL_LOG or by specifying the file path on the crawl command line. For example,

        export CRAWL_LOG=/var/hpssic/crawl.log
        crawl start

Or

        crawl start --log /var/hpssic/crawl.log
