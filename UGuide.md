<head><title>HPSSIC User Guide</title></head>

# HPSS Integrity Crawler User Guide

## Goals

The goals of the HPSS Integrity Crawler are

* To increase confidence in the reliability of HPSS at ORNL.
* To identify potential issues as they arise so they can be addressed
  and mitigated.

## Running the Crawler

To start the crawler, run

        crawl start

Optionally, you can specify the configuration and log file:

        crawl start --cfg <filename> --log <filename>

If the configuration file is not specified on the command line, the
crawler looks for environment variable $CRAWL_CONF and uses the file
it names, if it is set. If not, it looks for file 'crawl.cfg' in the
current working directory.

If the log file is not specified on the command line, the crawler
looks for environment variable $CRAWL_LOG and uses the file it names,
if it is set. If not, it writes to /var/log/crawl.log if running as
root or /tmp/crawl.log if running as some other user (normally, only
root can write files in /var/log).

To shut down the crawler, run

        crawl stop

To check whether the crawler is running or not, do

        crawl status

For other crawl sub-commands, consult the [Reference
Manual](http://users.nccs.gov/~tpb/hpssic/ReferenceManual.html).

## Plugins

The HPSS Integrity Crawler (HPSSIC) operates by loading and
periodically calling a collection of plugin modules. This modular
approach is intended to make it easy to add new kinds of integrity
checking to the system. A potential drawback of this approach is that
each plugin is expected to break its task down into small pieces that
can be done iteratively over time, rather than attempting to, for
example, retrieve a list of all files in the archive in a single
operation.

### Checksum Verifier

The Checksum Verifier (CV) selects files from the archive to build a
randomized, representative sample of the archive population, based on
COS. It uses [hsi's](http://www.mgleicher.us/GEL/hsi/) `hashcreate`
and `hashverify` commands to capture checksums for specific files and
subsequently check them to make sure the file data has not changed.

### Tape Copy Checker

The Tape Copy Checker (TCC) scans the whole archive, checking whether
each file is stored in the appropriate number of copies on tape, based
on it Class of Service (COS).




## Writing a New Plugin
