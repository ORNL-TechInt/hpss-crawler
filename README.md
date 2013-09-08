
# HPSS Integrity Crawler

Code for the HPSS Integrity Crawler resides in a git repo on the TechInt github
organization:

   https://github.com/ORNL-TechInt/hpss-crawler

## Goals of the program

A white paper motivating the HIC is available in Google documents here:

   http://tinyurl.com/n2xlof3

The high level design for the HIC is documented in a Google document here:

   http://tinyurl.com/n5b4cxy

These documents are owned by Tom Barron. Please contact him for access if
needed.

## Running Tests

   $ crawl.py [-v]

Test results are written to stdout and also logged in crawl_test.log so a
history is available.


## Git Repo Branches

The git repo has the following branches used for the indicated intentions

   devel - current development
   master - last stable release

   post - this branch is historic and the material from it has been merged to
          dev. it can be deleted

   prehistory - This branch contains the history of the work from before the
                github repo was established. It is fully incorporated into
                devel, however deleting this branch would lose the steps from
                project inception to when the github repo was set up.


## Configuration

The configuration file is determined by the -c/--cfg option on the command line,
the environment variable $CRAWL_CONF, and the default of 'crawl.cfg' in that
order.


## Log file

The log file is determined by the -l/--logpath command line option, the
'logpath' setting in the 'crawler' section of the configuration file, the
environment variable $CRAWL_LOG, or the default of '/var/log/crawl.log', in that
order.
