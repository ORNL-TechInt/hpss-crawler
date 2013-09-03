#!/usr/bin/python
"""
crawl - program description
"""

import optparse
import os
import re
import sys
import toolframe

# ------------------------------------------------------------------------------
def crl_example(argv):
    """example - how to set up a function

    Complete description of how the function works.
    """

    print("this is an example")

# ------------------------------------------------------------------------------
def crl_start(argv):
    """start - start the crawler running as a daemon if it is not running

    usage: crawl start
    """

# ------------------------------------------------------------------------------
def crl_status(argv):
    """status - report whether the crawler is running or not

    usage: crawl stop
    """

# ------------------------------------------------------------------------------
def crl_stop(argv):
    """stop - shut down the crawler daemon if it is running

    usage: crawl stop
    """

# ------------------------------------------------------------------------------
def daemonize():
    """
    Spawn a child to run in the background and run plugins as appropriate
    based on the configuration.
    """
    
# ------------------------------------------------------------------------------
toolframe.tf_launch("crl")
