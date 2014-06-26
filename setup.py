#!/usr/bin/env python

from distutils.core import setup
from distutils.file_util import copy_file

# copy_file("README.md", "README")
setup(name='hpssic',
      version='2014.0725dev',
      description='HPSS Integrity Crawler',
      author='Tom Barron',
      author_email='tbarron@ornl.gov',
      url='https://github.com/ORNL-TechInt/hpss-crawler',
      scripts=['bin/ztool.py',
               'bin/cv',
               'bin/crawl',
               'bin/rpt',
               'bin/mpra',
               'bin/tcc',
               ],
      packages=['hpssic'],
      )
