#!/usr/bin/env python

from setuptools import setup
import os

exec(open(os.path.join('hpssic', 'version.py')).read())

setup(name='hpssic',
      version=__version__,
      description='HPSS Integrity Crawler',
      author='Tom Barron',
      author_email='tbarron@ornl.gov',
      url='https://github.com/ORNL-TechInt/hpss-crawler',
      scripts=['bin/cv',
               'bin/crawl',
               'bin/html',
               'bin/rpt',
               'bin/mpra',
               'bin/tcc',
               ],
      packages=['hpssic', 'hpssic/plugins', 'hpssic/test'],
      )
