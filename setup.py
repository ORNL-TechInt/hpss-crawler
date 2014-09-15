#!/usr/bin/env python

from distutils.core import setup
from distutils.file_util import copy_file
from distutils.sysconfig import get_python_lib
import os

exec(open(os.path.join('hpssic', 'version.py')).read())
sitelib = os.path.join(get_python_lib(), 'hpssic')

# copy_file("README.md", "README")
setup(name='hpssic',
      version=__version__,
      description='HPSS Integrity Crawler',
      author='Tom Barron',
      author_email='tbarron@ornl.gov',
      url='https://github.com/ORNL-TechInt/hpss-crawler',
      scripts=['bin/cv',
               'bin/crawl',
               'bin/rpt',
               'bin/mpra',
               'bin/tcc',
               ],
      packages=['hpssic', 'hpssic/plugins', 'hpssic/test'],
      data_files=[(sitelib, ['README.md', 'crawl.cfg.sample']),
                  ]
      )
