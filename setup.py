#!/usr/bin/env python

from setuptools import setup

setup(name='tap-gmail-csv',
      version='0.0.1',
      description='Singer.io tap for extracting CSV files from GMail',
      author='MOOPrint',
      url='http://moo.com',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_gmail_csv'],
      install_requires=[
          'google-api-python-client==1.7.11',
          'singer-python==1.5.0',
          'voluptuous==0.10.5',
          'xlrd==1.0.0',
      ],
      entry_points='''
          [console_scripts]
          tap-gmail-csv=tap_gmail_csv:main
      ''',
      packages=['tap_gmail_csv'])
