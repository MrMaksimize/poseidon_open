#!/usr/bin/python
# File Name: setup.py
# Created:   23-02-2011

import sys

def main(argv):
  """main func"""
  try:
    from setuptools import setup
  except ImportError:
    from distutils.core import setup

  config = {
    'description': 'Poseidon',
    'author': 'Maksim P',
    'url': 'url',
    'download_url': 'url',
    'author_email': 'max@maksimize.com',
    'version': '0.1',
    'install_requires': ['nose'],
    'packages': ['poseidon'],
    'scripts': [],
    'name': 'poseidon'
  }
  setup(**config)

if __name__ == "__main__":
  sys.exit(main(sys.argv))

