#!/usr/bin/env python

from setuptools import setup

setup(name='tap-pipefy',
      version='0.0.6',
      description='Singer.io tap for extracting data from the Pipefy API',
      author='Pedro Machado',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_pipefy'],
      install_requires=[
          'singer-python>=5.2',
          'requests>=2.12',
          'pendulum==1.2.0'
      ],
      entry_points='''
          [console_scripts]
          tap-pipefy=tap_pipefy:main
      ''',
      packages=['tap_pipefy'],
      package_data={
          'tap_pipefy/schemas': [
              'members.json',
              'pipes.json',
              'cards.json',
              'tables.json'
          ],
      },
      include_package_data=True,
      )
