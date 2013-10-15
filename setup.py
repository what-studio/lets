# -*- coding: utf-8 -*-
"""
Lets
~~~~

Several ``gevent.Greenlet`` subclasses.

* ``Processlet``
* ``Transparentlet``

.. sourcecode:: python

   import bcrypt
   import gevent
   from lets import Processlet

   # bcrypt.hashpw is very heavy cpu-bound task. it can spend a few seconds.
   def hash_password(password, salt=bcrypt.gensalt()):
       return bcrypt.hashpw(str(password), salt)

   def tictoc(delay=0.1):
       while True:
           print '.'
           gevent.sleep(delay)

   gevent.spawn(tictoc)

   # Greenlet, tictoc pauses for a few seconds
   glet = gevent.spawn(hash_password, 'my_password')
   hash = glet.get()

   # Processlet, tictoc never pauses
   proc = Processlet.spawn(hash_password, 'my_password')
   hash = proc.get()

Links
`````

* `GitHub repository <http://github.com/sublee/lets>`_
* `development version
  <http://github.com/sublee/lets/zipball/master#egg=lets-dev>`_

"""
from __future__ import with_statement
import multiprocessing  # prevent an error in atexit._run_exitfuncs
import re
from setuptools import setup
from setuptools.command.test import test as TestCommand
import sys


# detect the current version
with open('lets/__init__.py') as f:
    version = re.search(r'__version__\s*=\s*\'(.+?)\'', f.read()).group(1)
assert version


# use pytest instead
class PyTest(TestCommand):

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        errno = pytest.main(self.test_args)
        sys.exit(errno)


setup(
    name='lets',
    version=version,
    license='BSD',
    author='Heungsub Lee',
    author_email=re.sub('((sub).)(.*)', r'\2@\1.\3', 'sublee'),
    url='https://github.com/sublee/lets',
    description='Several greenlet subclasses',
    long_description=__doc__,
    platforms='any',
    packages=['lets'],
    zip_safe=False,  # i don't like egg
    classifiers=['Development Status :: 4 - Beta',
                 'Intended Audience :: Developers',
                 'License :: OSI Approved :: BSD License',
                 'Operating System :: OS Independent',
                 'Programming Language :: Python',
                 'Programming Language :: Python :: 2',
                 'Programming Language :: Python :: 2.7',
                 'Programming Language :: Python :: Implementation :: CPython',
                 'Topic :: Software Development'],
    # gevent 1.0 is currently not available at PyPI
    # install_requires=['gevent>=1.0', 'gipc'],
    install_requires=['gipc'],
    tests_require=['pytest'],
    cmdclass={'test': PyTest},
)
