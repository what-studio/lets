# -*- coding: utf-8 -*-
"""
   lets
   ~~~~

   Utilities for gevent_.

   .. _gevent: http://gevent.org/

   :copyright: (c) 2013-2017 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from lets.__about__ import __version__  # noqa
from lets.alarm import Alarm, Earliest, Latest
from lets.atomicity import atomic
from lets.jobqueue import JobQueue
from lets.objectpool import ObjectPool
from lets.processlet import (
    pipe, ProcessExit, Processlet, ProcessLocal, ProcessPool)
from lets.quietlet import Quietlet
from lets.slavery import (
    join_slaves, link_partner, link_slave, MasterGreenletExit, spawn_partner,
    spawn_partner_later, spawn_slave, spawn_slave_later)
from lets.transparentlet import TransparentGroup, Transparentlet


__all__ = ['Alarm', 'atomic', 'Earliest', 'JobQueue', 'join_slaves', 'Latest',
           'link_partner', 'link_slave', 'MasterGreenletExit', 'ObjectPool',
           'pipe', 'Processlet', 'ProcessLocal', 'ProcessExit', 'ProcessPool',
           'Quietlet', 'spawn_partner', 'spawn_partner_later', 'spawn_slave',
           'spawn_slave_later', 'Transparentlet', 'TransparentGroup']
