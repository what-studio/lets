# -*- coding: utf-8 -*-
"""
   lets
   ~~~~

   Utilities for gevent_.

   .. _gevent: http://gevent.org/

   :copyright: (c) 2013-2016 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from .__about__ import __version__  # noqa
from .jobqueue import JobQueue
from .objectpool import ObjectPool
from .processlet import ProcessExit, Processlet, ProcessPool
from .quietlet import Quietlet
from .slavery import (
    join_slaves, link_partner, link_slave, MasterGreenletExit, spawn_partner,
    spawn_partner_later, spawn_slave, spawn_slave_later)
from .transparentlet import TransparentGroup, Transparentlet


__all__ = ['JobQueue', 'join_slaves', 'link_partner', 'link_slave',
           'MasterGreenletExit', 'ObjectPool', 'Processlet', 'ProcessExit',
           'ProcessPool', 'spawn_partner', 'spawn_partner_later',
           'spawn_slave', 'spawn_slave_later', 'Quietlet', 'Transparentlet',
           'TransparentGroup']
