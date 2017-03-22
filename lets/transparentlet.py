# -*- coding: utf-8 -*-
"""
   lets.transparentlet
   ~~~~~~~~~~~~~~~~~~~

   Deprecated.  gevent-1.1 keeps a traceback exactly.

   If you want to just prevent to print an exception by the hub, use
   :mod:`lets.quietlet` instead.

   :copyright: (c) 2013-2017 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from gevent.pool import Group as TransparentGroup

from lets.quietlet import quiet as no_error_handling
from lets.quietlet import Quietlet as Transparentlet


__all__ = ['Transparentlet', 'TransparentGroup', 'no_error_handling']
