# -*- coding: utf-8 -*-
"""
   lets.quietlet
   ~~~~~~~~~~~~~

   A quietlet swallows an exception to the hub.  The only way to get an
   exception raised in a quietlet is to call `get()`.

   :copyright: (c) 2013-2016 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from contextlib import contextmanager
import types

import gevent.hub
from greenlet import greenlet

from .utils import greenlet_parent_manager


__all__ = ['Quietlet', 'quiet']


noop = lambda *args: None


class Quietlet(gevent.Greenlet):
    """Saves the actual exc_info when the function raises some exception.  It
    doesn't print exception to stderr.  Consider to use this.  It saves heavy
    traceback object also.
    """

    def _report_error(self, exc_info):
        """Same with :meth:`gevent.Greenlet._report_error` but saves exc_info
        event a traceback object and doesn't call the parent's
        ``handle_error``.
        """
        with quiet(self):
            super(Quietlet, self)._report_error(exc_info)


@greenlet_parent_manager
def quiet(parent):
    """The gevent hub prints greenlet exception to stderr and handles system
    errors.  This context makes the hub do not interest in greenlet errors.
    """
    handle_error = parent.handle_error
    parent.handle_error = noop
    try:
        yield
    finally:
        parent.handle_error = handle_error
