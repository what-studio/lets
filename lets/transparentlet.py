# -*- coding: utf-8 -*-
"""
   lets.transparentlet
   ~~~~~~~~~~~~~~~~~~~

   Keeping `exc_info` rather than printing exception.

   :copyright: (c) 2013-2015 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
from contextlib import contextmanager

import gevent
import gevent.pool


__all__ = ['Transparentlet', 'TransparentGroup', 'no_error_handling']


noop = lambda *args, **kwargs: None


@contextmanager
def no_error_handling(hub=None):
    """The gevent hub prints greenlet exception to stderr and handles system
    errors.  This context makes the hub do not interest in greenlet errors.
    """
    if hub is None:
        hub = gevent.hub.get_hub()
    hub.handle_error, handle_error = noop, hub.handle_error
    try:
        yield
    finally:
        hub.handle_error = handle_error


class Transparentlet(gevent.Greenlet):
    """Saves the actual exc_info when the function raises some exception.  It
    doesn't print exception to stderr.  Consider to use this.  It saves heavy
    traceback object also.
    """

    exc_info = None

    def _report_error(self, exc_info):
        """Same with :meth:`gevent.Greenlet._report_error` but saves exc_info
        event a traceback object and doesn't call the parent's
        ``handle_error``.
        """
        self.exc_info = exc_info
        with no_error_handling(self.parent):
            super(Transparentlet, self)._report_error(exc_info)

    def get(self, block=True, timeout=None):
        """Returns the result.  If the function raises an exception, it also
        raises the exception and traceback transparently.
        """
        try:
            return super(Transparentlet, self).get(block, timeout)
        except:
            if self.exc_info is None or self.exc_info[2] is None:
                # killed by outside
                raise
            else:
                raise self.exc_info[0], self.exc_info[1], self.exc_info[2]


class TransparentGroup(gevent.pool.Group):
    """Raises an exception and traceback in the greenlets transparently."""

    greenlet_class = Transparentlet

    def join(self, timeout=None, raise_error=False):
        if not raise_error:
            self._empty_event.wait(timeout=timeout)
            return
        greenlets = self.greenlets.copy()
        self._empty_event.wait(timeout=timeout)
        for greenlet in greenlets:
            if greenlet.ready() and not greenlet.successful():
                greenlet.get(timeout=timeout)
