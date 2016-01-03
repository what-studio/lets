# -*- coding: utf-8 -*-
"""
   lets.quietlet
   ~~~~~~~~~~~~~

   A quietlet swallows an exception to the hub.  The only way to get an
   exception raised in a quietlet is to call `get()`.

   :copyright: (c) 2013-2016 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
from contextlib import contextmanager

import gevent.hub
from greenlet import greenlet


__all__ = ['Quietlet', 'quiet']


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


class QuietHub(greenlet):

    def __init__(self, hub):
        super(QuietHub, self).__init__(None, hub)

    @property
    def hub(self):
        return self.parent

    @property
    def loop(self):
        return self.hub.loop

    def handle_error(self, *args):
        pass


def quiet(greenlet=None):
    """The gevent hub prints greenlet exception to stderr and handles system
    errors.  This context makes the hub do not interest in greenlet errors.
    """
    if greenlet is None:
        return _quiet_hub()
    else:
        return _quiet_greenlet(greenlet)


@contextmanager
def _quiet_hub():
    hub = gevent.hub.get_hub()
    gevent.hub.set_hub(QuietHub(hub))
    try:
        yield
    finally:
        gevent.hub.set_hub(hub)


@contextmanager
def _quiet_greenlet(greenlet):
    parent = greenlet.parent
    greenlet.parent = QuietHub(greenlet.parent)
    try:
        yield
    finally:
        greenlet.parent = parent
