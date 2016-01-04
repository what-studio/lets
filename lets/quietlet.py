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

import gevent

from .utils import hub_replacer, HubWrapper


__all__ = ['Quietlet', 'quiet']


class Quietlet(gevent.Greenlet):
    """A greenlet but it doesn't report exceptions to the hub."""

    def _report_error(self, exc_info):
        with quiet(self):
            super(Quietlet, self)._report_error(exc_info)


class QuietHub(HubWrapper):

    def handle_error(self, *args):
        pass


@hub_replacer
def quiet(hub):
    """Makes a context which suppresses the greenlet exception handler.

    The gevent hub handles exceptions from a greenlet by default.  If the
    exception is a system error, it will be propagated to the main greenlet.
    Otherwise, the exception will be printed to stderr.

    """
    yield QuietHub(hub)
