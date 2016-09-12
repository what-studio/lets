# -*- coding: utf-8 -*-
"""
   lets.atomicity
   ~~~~~~~~~~~~~~

   :copyright: (c) 2013-2016 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from contextlib import contextmanager

import gevent


__all__ = ['atomic']


ATOMICITY_ERROR = AssertionError('impossible to call blocking '
                                 'function on the atomic hub')


def atomicity_error():
    raise ATOMICITY_ERROR


@contextmanager
def atomic():
    """Raises an :exc:`AssertionError` when a gevent blocking function called
    in the context.
    """
    greenlet = gevent.getcurrent()
    switch_out = getattr(greenlet, 'switch_out', None)
    greenlet.switch_out = atomicity_error
    try:
        yield
    finally:
        if switch_out is None:
            del greenlet.switch_out
        else:
            greenlet.switch_out = switch_out
