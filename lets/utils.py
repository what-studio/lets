# -*- coding: utf-8 -*-
"""
   lets.utils
   ~~~~~~~~~~

   :copyright: (c) 2013-2016 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
import contextlib
import functools

import gevent.hub


__all__ = ['greenlet_parent_manager']


def greenlet_parent_manager(f):
    @contextlib.contextmanager
    @functools.wraps(f)
    def manage_greenlet_parent(greenlet=None):
        if greenlet is None:
            parent = gevent.hub.get_hub()
        else:
            parent = greenlet.parent
        gen = f(parent)
        try:
            yield next(gen)
        finally:
            try:
                next(gen)
            except StopIteration:
                pass
            else:
                raise TypeError('greenlet_parent_manager must yield 1 time')
    return manage_greenlet_parent
