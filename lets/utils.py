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


__all__ = ['hub_replacer']


def final_next(gen):
    try:
        next(gen)
    except StopIteration:
        pass
    else:
        raise RuntimeError('Generator didn\'t stop')


def hub_replacer(f):
    @contextlib.contextmanager
    @functools.wraps(f)
    def replace_hub(greenlet=None):
        # Get the current hub.
        if greenlet is None or greenlet.parent is None:
            hub = gevent.hub.get_hub()
        else:
            hub = greenlet.parent
        # How set a new hub.
        if greenlet is None:
            set_hub = gevent.hub.set_hub
        else:
            set_hub = lambda hub: setattr(greenlet, 'parent', hub)
        gen = f(hub)
        new_hub = next(gen)
        set_hub(new_hub)
        try:
            yield new_hub
        finally:
            set_hub(hub)
            final_next(gen)
    return replace_hub
