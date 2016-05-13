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
from greenlet import greenlet


__all__ = ['HubWrapper', 'hub_replacer']


class HubWrapper(gevent.hub.Hub):
    """A gevent hub which wraps another hub.  Implement a subclass to override
    partial methods of the underlying hub.  That will be useful to use
    :func:`hub_replacer`.
    """

    def __init__(self, hub):
        greenlet.__init__(self, parent=hub.parent)
        self.hub = hub

    def __getattr__(self, attr):
        return getattr(self.hub, attr)

    @property
    def format_context(self):
        return self.hub.format_context


def hub_replacer(f):
    """Decorates a function to be a context manager which replaces the gevent
    hub in a context.  If the context manager takes a greenlet as the argument,
    the parent of the greenlet will be replaced instead of the current hub.

    The decorated function takes the current hub as the argument.  Then the
    function should yield another hub to replace.

    ::

       class MyHub(HubWrapper):
           pass

       @hub_replacer
       def my_hub(hub):
           yield MyHub(hub)

       # Replace the hub.
       with my_hub():
           g = gevent.spawn(f)
           g.join()

       # Replace the parent of 'g'.
       g = gevent.spawn(f)
       with my_hub(g):
           g.join()

    """
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


def final_next(gen):
    try:
        next(gen)
    except StopIteration:
        pass
    else:
        raise RuntimeError('Generator didn\'t stop')
