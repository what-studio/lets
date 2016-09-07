# -*- coding: utf-8 -*-
"""
   lets.earliest
   ~~~~~~~~~~~~~

   An event which is awoken up based on time.

   :copyright: (c) 2013-2016 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
from time import time as now

from gevent import get_hub
from gevent.event import Event


__all__ = ['Earliest']


class Earliest(object):
    """A :class:`gevent.event.Event`-like class to wait until the earliest time
    among many times that've been set.  So you can set many times but it will
    be wakened up just once.
    """

    __slots__ = ('time', 'timer', 'event')

    def __init__(self):
        self.time = self.timer = None
        self.event = Event()

    def set(self, time):
        """Sets the time to awake up.  If the time is later than the previously
        given time, will be ignored and it returns ``False``.
        """
        if self.time is not None and self.time <= time:
            # Later time given.
            return False
        self._reset(time)
        if time is None:
            # Reset only.
            return False
        delay = time - now()
        if delay > 0:
            # Set timer to wake up.
            self.timer = get_hub().loop.timer(delay)
            self.timer.start(self.event.set)
        else:
            # Wake up immediately.
            self.event.set()
        return True

    def wait(self, timeout=None):
        """Waits until the earliest awaking time.  It returns the time."""
        if not self.event.wait(timeout):
            return None
        return self.time

    def clear(self):
        """Discards the schedule for awaking."""
        self.event.clear()
        self._reset(None)

    def _reset(self, time):
        self.time = time
        if self.timer is not None:
            self.timer.stop()

    def __nonzero__(self):
        return self.time is not None