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

    __slots__ = ('time', 'value', 'timer', 'event')

    def __init__(self):
        self.time = self.value = self.timer = None
        self.event = Event()

    def set(self, time, value=None):
        """Sets the time to awake up.  If the time is later than the previously
        given time, will be ignored and it returns ``False``.
        """
        if time is None:
            raise TypeError('use clear() instead of setting none time')
        if self.time is not None and self.time <= time:
            # Later time given.
            return False
        self._reset(time, value)
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
        if self.event.wait(timeout):
            return self.time

    def get(self, timeout=None):
        """Waits and gets the earliest awaking value."""
        if self.event.wait(timeout):
            return self.value

    def clear(self):
        """Discards the schedule for awaking."""
        self.event.clear()
        self._reset(None, None)

    def _reset(self, time, value):
        self.time = time
        self.value = value
        if self.timer is not None:
            self.timer.stop()

    def __nonzero__(self):
        return self.time is not None
