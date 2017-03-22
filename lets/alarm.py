# -*- coding: utf-8 -*-
"""
   lets.alarm
   ~~~~~~~~~~

   An event which is awoken up based on time.

   :copyright: (c) 2013-2017 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
import operator
import time as time_

from gevent import get_hub, Timeout
from gevent.event import Event


__all__ = ['Alarm', 'Earliest', 'Latest']


class _Alarm(object):
    """A :class:`gevent.event.AsyncResult`-like class to wait until the final
    set time.
    """

    __slots__ = ('time', 'value', 'timer', 'event')

    #: Implement it to decide to reschedule the awaking time.  It's a function
    #: which takes 2 time arguments.  The first argument is new time to set,
    #: and the second argument is the previously accepted time.  Both arguments
    #: are never ``None``.
    accept = NotImplemented

    def __init__(self):
        self.time = self.value = self.timer = None
        self.event = Event()

    def set(self, time, value=None):
        """Sets the time to awake up.  If the time is not accepted, will be
        ignored and it returns ``False``.  Otherwise, returns ``True``.
        """
        if time is None:
            raise TypeError('use clear() instead of setting none time')
        elif self.time is not None and not self.accept(time, self.time):
            # Not accepted.
            return False
        self._reset(time, value)
        delay = time - time_.time()
        if delay > 0:
            # Set timer to wake up.
            self.timer = get_hub().loop.timer(delay)
            self.timer.start(self.event.set)
        else:
            # Wake up immediately.
            self.event.set()
        return True

    def when(self):
        """When it will be awoken or ``None``."""
        return self.time

    def ready(self):
        """Whether it has been awoken."""
        return self.event.ready()

    def wait(self, timeout=None):
        """Waits until the awaking time.  It returns the time."""
        if self.event.wait(timeout):
            return self.time

    def get(self, block=True, timeout=None):
        """Waits until and gets the awaking time and the value."""
        if not block and not self.ready():
            raise Timeout
        if self.event.wait(timeout):
            return self.time, self.value
        raise Timeout(timeout)

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


class Alarm(_Alarm):
    """An alarm which accepts any time.  Its awaking time will always be reset.
    """

    accept = lambda x, y: True


class Earliest(_Alarm):
    """An alarm which accepts only the earliest time among many times that've
    been set.  Earlier time means that smaller timestamp.
    """

    accept = operator.lt


class Latest(_Alarm):
    """An alarm which accepts only the latest time among many times that've
    been set.  Later time means that bigger timestamp.
    """

    accept = operator.gt
