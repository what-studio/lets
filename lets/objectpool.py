# -*- coding: utf-8 -*-
"""
   lets.objectpool
   ~~~~~~~~~~~~~~~

   :copyright: (c) 2013-2018 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
import sys

import gevent.lock
import gevent.queue
from six import reraise


__all__ = ['ObjectPool']


class ObjectReservation(object):

    def __init__(self, pool):
        self.pool = pool
        self.obj = None

    def __enter__(self):
        self.obj = self.pool.get()
        return self.obj

    def __exit__(self, *exc_info):
        self.pool.release(self.obj)


class ObjectPool(object):
    """Greenlet-safe object pool.

    :param size: the maximum number of objects.  ``None`` means that
                 unlimited.
    :param factory: the function which makes a new object.
    :param destroy: (optional) the function which destroys an object.
                    It is used to discard an object from the pool by
                    :meth:`discard`.
    :param discard_later: (optional) how many seconds to discard an object
                          after it is released.
    """

    __slots__ = ('objects', 'size', 'factory', 'destroy', 'discard_later',
                 '_lock', '_queue', '_busy', '_discard_greenlets')

    def __init__(self, size, factory, destroy=None, discard_later=None):
        if size is None:
            self._lock = gevent.lock.DummySemaphore()
        else:
            self._lock = gevent.lock.Semaphore(size)
        self._queue = gevent.queue.Queue(size)
        self._busy = set()
        self.objects = set()
        self.size = size
        self.factory = factory
        self.destroy = destroy
        self.discard_later = discard_later
        self._discard_greenlets = {}

    def count(self):
        """The number of objects in the pool."""
        return len(self.objects)

    def count_busy(self):
        """The number of busy objects in the pool."""
        return len(self._busy)

    def count_free(self):
        """The number of free objects in the pool."""
        return len(self.objects) - len(self._busy)

    def available(self):
        """Whether the pool is available."""
        return not self._lock.locked()

    def wait_available(self, timeout=None):
        """Waits until the pool is available."""
        self._lock.wait(timeout)
        self._queue.peek(timeout=timeout)

    def get(self, block=True, timeout=None):
        """Gets an object.  When the pool is available but doesn't have an
        object yet, it creates a new object.  It also acquires the lock.  Don't
        forget release the object got to the pool.
        """
        if not self._lock.acquire(block, timeout):
            raise gevent.Timeout(timeout)
        while True:
            try:
                obj = self._queue.get(block=False)
            except gevent.queue.Empty:
                # create new object
                try:
                    obj = self.factory()
                except:
                    exc_info = sys.exc_info()
                    self._lock.release()
                    reraise(*exc_info)
                else:
                    self.objects.add(obj)
                    break
            if obj in self.objects:
                # found
                break
        self._kill_discard(obj)
        self._busy.add(obj)
        return obj

    def release(self, obj):
        """Releases the object to be usable by others."""
        if obj not in self.objects or obj not in self._busy:
            return
        self._busy.remove(obj)
        self._queue.put(obj)
        self._lock.release()
        self._spawn_discard(obj)

    def _spawn_discard(self, obj):
        if self.discard_later is None:
            return
        if self.discard_later <= 0:
            # discard immediately
            self.discard(obj)
        else:
            # discard some seconds later
            assert obj not in self._discard_greenlets
            f = self._scheduled_discard
            g = gevent.spawn_later(self.discard_later, f, obj)
            self._discard_greenlets[obj] = g

    def _kill_discard(self, obj):
        try:
            g = self._discard_greenlets.pop(obj)
        except KeyError:
            pass
        else:
            g.kill(block=False)

    def _scheduled_discard(self, obj):
        self._discard_greenlets.pop(obj)
        if obj not in self._busy and obj in self.objects:
            return self.discard(obj)

    def discard(self, obj):
        """Discards the object from the pool."""
        self.objects.discard(obj)
        if self.destroy is not None:
            self.destroy(obj)
        if obj in self._busy:
            self._busy.remove(obj)
            self._lock.release()

    def clear(self):
        """Discards all objects in the pool."""
        while True:
            try:
                obj = self._queue.get(block=False)
            except gevent.queue.Empty:
                break
            # assert obj not in self._busy
            self.discard(obj)

    def reserve(self):
        """Makes a reservation context::

           with db_pool.reserve() as db:
               db.set('foo-key', 'bar-value')
               db.set('egg-key', 'spam-value')
        """
        return ObjectReservation(self)
