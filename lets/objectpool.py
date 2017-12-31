# -*- coding: utf-8 -*-
"""
   lets.objectpool
   ~~~~~~~~~~~~~~~

   :copyright: (c) 2013-2017 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
import gevent.lock
import gevent.queue


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
    :param discard_after: (optional) how many seconds to discard an object
                          after it is released.
    """

    __slots__ = ('objects', 'size', 'factory', 'destroy', 'discard_after',
                 '_lock', '_queue', '_busy')

    def __init__(self, size, factory, destroy=None, discard_after=None):
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
        self.discard_after = discard_after

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
                obj = self.factory()
                self.objects.add(obj)
                break
            if obj in self.objects:
                # found
                break
        self._busy.add(obj)
        return obj

    def release(self, obj):
        """Releases the object to be usable by others."""
        if obj not in self.objects or obj not in self._busy:
            return
        self._busy.remove(obj)
        self._queue.put(obj)
        self._lock.release()
        if self.discard_after is not None:
            gevent.spawn_later(self.discard_after,
                               self._discard_if_not_busy, obj)

    def discard(self, obj):
        """Discards the object from the pool."""
        if self.destroy is not None:
            self.destroy(obj)
        self.objects.discard(obj)
        if obj in self._busy:
            self._busy.remove(obj)
            self._lock.release()

    def _discard_if_not_busy(self, obj):
        if obj not in self._busy and obj in self.objects:
            return self.discard(obj)

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
