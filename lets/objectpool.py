# -*- coding: utf-8 -*-
"""
   lets.objectpool
   ~~~~~~~~~~~~~~~

   :copyright: (c) 2013-2016 by Heungsub Lee
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
    """Greenlet-safe object pool."""

    __slots__ = ('objects', 'size', 'function', 'args', 'kwargs',
                 '_lock', '_queue', '_busy')

    def __init__(self, size, function, *args, **kwargs):
        if size is None:
            self._lock = gevent.lock.DummySemaphore()
        else:
            self._lock = gevent.lock.Semaphore(size)
        self._queue = gevent.queue.Queue(size)
        self._busy = set()
        self.objects = set()
        self.size = size
        self.function = function
        self.args = args
        self.kwargs = kwargs

    def factory(self):
        """The factory function."""
        return self.function(*self.args, **self.kwargs)

    def available(self):
        """Whether the pool is available."""
        return not self._lock.locked()

    def wait_available(self, timeout=None):
        """Waits until the pool is available."""
        self._lock.wait(timeout)
        self._queue.peek(timeout=timeout)

    def get(self):
        """Gets an object.  When the pool is available but doesn't have an
        object yet, it creates a new object.  It also acquires the lock.  Don't
        forget release the object got to the pool.
        """
        self._lock.acquire()
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

    def discard(self, obj):
        """Discards the object from the pool."""
        self.objects.discard(obj)
        if obj in self._busy:
            self._busy.remove(obj)
            self._lock.release()

    def reserve(self):
        """Makes a reservation context::

           with db_pool.reserve() as db:
               db.set('foo-key', 'bar-value')
               db.set('egg-key', 'spam-value')
        """
        return ObjectReservation(self)
