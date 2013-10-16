# -*- coding: utf-8 -*-
"""
    lets.objectpool
    ~~~~~~~~~~~~~~~

    :copyright: (c) 2013 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
import gevent
import gevent.lock
import gevent.queue


__all__ = ['ObjectPool']


class ObjectPool(object):

    def __init__(self, size, function, *args, **kwargs):
        if size is None:
            self._lock = gevent.lock.DummySemaphore()
        else:
            self._lock = gevent.lock.Semaphore(size)
        self.objects = set()
        self._busy = dict()
        self._queue = gevent.queue.Queue(size)
        self.size = size
        self.function = function
        self.args = args
        self.kwargs = kwargs

    def factory(self):
        return self.function(*self.args, **self.kwargs)

    def available(self):
        return not (self._lock.locked() or self._queue.full())

    def wait_available(self, timeout=None):
        self._lock.wait(timeout)
        self._queue.peek(timeout=timeout)

    def get(self, block=True, timeout=None):
        self._lock.acquire(block, timeout)
        while True:
            try:
                obj = self._queue.get(block=False)
            except gevent.queue.Empty:
                # create new object
                obj = self.factory()
                self.objects.add(obj)
            else:
                if obj not in self.objects:
                    continue
            # found
            break
        self._busy[gevent.getcurrent()] = obj
        return obj

    def release(self, obj):
        if obj not in self.objects:
            return
        assert obj is self._busy.pop(gevent.getcurrent())
        self._queue.put(obj)
        self._lock.release()

    def discard(self, obj):
        self.objects.discard(obj)
        if obj in self._busy.values():
            self._lock.release()

    def __enter__(self):
        return self.get()

    def __exit__(self, *exc_info):
        obj = self._busy[gevent.getcurrent()]
        self.release(obj)
