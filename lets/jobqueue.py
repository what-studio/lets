# -*- coding: utf-8 -*-
"""
    lets.jobqueue
    ~~~~~~~~~~~~~

    :copyright: (c) 2013-2015 by Heungsub Lee
    :license: BSD, see LICENSE for more details.

"""
import gevent
import gevent.pool
import gevent.queue


__all__ = ['JobQueue']


class JobQueue(object):
    """Pool-like job queue.

    :param size: the queue size.
    :param workers: the size of the worker pool.  (default: 1)
    """

    __slots__ = ['queue', 'worker_pool']

    def __init__(self, size=None, workers=1):
        self.queue = gevent.queue.JoinableQueue(size)
        self.worker_pool = gevent.pool.Pool(workers)

    def put(self, greenlet, block=True, timeout=None):
        """Enqueues a greenlet and spawns a worker.

        :param greenlet: a job greenlet.  It must not be started.
        :param block: block the current greenlet if the job queue is full.
        :param timeout: how long it blocks.

        :returns: a worker greenlet if it is spawned.
        :raises ValueError: the greenlet is already started.
        """
        if greenlet.started:
            raise ValueError('Job greenlet is already started')
        self.queue.put(greenlet, block=block, timeout=timeout)
        # spawn a worker if the pool is available.
        self.worker_pool.join(0)
        if not self.worker_pool.full():
            return self.worker_pool.spawn(self.work)

    def work(self):
        """Consumes queued jobs.  It would be spawned when a job is enqueued.
        """
        def greenlets():
            while not self.queue.empty():
                greenlet = self.queue.get(block=False)
                greenlet.start()
                yield greenlet
        try:
            for greenlet in greenlets():
                greenlet.join()
                self.queue.task_done()
        except BaseException as exc:
            # kill remaining jobs.
            greenlet.kill(exc)
            for greenlet in greenlets():
                greenlet.kill(exc)
            raise
        finally:
            assert self.queue.empty()

    # methods from queue.

    def qsize(self):
        return self.queue.qsize()

    def empty(self):
        return self.queue.empty()

    def full(self):
        return self.queue.full()

    # methods from worker pool.

    def join(self, timeout=None, raise_error=False):
        self.worker_pool.join(timeout=timeout, raise_error=raise_error)

    def kill(self, exception=gevent.GreenletExit, block=True, timeout=None):
        self.worker_pool.join(0)
        self.worker_pool.kill(exception, block=block, timeout=timeout)
