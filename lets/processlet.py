# -*- coding: utf-8 -*-
"""
    lets.processlet
    ~~~~~~~~~~~~~~~

    Maximizing multi-core use in gevent environment.

    :class:`Processlet` is a subclass of :class:`gevent.Greenlet` but focuses
    to CPU-bound tasks not I/O-bound. Never give up high concurrency gevent
    offered.

    .. sourcecode:: python

       import bcrypt
       import gevent
       from lets import Processlet
       
       # bcrypt.hashpw is very heavy cpu-bound task. it can spend a few seconds.
       def hash_password(password, salt=bcrypt.gensalt()):
           return bcrypt.hashpw(str(password), salt)
       
       def tictoc(delay=0.1):
           while True:
               print '.'
               gevent.sleep(delay)
       
       gevent.spawn(tictoc)
       
       # Greenlet, tictoc pauses for a few seconds
       glet = gevent.spawn(hash_password, 'my_password')
       hash = glet.get()
       
       # Processlet, tictoc never pauses
       proc = Processlet.spawn(hash_password, 'my_password')
       hash = proc.get()

    :copyright: (c) 2013 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
from __future__ import absolute_import
import os
import warnings

import gevent
import gevent.pool
import gevent.queue
import gipc

from .objectpool import ObjectPool


__all__ = ['ProcessExit', 'Processlet', 'ProcessPool']


class ProcessExit(BaseException):
    """Originally, :exc:`SystemExit` kills all independent gevent waitings.
    To prevent killing the current process, :class:`Processlet` replaces
    :exc:`SystemExit` from child process with this exception.
    """

    def __init__(self, code):
        self.code = code
        super(ProcessExit, self).__init__(code)


def call_and_put(function, args, kwargs, pipe):
    """Calls the function and sends result to the pipe."""
    try:
        value = function(*args, **kwargs)
    except gevent.GreenletExit as exc:
        pipe.put((False, exc))
    except SystemExit as exc:
        pipe.put((False, exc))
        raise
    except BaseException as exc:
        pipe.put((False, exc))
        raise SystemExit(1)
    else:
        pipe.put((True, value))
    raise SystemExit(0)


def get_and_kill(pipe, greenlet):
    """Kills the greenlet if the parent sends an exception."""
    try:
        successful, exc = pipe.get()
    except EOFError as exc:
        pass
    else:
        assert not successful
    greenlet.kill(exc, block=False)


class Processlet(gevent.Greenlet):
    """Calls a function in child process."""

    function = None
    exit_code = None

    def __init__(self, function=None, *args, **kwargs):
        super(Processlet, self).__init__(None, *args, **kwargs)
        self.function = function

    @property
    def pid(self):
        """The pid of the child process."""
        self.join(0)
        try:
            return self._pid
        except AttributeError:
            return None

    @pid.setter
    def pid(self, pid):
        assert self.pid is None
        self._pid = pid

    def send(self, signo, block=True, timeout=None):
        """Sends a signal to the child process."""
        self.join(0)
        os.kill(self.pid, signo)
        if block:
            self.join(timeout)

    def _run(self, *args, **kwargs):
        """Opens pipe and starts child process to run :meth:`_run_child`. Then
        it waits for the child process done.
        """
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            pipe_pair = gipc.pipe(duplex=True)
        with pipe_pair as (p_pipe, c_pipe):
            args = (c_pipe,) + args
            proc = gipc.start_process(self._run_child, args, kwargs)
            self.pid = proc.pid
            successful, value = True, None
            try:
                successful, value = p_pipe.get()
            except EOFError:
                proc.join()
                if proc.exitcode:
                    successful, value = False, SystemExit(proc.exitcode)
            except BaseException as exc:
                p_pipe.put((False, exc))
                successful, value = p_pipe.get()
            proc.join()  # wait until the child process exits
            self.exit_code = proc.exitcode
            if successful:
                return value
            # failure
            if isinstance(value, SystemExit):
                raise ProcessExit(value.code)
            else:
                raise value

    def _run_child(self, *args, **kwargs):
        """The target of child process. It puts result to the pipe when it
        done.
        """
        pipe, args = args[0], args[1:]
        greenlet = gevent.spawn(
            call_and_put, self.function, args, kwargs, pipe)
        gevent.spawn(get_and_kill, pipe, greenlet)
        greenlet.join()


class ProcessPool(gevent.pool.Pool):
    """Recyclable worker :class:`Processlet` pool. It should be finalized with
    :meth:`kill` to close all child processes.
    """

    def __init__(self, size=None):
        super(ProcessPool, self).__init__(size)
        self._worker_pool = ObjectPool(size, self._spawn_worker)

    def kill(self, exception=gevent.GreenletExit, block=True, timeout=None):
        """Kills all workers and customer greenlets."""
        workers = self._worker_pool.objects
        for worker in workers:
            worker.kill(exception, block=False)
        if block:
            gevent.joinall(workers, timeout=timeout)
        super(ProcessPool, self).kill(exception, block, timeout)

    def greenlet_class(self, function, *args, **kwargs):
        """The fake greenlet class. It wraps the function with
        :meth:`_run_customer`.
        """
        return gevent.Greenlet(self._run_customer, function, *args, **kwargs)

    def _run_customer(self, function, *args, **kwargs):
        """Sends a call to an available worker and receives result."""
        worker = self._worker_pool.get()
        worker.pipe.put((function, args, kwargs))
        try:
            successful, value = worker.pipe.get()
        finally:
            self._worker_pool.release(worker)
        if successful:
            return value
        else:
            raise value

    def _run_worker(self, pipe):
        """The main loop of worker."""
        while True:
            try:
                function, args, kwargs = pipe.get()
            except EOFError:
                break
            try:
                call_and_put(function, args, kwargs, pipe)
            except SystemExit:
                pass

    def _spawn_worker(self):
        """Spanws a new worker."""
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            p_pipe, c_pipe = gipc.pipe(duplex=True)
        worker = Processlet.spawn(self._run_worker, c_pipe)
        worker.pipe = p_pipe
        worker.rawlink(self._discard_worker)
        return worker

    def _discard_worker(self, worker):
        """Unregisters the worker. Used for rawlink."""
        worker.unlink(self._discard_worker)
        worker.pipe.close()
        self._worker_pool.discard(worker)
