# -*- coding: utf-8 -*-
"""
   lets.processlet
   ~~~~~~~~~~~~~~~

   Maximizing multi-core use in gevent environment.

   :class:`Processlet` is a subclass of :class:`gevent.Greenlet` but focuses
   to CPU-bound tasks not I/O-bound.  Never give up high concurrency gevent
   offered.

   .. sourcecode:: python

      import bcrypt
      import gevent
      from lets import Processlet

      # bcrypt.hashpw is very heavy cpu-bound task.
      # it can spend a few seconds.
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

   :copyright: (c) 2013-2016 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from contextlib import contextmanager
import os
import signal
import sys
import warnings

import gevent
import gevent.event
import gevent.local
import gevent.pool
import gevent.queue
import gipc

from .objectpool import ObjectPool


__all__ = ['ProcessExit', 'Processlet', 'ProcessPool', 'ProcessLocal']


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
    def pipe_put(value):
        try:
            pipe.put(value)
        except OSError:
            pass
    try:
        value = function(*args, **kwargs)
    except gevent.GreenletExit as exc:
        pipe_put((True, exc))
    except SystemExit as exc:
        exc_info = sys.exc_info()
        pipe_put((False, exc))
        raise exc_info[0], exc_info[1], exc_info[2]
    except BaseException as exc:
        pipe_put((False, exc))
        raise SystemExit(1)
    else:
        pipe_put((True, value))
    raise SystemExit(0)


def get_and_kill(pipe, greenlet):
    """Kills the greenlet if the parent sends an exception."""
    try:
        exc = pipe.get()
    except EOFError as exc:
        pass
    greenlet.kill(exc, block=False)


class Processlet(gevent.Greenlet):
    """Calls a function in child process."""

    function = None
    pid = None
    exit_code = None

    def __init__(self, function=None, *args, **kwargs):
        super(Processlet, self).__init__(None, *args, **kwargs)
        self.function = function
        self._queue = gevent.queue.Queue()
        self._started = gevent.event.Event()

    def _call_when_started(self, function, *args, **kwargs):
        if self._queue is None:
            raise RuntimeError('Child process already started')
        self._queue.put((function, args, kwargs))

    def _wait_starting(self, timeout=None):
        self._started.wait(timeout=timeout)

    def send(self, signo, block=True, timeout=None):
        """Sends a signal to the child process."""
        if block:
            self._wait_starting(timeout=timeout)
        elif not self._started.is_set():
            self._call_when_started(self.send, signo, block=False)
            return
        os.kill(self.pid, signo)
        if block:
            self.join(timeout)

    def join(self, timeout=None):
        """Waits until finishes or timeout expires like a greenlet.  If you
        set timeout as Zero, it waits until the child process starts.
        """
        if timeout == 0:
            self._wait_starting()
        return super(Processlet, self).join(timeout)

    def kill(self, exception=gevent.GreenletExit, block=True, timeout=None):
        """Kills the child process like a greenlet."""
        if block:
            self._wait_starting(timeout=timeout)
        elif not self._started.is_set():
            self._call_when_started(self.kill, exception, block=False)
            return
        return super(Processlet, self).kill(exception, block, timeout)

    def _run(self, *args, **kwargs):
        """Opens pipe and starts child process to run :meth:`_run_child`.
        Then it waits for the child process done.
        """
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            pipe_pair = gipc.pipe(duplex=True)
        with pipe_pair as (p_pipe, c_pipe):
            args = (c_pipe,) + args
            proc = gipc.start_process(self._run_child, args, kwargs)
            successful, value = True, None
            try:
                # wait for child process started.
                self.pid = p_pipe.get()
                assert self.pid == proc.pid
                self._started.set()
                # call reserved function calls by :meth:`_call_when_started`.
                while True:
                    try:
                        f, args, kwargs = self._queue.get(block=False)
                    except gevent.queue.Empty:
                        break
                    else:
                        f(*args, **kwargs)
                assert self._queue.empty()
                self._queue = None
                # wait for result.
                successful, value = p_pipe.get()
            except EOFError:
                proc.join()
                if proc.exitcode:
                    successful, value = False, SystemExit(proc.exitcode)
            except BaseException as value:
                successful = isinstance(value, gevent.GreenletExit)
                try:
                    p_pipe.put(value)
                except OSError:
                    # broken pipe.
                    pass
                else:
                    if self._started.is_set():
                        successful, value = p_pipe.get()
                proc.join()
            finally:
                proc.join()  # wait until the child process exits.
        self.exit_code = proc.exitcode
        if successful:
            return value
        # failure.
        if isinstance(value, SystemExit):
            if value.code == -signal.SIGINT:
                raise gevent.GreenletExit('Exited by SIGINT')
            else:
                raise ProcessExit(value.code)
        else:
            raise value

    def _run_child(self, pipe, *args, **kwargs):
        """The target of child process.  It puts result to the pipe when it
        done.
        """
        try:
            pipe.put(os.getpid())  # child started.
        except OSError:
            pass
        else:
            g = gevent.spawn(call_and_put, self.function, args, kwargs, pipe)
            gevent.spawn(get_and_kill, pipe, g)
            g.join()


class ProcessPool(gevent.pool.Pool):
    """Recyclable worker :class:`Processlet` pool.  It should be finalized with
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
        """The fake greenlet class.  It wraps the function with
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
        """Unregisters the worker.  Used for rawlink."""
        worker.unlink(self._discard_worker)
        self._worker_pool.discard(worker)


class KeepDict(BaseException):

    pass


@contextmanager
def _patch(self):
    pid = os.getpid()
    local_pid = object.__getattribute__(self, '_local__pid')
    if local_pid == pid:
        # Don't work as thread-local on the same process.
        yield
    else:
        object.__setattr__(self, '_local__pid', pid)
        try:
            with gevent.local._patch(self):
                yield
                # Don't recover the previous local __dict__ by _patch() to
                # keep the current one.
                raise KeepDict
        except KeepDict:
            pass


class ProcessLocal(gevent.local.local):
    """Process-local object."""

    __slots__ = ('_local__impl', '_local__pid')

    def __new__(cls, *args, **kwargs):
        self = super(ProcessLocal, cls).__new__(cls, *args, **kwargs)
        object.__setattr__(self, '_local__pid', os.getpid())
        return self

    def __getattribute__(self, attr):
        with _patch(self):
            return object.__getattribute__(self, attr)

    def __setattr__(self, attr, value):
        with _patch(self):
            return object.__setattr__(self, attr, value)

    def __delattr__(self, attr):
        with _patch(self):
            return object.__delattr__(self, attr)
