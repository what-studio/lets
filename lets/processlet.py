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
import io
import os
try:
    import cPickle as pickle
except ImportError:
    import pickle
import signal
import struct
import sys
import warnings

import gevent
import gevent.event
import gevent.local
import gevent.pool
import gevent.queue
import gevent.select
import gevent.signal
import gevent.socket
import gipc

from .objectpool import ObjectPool
from .quietlet import Quietlet


__all__ = ['ProcessExit', 'Processlet', 'ProcessPool', 'ProcessLocal']


class ProcessExit(BaseException):
    """Originally, :exc:`SystemExit` kills all independent gevent waitings.
    To prevent killing the current process, :class:`Processlet` replaces
    :exc:`SystemExit` from child process with this exception.
    """

    def __init__(self, code):
        self.code = code
        super(ProcessExit, self).__init__(code)


class ChildExit(BaseException):

    def __init__(self, stat):
        self.stat = stat
        super(ChildExit, self).__init__(stat)


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


# class Processlet(gevent.Greenlet):
#     """Calls a function in child process."""

#     function = None
#     pid = None
#     exit_code = None

#     def __init__(self, function=None, *args, **kwargs):
#         super(Processlet, self).__init__(None, *args, **kwargs)
#         self.function = function
#         self._queue = gevent.queue.Queue()
#         self._started = gevent.event.Event()

#     def _call_when_started(self, function, *args, **kwargs):
#         if self._queue is None:
#             raise RuntimeError('child process already started')
#         self._queue.put((function, args, kwargs))

#     def _wait_starting(self, timeout=None):
#         self._started.wait(timeout=timeout)

#     def send(self, signo, block=True, timeout=None):
#         """Sends a signal to the child process."""
#         if block:
#             self._wait_starting(timeout=timeout)
#         elif not self._started.is_set():
#             self._call_when_started(self.send, signo, block=False)
#             return
#         os.kill(self.pid, signo)
#         if block:
#             self.join(timeout)

#     def join(self, timeout=None):
#         """Waits until finishes or timeout expires like a greenlet.  If you
#         set timeout as Zero, it waits until the child process starts.
#         """
#         if timeout == 0:
#             self._wait_starting()
#         return super(Processlet, self).join(timeout)

#     def kill(self, exception=gevent.GreenletExit, block=True, timeout=None):
#         """Kills the child process like a greenlet."""
#         if block:
#             self._wait_starting(timeout=timeout)
#         elif not self._started.is_set():
#             self._call_when_started(self.kill, exception, block=False)
#             return
#         return super(Processlet, self).kill(exception, block, timeout)

#     def _run(self, *args, **kwargs):
#         """Opens pipe and starts child process to run :meth:`_run_child`.
#         Then it waits for the child process done.
#         """
#         with warnings.catch_warnings():
#             warnings.simplefilter('ignore')
#             pipe_pair = gipc.pipe(duplex=True)
#         with pipe_pair as (p_pipe, c_pipe):
#             args = (c_pipe,) + args
#             proc = gipc.start_process(self._run_child, args, kwargs)
#             successful, value = True, None
#             try:
#                 # wait for child process started.
#                 self.pid = p_pipe.get()
#                 assert self.pid == proc.pid
#                 self._started.set()
#                 # call reserved function calls by :meth:`_call_when_started`.
#                 while True:
#                     try:
#                         f, args, kwargs = self._queue.get(block=False)
#                     except gevent.queue.Empty:
#                         break
#                     else:
#                         f(*args, **kwargs)
#                 assert self._queue.empty()
#                 self._queue = None
#                 # wait for result.
#                 successful, value = p_pipe.get()
#             except EOFError:
#                 proc.join()
#                 if proc.exitcode:
#                     successful, value = False, SystemExit(proc.exitcode)
#             except BaseException as value:
#                 successful = isinstance(value, gevent.GreenletExit)
#                 try:
#                     p_pipe.put(value)
#                 except OSError:
#                     # broken pipe.
#                     pass
#                 else:
#                     if self._started.is_set():
#                         successful, value = p_pipe.get()
#                 proc.join()
#             finally:
#                 proc.join()  # wait until the child process exits.
#         self.exit_code = proc.exitcode
#         if successful:
#             return value
#         # failure.
#         if isinstance(value, SystemExit):
#             if value.code == -signal.SIGINT:
#                 raise gevent.GreenletExit('Exited by SIGINT')
#             else:
#                 raise ProcessExit(value.code)
#         else:
#             raise value

#     def _run_child(self, pipe, *args, **kwargs):
#         """The target of child process.  It puts result to the pipe when it
#         done.
#         """
#         try:
#             pipe.put(os.getpid())  # child started.
#         except OSError:
#             pass
#         else:
#             g = gevent.spawn(call_and_put, self.function, args, kwargs, pipe)
#             gevent.spawn(get_and_kill, pipe, g)
#             g.join()


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


HEADER_SPEC = '=I'
HEADER_SIZE = struct.calcsize(HEADER_SPEC)


def recv_enough(sock, size):
    buf = io.BytesIO()
    more = size
    while more:
        chunk = sock.recv(more)
        if not chunk:
            raise EOFError
        buf.write(chunk)
        more -= len(chunk)
    return buf.getvalue()


class Processlet(gevent.Greenlet):

    pid = None
    code = None

    def __init__(self, run=None, *args, **kwargs):
        args = (run,) + args
        super(Processlet, self).__init__(None, *args, **kwargs)
        self._started = gevent.event.Event()
        self._exited = gevent.event.Event()

    @property
    def exit_code(self):
        return self.code

    def started(self):
        return self._started.is_set()

    def kill(self, exception=gevent.GreenletExit, block=True, timeout=None):
        """Kills the child process like a greenlet."""
        if not self.started():
            self._deferred_exception = exception
            if block:
                self.join(timeout)
            return
        return super(Processlet, self).kill(exception, block, timeout)

    def _run(self, run, *args, **kwargs):
        p, c = gevent.socket.socketpair()
        def callback(watcher):
            self.throw(ChildExit(watcher.rstatus))
            self._exited.set()
        self.pid = pid = gevent.os.fork_and_watch(callback=callback)
        if pid == 0:
            self._child(c, run, args, kwargs)
            return
        ok, rv, self.code = self._parent(p, pid)
        if ok:
            return rv
        else:
            raise rv

    def _parent(self, sock, pid):
        """The body of a parent process."""
        watcher = gevent.os._watched_children[pid]
        try:
            # Wait for the child to start.
            sock.recv(1)
            self._started.set()
            try:
                exc = self._deferred_exception
            except AttributeError:
                pass
            else:
                del self._deferred_exception
                self.kill(exc, block=False)
            while True:
                new_watcher = watcher.loop.child(pid, False)
                try:
                    # NOTE: It can work cooperatively since 1.2a1.
                    # See issue: https://github.com/gevent/gevent/issues/878
                    # gevent.os.waitpid(pid, 0)
                    gevent.get_hub().wait(new_watcher)
                except ChildExit:
                    raise
                except BaseException as exc:
                    # import traceback
                    # traceback.print_exc()
                    self._send(sock, exc)
                    os.kill(pid, signal.SIGHUP)
                else:
                    break
        except ChildExit as exc:
            stat = exc.stat
        # Normalize child status.
        if os.WIFEXITED(stat):
            code = os.WEXITSTATUS(stat)
        elif os.WIFSIGNALED(stat):
            code = -os.WTERMSIG(stat)
        else:
            assert False
        # Collect result.
        ready, __, __ = gevent.select.select([sock], [], [], 0)
        if ready:
            ok, rv = self._recv(sock)
            if not ok and isinstance(rv, SystemExit):
                rv = ProcessExit(rv.code)
        else:
            ok, rv = False, ProcessExit(code)
        return ok, rv, code

    def _child(self, sock, run, args, kwargs):
        """The body of a child process."""
        # Cancel all scheduled greenlets.
        gevent.get_hub().destroy(destroy_loop=True)
        # Reinit the socket because the hub has been destroyed.
        sock = gevent.socket.fromfd(sock.fileno(), sock.family, sock.proto)
        greenlet = Quietlet.spawn(run, *args, **kwargs)
        killed = lambda __, frame: self._child_killed(sock, greenlet, frame)
        assert signal.signal(signal.SIGHUP, killed) == 0
        # Notify starting.
        greenlet.join(0)
        sock.send(b'\x00')
        # Run the function.
        try:
            rv = greenlet.get()
        except SystemExit as rv:
            ok, code = False, rv.code
        except BaseException as rv:
            ok, code = False, 1
        else:
            ok, code = True, 0
        # Notify the result.
        self._send(sock, (ok, rv))
        os._exit(code)

    @classmethod
    def _child_killed(cls, sock, greenlet, frame):
        """A signal handler on a child process to detect killing exceptions
        from the parent process.
        """
        exc = cls._recv(sock)
        if greenlet.gr_frame in [frame, None]:
            # The greenlet is busy.
            raise exc
        greenlet.kill(exc, block=False)

    @staticmethod
    def _send(sock, value):
        """Sends a Python value through the socket."""
        data = pickle.dumps(value)
        sock.send(struct.pack(HEADER_SPEC, len(data)))
        sock.send(data)

    @staticmethod
    def _recv(sock):
        """Receives a Python value through the socket."""
        size_data = recv_enough(sock, HEADER_SIZE)
        size, = struct.unpack(HEADER_SPEC, size_data)
        data = recv_enough(sock, size)
        return pickle.loads(data)
