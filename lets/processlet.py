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

   :copyright: (c) 2013-2017 by Heungsub Lee
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
from gevent.socket import fromfd

from lets.objectpool import ObjectPool
from lets.quietlet import Quietlet


__all__ = ['ProcessExit', 'Processlet', 'ProcessPool', 'ProcessLocal']


class ProcessExit(Exception):
    """Originally, :exc:`SystemExit` kills all independent gevent waitings.
    To prevent killing the current process, :class:`Processlet` replaces
    :exc:`SystemExit` from child process with this exception.
    """

    def __init__(self, code):
        self.code = code
        super(ProcessExit, self).__init__(code)


# def call_and_put(function, args, kwargs, hole):
#     """Calls the function and sends result to the hole."""
#     def hole_put(value):
#         try:
#             put(hole.socket, value)
#         except OSError:
#             pass
#     try:
#         value = function(*args, **kwargs)
#     except gevent.GreenletExit as exc:
#         hole_put((True, exc))
#     except SystemExit as exc:
#         exc_info = sys.exc_info()
#         hole_put((False, exc))
#         raise exc_info[0], exc_info[1], exc_info[2]
#     except BaseException as exc:
#         hole_put((False, exc))
#         raise SystemExit(1)
#     else:
#         hole_put((True, value))
#     # raise SystemExit(0)


# def get_and_kill(hole, greenlet):
#     """Kills the greenlet if the parent sends an exception."""
#     try:
#         exc = get(hole.socket)
#     except EOFError as exc:
#         pass
#     greenlet.kill(exc, block=False)


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
        """A fake greenlet class which wraps the given function call with
        :meth:`_run_customer`.
        """
        return gevent.Greenlet(self._run_customer, function, *args, **kwargs)

    def _run_customer(self, function, *args, **kwargs):
        """Sends a call to an available worker and receives result."""
        worker = self._worker_pool.get()
        socket = worker.hole.socket()
        try:
            # Request the function call.
            put(socket, (function, args, kwargs))
            # Receive the result.
            ok, rv = get(socket)
        finally:
            self._worker_pool.release(worker)
        if ok:
            return rv
        else:
            raise rv

    def _run_worker(self, hole):
        """The main loop of worker."""
        socket = hole.socket()
        def _put(value):
            try:
                put(socket, value)
            except OSError:
                pass
        while True:
            # Receive a function call request from customers.
            try:
                function, args, kwargs = get(socket)
            except EOFError:
                break
            # Call the function and let the customer know.
            try:
                value = function(*args, **kwargs)
            except gevent.GreenletExit as exc:
                _put((True, exc))
            except SystemExit as exc:
                exc_info = sys.exc_info()
                _put((False, exc))
                raise exc_info[0], exc_info[1], exc_info[2]
            except BaseException as exc:
                _put((False, exc))
                raise SystemExit(1)
            else:
                _put((True, value))

    def _spawn_worker(self):
        """Spanws a new worker."""
        p, c = pipe()
        worker = Processlet.spawn(self._run_worker, c)
        worker.hole = p
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


def put(socket, value):
    """Sends a Python value through the socket."""
    data = pickle.dumps(value)
    socket.sendall(struct.pack(HEADER_SPEC, len(data)))
    socket.sendall(data)


def get(socket):
    """Receives a Python value through the socket."""
    size_data = recv_enough(socket, HEADER_SIZE)
    size, = struct.unpack(HEADER_SPEC, size_data)
    data = recv_enough(socket, size)
    return pickle.loads(data)


def recv_enough(socket, size):
    buf = io.BytesIO()
    more = size
    while more:
        chunk = socket.recv(more)
        if not chunk:
            raise EOFError
        buf.write(chunk)
        more -= len(chunk)
    return buf.getvalue()


SIGNAL_NUMBERS = [getattr(signal, name) for name in dir(signal) if
                  name.startswith('SIG') and not name.startswith('SIG_') and
                  name not in ['SIGSTOP', 'SIGKILL', 'SIGPIPE']]


def reset_signal_handlers(signos=SIGNAL_NUMBERS):
    for signo in signos:
        if signo < signal.NSIG:
            signal.signal(signo, signal.SIG_DFL)


def reset_gevent():
    gevent.reinit()
    gevent.get_hub().destroy(destroy_loop=True)
    gevent.get_hub(default=True)  # Here is necessary.


class Processlet(gevent.Greenlet):

    pid = None
    code = None

    def __init__(self, run=None, *args, **kwargs):
        args = (run,) + args
        super(Processlet, self).__init__(None, *args, **kwargs)
        self._result = gevent.event.AsyncResult()

    @property
    def exit_code(self):
        return self.code

    def _run(self, run, *args, **kwargs):
        p, c = gevent.socket.socketpair()
        self.pid = pid = gevent.os.fork(callback=self._child_exited)
        if pid == 0:
            self._child(c, run, args, kwargs)
            return
        ok, rv, self.code = self._parent(p, pid)
        if ok:
            return rv
        else:
            raise rv

    def _child_exited(self, watcher):
        watcher.stop()
        status = watcher.rstatus
        if os.WIFEXITED(status):
            code = os.WEXITSTATUS(status)
        else:
            assert os.WIFSIGNALED(status)
            code = -os.WTERMSIG(status)
        exc = ProcessExit(code)
        self._result.set_exception(exc)
        self.throw(exc)

    def _parent(self, socket, pid):
        """The body of a parent process."""
        loop = gevent.get_hub().loop
        child_ready = gevent.spawn(socket.recv, 1)
        try:
            # Wait for the child ready.
            child_ready.join()
            # Wait for the child to exit.
            while True:
                # NOTE: If we don't start a new watcher, the below
                # :meth:`AsyncResult.get` will be failed with :exc:`LoopExit`.
                # See this issue: https://github.com/gevent/gevent/issues/878
                new_watcher = loop.child(pid, False)
                new_watcher.start(lambda *x: None)
                try:
                    self._result.get()
                except ProcessExit:
                    # Child has been exited.
                    raise
                except (gevent.GreenletExit, Exception) as exc:
                    # This processlet has been killed by another greenlet.  The
                    # received exception should be relayed to the child.
                    put(socket, exc)
                    os.kill(pid, signal.SIGHUP)
                finally:
                    new_watcher.stop()
        except ProcessExit as exc:
            code = exc.code
        # Collect the function result.
        ready, __, __ = gevent.select.select([socket], [], [], 1)
        if ready:
            ok, rv = get(socket)
            if not ok and isinstance(rv, SystemExit):
                rv = ProcessExit(rv.code)
        else:
            ok, rv = False, ProcessExit(code)
        return ok, rv, code

    def _child(self, socket, run, args, kwargs):
        """The body of a child process."""
        # Reset environments.
        reset_signal_handlers()
        reset_gevent()
        # Reinit the socket because the hub has been destroyed.
        socket = fromfd(socket.fileno(), socket.family, socket.proto)
        # Catch exception before the greenlet is ready.
        early_exc = gevent.event.AsyncResult()
        signal.signal(signal.SIGHUP,
                      lambda g, f: self._child_killed_early(socket, early_exc))
        # Spawn and ensure to be started the greenlet.
        greenlet = Quietlet.spawn(run, *args, **kwargs)
        greenlet.join(0)
        # Kill the greenlet if there's early exception.  Otherwise, register
        # the formal exception catcher.
        if early_exc.ready():
            signal.signal(signal.SIGHUP, signal.SIG_IGN)
            greenlet.kill(early_exc.exception, block=False)
        else:
            signal.signal(signal.SIGHUP,
                          lambda g, f: self._child_killed(socket, greenlet, f))
        try:
            # Notify starting.
            socket.send(b'\x01')
            # Run the function.
            rv = greenlet.get()
        except SystemExit as rv:
            ok, code = False, rv.code
        except BaseException as rv:
            import traceback
            traceback.print_exc()
            ok, code = False, 1
        else:
            ok, code = True, 0
        # Notify the result.
        put(socket, (ok, rv))
        os._exit(code)

    @staticmethod
    def _child_killed_early(socket, result):
        exc = get(socket)
        result.set_exception(exc)

    @staticmethod
    def _child_killed(socket, greenlet, frame):
        """A signal handler on a child process to detect killing exceptions
        from the parent process.
        """
        exc = get(socket)
        if greenlet.gr_frame is frame:
            # The greenlet is busy.
            raise exc
        greenlet.kill(exc, block=False)


class Hole(object):
    """A socket holder to pass a socket into a :class:`Processlet` safely."""

    __slots__ = ('fileno', 'family', 'proto', '_socket', '_hub_id')

    def __init__(self, socket):
        self.fileno = socket.fileno()
        self.family = socket.family
        self.proto = socket.proto
        self._socket = socket
        self._hub_id = id(gevent.get_hub())

    def socket(self):
        hub_id = id(gevent.get_hub())
        if hub_id != getattr(self, '_hub_id', -1):
            self._hub_id = hub_id
            self._socket = fromfd(self.fileno, self.family, self.proto)
        return self._socket

    def __getstate__(self):
        return (self.fileno, self.family, self.proto)

    def __setstate__(self, (fileno, family, proto)):
        self.fileno, self.family, self.proto = fileno, family, proto


def pipe():
    left, right = gevent.socket.socketpair()
    return Hole(left), Hole(right)
