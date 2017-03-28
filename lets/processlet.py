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


SIGNAL_NUMBERS = set([
    getattr(signal, name) for name in dir(signal) if
    name.startswith('SIG') and not name.startswith('SIG_') and
    name not in ['SIGSTOP', 'SIGKILL', 'SIGPIPE']
])


def reset_signal_handlers(signos=SIGNAL_NUMBERS):
    for signo in signos:
        if signo < signal.NSIG:
            signal.signal(signo, signal.SIG_DFL)


def reset_gevent():
    gevent.reinit()
    gevent.get_hub().destroy(destroy_loop=True)
    gevent.get_hub(default=True)  # Here is necessary.


def is_socket_readable(socket, timeout=None):
    readable, __, __ = gevent.select.select([socket], [], [], timeout)
    return bool(readable)


NOOP_CALLBACK = lambda *x: None


class Processlet(gevent.Greenlet):

    pid = None
    code = None

    def __init__(self, run=None, *args, **kwargs):
        args = (run,) + args
        super(Processlet, self).__init__(None, *args, **kwargs)
        self._result = gevent.event.AsyncResult()

    def send(self, signo):
        os.kill(self.pid, signo)

    @property
    def exit_code(self):
        return self.code

    def _run(self, run, *args, **kwargs):
        p, c = gevent.socket.socketpair()
        pid = gevent.os.fork(callback=self._child_exited)
        if pid == 0:
            self.pid = os.getpid()
            self._child(c, run, args, kwargs)
            return
        self.pid = pid
        ok, rv, self.code = self._parent(p)
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

    def _parent(self, socket):
        """The body of a parent process."""
        # NOTE: This function MUST NOT RAISE an exception.
        # Return `(False, exc_info, code)` instead of raising an exception.
        child_ready = gevent.spawn(socket.recv, 1)
        # Wait for the child to exit.
        loop = gevent.get_hub().loop
        try:
            while True:
                # NOTE: If we don't start a new watcher, the below
                # :meth:`AsyncResult.get` will be failed with :exc:`LoopExit`.
                # See this issue: https://github.com/gevent/gevent/issues/878
                new_watcher = loop.child(self.pid, False)
                new_watcher.start(NOOP_CALLBACK)
                try:
                    self._result.get()
                except ProcessExit:
                    # Child has been exited.
                    raise
                except (gevent.GreenletExit, Exception) as exc:
                    # This processlet has been killed by another greenlet.  The
                    # received exception should be relayed to the child.
                    if not child_ready.ready():
                        # Before relaying the exception, wait for the child
                        # ready.
                        while True:
                            try:
                                child_ready.join()
                            except:
                                # Ignore more killing exceptions.
                                pass
                            else:
                                break
                    # Relay the exception to the child.
                    put(socket, exc)
                    self.send(signal.SIGHUP)
                finally:
                    new_watcher.stop()
        except ProcessExit as exc:
            code = exc.code
        # Collect the function result.
        # if is_socket_readable(socket, 0):
        #     child_ready.join()
        if is_socket_readable(socket, 0):
            # child_ready.join()
            ok, rv = get(socket)
            if not ok and isinstance(rv, SystemExit):
                rv = ProcessExit(rv.code)
        else:
            ok, rv = False, ProcessExit(code)
        return ok, rv, code

    def _child(self, socket, run, args, kwargs):
        """The body of a child process."""
        # Protect against SIGHUP from the parent.
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
        # Reset environments.
        reset_signal_handlers(SIGNAL_NUMBERS - set([signal.SIGHUP]))
        reset_gevent()
        # Reinit the socket because the hub has been destroyed.
        socket = fromfd(socket.fileno(), socket.family, socket.proto)
        # Spawn and ensure to be started the greenlet.
        greenlet = Quietlet.spawn(run, *args, **kwargs)
        greenlet.join(0)
        # Kill the greenlet if there's early exception.  Otherwise, register
        # the formal exception catcher.
        if is_socket_readable(socket, 0):
            greenlet.kill(get(socket), block=False)
        else:
            killed = lambda g, f: self._child_killed(socket, greenlet, f)
            signal.signal(signal.SIGHUP, killed)
        try:
            # Notify starting.
            socket.send(b'\x01')
            # Run the function.
            rv = greenlet.get()
        except SystemExit as rv:
            ok, code = False, rv.code
        except BaseException as rv:
            ok, code = False, 1
        else:
            ok, code = True, 0
        # Notify the result.
        put(socket, (ok, rv))
        os._exit(code)

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


class pipe(object):
    """Opens 2 :class:`Hole`s that pairs with each other.

    You can assign the holes into separate variables like tuple assigning:

       left, right = pipe()
       do_something(left, right)
       left.close()
       right.close()

    Or open and close as a context manager:

       with pipe() as (left, right):
           do_something(left, right)

    """

    __slots__ = ('left', 'right')

    def __init__(self):
        left, right = gevent.socket.socketpair()
        self.left, self.right = Hole(left), Hole(right)

    def close(self):
        self.left.close()
        self.right.close()

    def __iter__(self):
        yield self.left
        yield self.right

    def __enter__(self):
        return (self.left, self.right)

    def __exit__(self, *exc_info):
        self.close()


class Hole(object):
    """A socket holder to pass a socket into a :class:`Processlet` safely."""

    __slots__ = ('fileno', 'family', 'proto', '_socket', '_hub_id')

    def __init__(self, socket):
        self.fileno = socket.fileno()
        self.family = socket.family
        self.proto = socket.proto
        self._socket = socket
        self._hub_id = id(gevent.get_hub())

    def __getstate__(self):
        return (self.fileno, self.family, self.proto)

    def __setstate__(self, (fileno, family, proto)):
        self.fileno, self.family, self.proto = fileno, family, proto

    def socket(self):
        """Gets the underlying socket safely."""
        hub_id = id(gevent.get_hub())
        if hub_id != getattr(self, '_hub_id', -1):
            self._hub_id = hub_id
            self._socket = fromfd(self.fileno, self.family, self.proto)
        return self._socket

    def put(self, value):
        return put(self.socket(), value)

    def get(self):
        return get(self.socket())

    def close(self):
        self.socket().close()


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
            except BaseException as exc:
                _put((False, exc))
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
