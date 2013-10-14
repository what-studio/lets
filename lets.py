# -*- coding: utf-8 -*-
"""
    lets
    ~~~~

    Several :class:`gevent.Greenlet` subclasses.

    :copyright: (c) 2013 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
import itertools
import os
import warnings

import gevent
import gevent.pool
import gipc


__version__ = '0.0.4'
__all__ = ['Processlet', 'Transparentlet', 'TransparentGroup']


# methods used for child to parent communication on :class:`Processlet`.
RETURN = 0
RAISE = 1


def call_and_put(function, args, kwargs, pipe, exit=False):
    """Calls the function and sends result to the pipe."""
    try:
        value = function(*args, **kwargs)
    except SystemExit as exc:
        if exc.code:
            pipe.put((RAISE, exc))
            if exit:
                raise
        else:
            pipe.put((RETURN, None))
    except BaseException as exc:
        pipe.put((RAISE, exc))
        if exit:
            raise SystemExit(1)
    else:
        pipe.put((RETURN, value))
    if exit:
        raise SystemExit(0)


def get_and_kill(pipe, greenlet):
    """Kills the greenlet if the parent sends an exception."""
    try:
        method, exc = pipe.get()
    except EOFError as exc:
        pass
    else:
        assert method == RAISE
    greenlet.kill(exc, block=False)


class Processlet(gevent.Greenlet):
    """Calls a function in child process."""

    function = None
    exit_code = None

    def __init__(self, function=None, *args, **kwargs):
        self.function = function
        super(Processlet, self).__init__(None, *args, **kwargs)

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
            try:
                self.join(timeout)
            except:  # such as SystemExit or GreenletExit
                pass

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
            method, value = RETURN, None
            try:
                method, value = p_pipe.get()
            except EOFError:
                proc.join()
                if proc.exitcode:
                    method, value = RAISE, SystemExit(proc.exitcode)
            except BaseException as exc:
                p_pipe.put((RAISE, exc))
                method, value = p_pipe.get()
            proc.join()
            self.exit_code = proc.exitcode
            if method == RETURN:
                return value
            elif method == RAISE:
                raise value

    def _run_child(self, *args, **kwargs):
        """The target of child process. It puts result to the pipe when it
        done.
        """
        pipe, args = args[0], args[1:]
        greenlet = gevent.spawn(
            call_and_put, self.function, args, kwargs, pipe, exit=True)
        gevent.spawn(get_and_kill, pipe, greenlet)
        greenlet.join()


class ProcessPool(gevent.pool.Pool):

    def __init__(self, size=None):
        super(ProcessPool, self).__init__(size)
        self._workers = []

    def kill(self, exception=gevent.GreenletExit, block=True, timeout=None):
        for worker in self._workers[:]:
            worker.kill(exception, block=block, timeout=timeout)
        super(ProcessPool, self).kill(exception, block, timeout)

    def greenlet_class(self, function, *args, **kwargs):
        return gevent.Greenlet(self._run_customer, self._semaphore.counter,
                               function, *args, **kwargs)

    def _run_customer(self, counter, function, *args, **kwargs):
        worker = self._available_worker(counter)
        worker.pipe.put((function, args, kwargs))
        method, value = worker.pipe.get()
        if method == RETURN:
            return value
        elif method == RAISE:
            raise value

    def _spawn_worker(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            p_pipe, c_pipe = gipc.pipe(duplex=True)
        worker = Processlet.spawn(self._run_worker, c_pipe)
        worker.pipe = p_pipe
        worker.rawlink(self._close_worker_pipe)
        return worker

    def _close_worker_pipe(self, worker):
        worker.unlink(self._close_worker_pipe)
        worker.pipe.close()

    def _add_worker(self, worker):
        worker.rawlink(self._discard_worker)
        self._workers.append(worker)

    def _discard_worker(self, worker):
        worker.unlink(self._discard_worker)
        self._workers.remove(worker)

    def _available_worker(self, counter):
        if counter == 0:
            self.wait_available()
            counter = 1
        try:
            return self._workers[self.size - counter]
        except IndexError:
            worker = self._spawn_worker()
            self._add_worker(worker)
            return worker

    def _run_worker(self, pipe):
        while True:
            try:
                function, args, kwargs = pipe.get()
            except EOFError:
                break
            call_and_put(function, args, kwargs, pipe)


noop = lambda *args, **kwargs: None


class Transparentlet(gevent.Greenlet):
    """Saves the actual exc_info when the function raises some exception. It
    doesn't print exception to stderr. Consider to use this. It saves heavy
    traceback object also.
    """

    exc_info = None

    def _report_error(self, exc_info):
        """Same with :meth:`gevent.Greenlet._report_error` but saves exc_info
        event a traceback object and doesn't call the parent's
        ``handle_error``.
        """
        self.exc_info = exc_info
        handle_error, self.parent.handle_error = self.parent.handle_error, noop
        super(Transparentlet, self)._report_error(exc_info)
        self.parent.handle_error = handle_error

    def get(self, block=True, timeout=None):
        """Returns the result. If the function raises an exception, it also
        raises the exception and traceback transparently.
        """
        try:
            return super(Transparentlet, self).get(block, timeout)
        except:
            if self.exc_info is None:
                raise
            else:
                raise self.exc_info[0], self.exc_info[1], self.exc_info[2]


class TransparentGroup(gevent.pool.Group):
    """Raises an exception and traceback in the greenlets transparently."""

    greenlet_class = Transparentlet

    def join(self, timeout=None, raise_error=False):
        if raise_error:
            greenlets = self.greenlets.copy()
            self._empty_event.wait(timeout=timeout)
            for greenlet in greenlets:
                if greenlet.ready() and not greenlet.successful():
                    greenlet.get(timeout=timeout)
        else:
            self._empty_event.wait(timeout=timeout)
