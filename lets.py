# -*- coding: utf-8 -*-
"""
    lets
    ~~~~

    Several :class:`gevent.Greenlet` subclasses.

    :copyright: (c) 2013 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
import os
import warnings

import gevent
import gevent.pool
import gipc


__version__ = '0.0.3'
__all__ = ['Processlet', 'Transparentlet', 'TransparentGroup']


# methods used for child to parent communication on :class:`Processlet`.
RETURN = 0
RAISE = 1


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
        gevent.spawn(self._call_and_put, pipe, *args, **kwargs).join()

    def _call_and_put(self, pipe, *args, **kwargs):
        """Calls the function and sends result to the pipe."""
        gevent.spawn(self._get_and_kill, pipe, gevent.getcurrent())
        try:
            value = self.function(*args, **kwargs)
        except SystemExit as exc:
            if exc.code:
                pipe.put((RAISE, exc))
                raise
            else:
                pipe.put((RETURN, None))
        except BaseException as exc:
            pipe.put((RAISE, exc))
            raise SystemExit(1)
        else:
            pipe.put((RETURN, value))
            raise SystemExit(0)

    def _get_and_kill(self, pipe, greenlet):
        """Kills the greenlet if the parent sends an exception."""
        method, exc = pipe.get()
        assert method == RAISE
        greenlet.kill(exc, block=False)


class _FakeParent(gevent.Greenlet):

    def handle_error(self, *exc_info):
        pass


fake_parent = _FakeParent()


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
        real_parent = self.parent
        self.parent = fake_parent
        super(Transparentlet, self)._report_error(exc_info)
        self.parent = real_parent

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
