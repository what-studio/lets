# -*- coding: utf-8 -*-
"""
    queen
    ~~~~~

    Spawns processes or threads with gevent like interface.

    :copyright: (c) 2013 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
import multiprocessing
import os
import signal
import threading
import time


# methods for :class:`Process`.
RETURN = 0x00
RAISE = 0x01


class Egg(object):
    """Super class of Greenlet like objects."""

    value = None
    exception = None

    _ready = False

    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    @classmethod
    def spawn(cls, func, *args, **kwargs):
        egg = cls(func, *args, **kwargs)
        egg.start()
        return egg

    @classmethod
    def spawn_later(cls, seconds, func, *args, **kwargs):
        egg = cls(func, *args, **kwargs)
        egg.start_later(seconds)
        return egg

    def ready(self):
        return self._ready

    def successful(self):
        return self.ready() and self.exception is None

    def get(self, block=True, timeout=None):
        if not self.ready():
            if not block:
                raise IOError('Would block')
            self.join(timeout)
        if self.successful():
            return self.value
        else:
            raise self.exception

    def start(self):
        self.start_later(0)

    def start_later(self, seconds):
        raise NotImplementedError

    def join(self, timeout=None):
        raise NotImplementedError

    def kill(self, exception=KeyboardInterrupt, block=False, timeout=None):
        raise NotImplementedError


class Process(Egg):

    _proc = None
    _pipe = None

    def _run(self, pipe, sleep=0):
        def killed(signo, frame):
            exc = pipe.recv()
            raise exc
        signal.signal(signal.SIGUSR2, killed)
        if sleep:
            time.sleep(sleep)
        try:
            val = self.func(*self.args, **self.kwargs)
        except SystemExit as exc:
            if exc.code:
                pipe.send((RAISE, exc))
            else:
                pipe.send((RETURN, None))
        except BaseException as exc:
            pipe.send((RAISE, exc))
        else:
            pipe.send((RETURN, val))

    def start_later(self, seconds):
        if self._proc:
            return
        parent_pipe, child_pipe = multiprocessing.Pipe()
        proc = multiprocessing.Process(
            target=self._run, args=(child_pipe, seconds))
        proc.start()
        self._proc = proc
        self._pipe = parent_pipe

    def join(self, timeout=None):
        if self._proc is None:
            raise IOError('Would block')
        if self.ready():
            return
        self._pipe.poll(timeout)
        method, val = self._pipe.recv()
        self._proc.join()
        self._proc = None
        self._pipe = None
        if method == RETURN:
            self.value = val
        elif method == RAISE:
            self.exception = val
        self._ready = True

    def kill(self, exception=KeyboardInterrupt, block=False, timeout=None):
        self._pipe.send(exception)
        os.kill(self._proc.pid, signal.SIGUSR2)
        if block:
            self.join(timeout)


class Thread(Egg):

    _thread = None
    _event = None

    def _run(self, event, sleep=0):
        if sleep:
            time.sleep(sleep)
        try:
            val = self.func(*self.args, **self.kwargs)
        except SystemExit as exc:
            if exc.code:
                self.exception = exc
        except BaseException as exc:
            self.exception = exc
        else:
            self.value = val
        finally:
            event.set()

    def start_later(self, seconds):
        if self._thread:
            return
        event = threading.Event()
        thread = threading.Thread(
            target=self._run, args=(event, seconds))
        thread.start()
        self._thread = thread
        self._event = event

    def join(self, timeout=None):
        if self._thread is None:
            raise IOError('Would block')
        if self.ready():
            return
        self._event.wait(timeout)
        self._thread = None
        self._event = None
        self._ready = True


class Group(object):

    egg_class = None
