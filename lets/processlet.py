# -*- coding: utf-8 -*-
"""
    lets.processlet
    ~~~~~~~~~~~~~~~

    :copyright: (c) 2013 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
import os
import warnings

import gevent
import gevent.pool
import gipc


__all__ = ['Processlet', 'ProcessPool']


def call_and_put(function, args, kwargs, pipe, exit=False):
    """Calls the function and sends result to the pipe."""
    try:
        value = function(*args, **kwargs)
    except SystemExit as exc:
        if exc.code:
            pipe.put((False, exc))
            if exit:
                raise
        else:
            pipe.put((True, None))
    except BaseException as exc:
        pipe.put((False, exc))
        if exit:
            raise SystemExit(1)
    else:
        pipe.put((True, value))
    if exit:
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
            proc.join()
            self.exit_code = proc.exitcode
            if successful:
                return value
            else:
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
    """Recyclable worker :class:`Processlet` pool. It should be finalized with
    :meth:`kill` to close all child processes.
    """

    def __init__(self, size=None):
        super(ProcessPool, self).__init__(size)
        self._workers = []

    def kill(self, exception=gevent.GreenletExit, block=True, timeout=None):
        """Kills all workers and customer greenlets."""
        for worker in self._workers[:]:
            worker.kill(exception, block=block, timeout=timeout)
        super(ProcessPool, self).kill(exception, block, timeout)

    def greenlet_class(self, function, *args, **kwargs):
        """The fake greenlet class. It wraps the function with
        :meth:`_run_customer`.
        """
        return gevent.Greenlet(
            self._run_customer, self._semaphore.counter,
            function, *args, **kwargs)

    def _available_worker(self, counter):
        """Gets an available worker. If there's no, spawns and adds a new
        worker.
        """
        if counter == 0:
            self.wait_available()
            counter = 1
        try:
            return self._workers[self.size - counter]
        except IndexError:
            worker = self._spawn_worker()
            self._add_worker(worker)
            return worker

    def _run_customer(self, counter, function, *args, **kwargs):
        """Sends a call to an available worker and receives result."""
        worker = self._available_worker(counter)
        worker.pipe.put((function, args, kwargs))
        successful, value = worker.pipe.get()
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
            call_and_put(function, args, kwargs, pipe)

    def _spawn_worker(self):
        """Spanws a new worker."""
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            p_pipe, c_pipe = gipc.pipe(duplex=True)
        worker = Processlet.spawn(self._run_worker, c_pipe)
        worker.pipe = p_pipe
        worker.rawlink(self._close_worker_pipe)
        return worker

    def _close_worker_pipe(self, worker):
        """Closes the pipe of the worker. Used for rawlink."""
        worker.unlink(self._close_worker_pipe)
        worker.pipe.close()

    def _add_worker(self, worker):
        """Registers the worker."""
        worker.rawlink(self._discard_worker)
        self._workers.append(worker)

    def _discard_worker(self, worker):
        """Unregisters the worker. Used for rawlink."""
        worker.unlink(self._discard_worker)
        self._workers.remove(worker)
