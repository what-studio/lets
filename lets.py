# -*- coding: utf-8 -*-
"""
    lets
    ~~~~

    Several :class:`gevent.Greenlet` subclasses.

    :copyright: (c) 2013 by Heungsub Lee
    :license: BSD, see LICENSE for more details.
"""
import gevent
import gevent.pool
import gipc


__version__ = '0.0.1'
__all__ = ['Processlet', 'Transparentlet', 'TransparentGroup']


# methods used for child to parent communication on :class:`Processlet`.
RETURN = 0
RAISE = 1


class Processlet(gevent.Greenlet):
    """Calls a function in child process."""

    function = None

    def __init__(self, function=None, *args, **kwargs):
        self.function = function
        super(Processlet, self).__init__(*args, **kwargs)

    def _run(self, *args, **kwargs):
        """Opens pipe and starts child process to run :meth:`_run_child`. Then
        it waits for the child process done.
        """
        with gipc.pipe() as (r_pipe, w_pipe):
            proc = gipc.start_process(self._run_child, (w_pipe, args, kwargs))
            method, value = r_pipe.get()
            proc.join()
            if method == RETURN:
                return value
            elif method == RAISE:
                raise value

    def _run_child(self, pipe, args, kwargs):
        """The target of child process. It puts result to the pipe when it
        done.
        """
        try:
            value = self.function(*args, **kwargs)
        except SystemExit as exc:
            if exc.code:
                pipe.put((RAISE, exc))
            else:
                pipe.put((RETURN, None))
        except BaseException as exc:
            pipe.put((RAISE, exc))
        else:
            pipe.put((RETURN, value))


class Transparentlet(gevent.Greenlet):
    """Saves the actual exc_info when the function raises some exception. It
    doesn't print exception to stderr. Consider to use this. It saves heavy
    traceback object also.
    """

    exc_info = (None, None, None)

    def _report_error(self, exc_info):
        self.exc_info = exc_info
        exception = exc_info[1]
        if isinstance(exception, gevent.GreenletExit):
            self._report_result(exception)
            return
        self._exception = exception
        if self._links and not self._notifier:
            self._notifier = self.parent.loop.run_callback(self._notify_links)

    def get(self, block=True, timeout=None):
        """Returns the result. If the function raises an exception, it also
        raises the exception and traceback transparently.
        """
        try:
            return super(Transparentlet, self).get(block, timeout)
        except:
            raise self.exc_info[0], self.exc_info[1], self.exc_info[2]


class TransparentGroup(gevent.pool.Group):
    """Raises an exception and traceback in the greenlets transparently."""

    greenlet_class = Transparentlet

    def join(self, timeout=None, raise_error=False):
        if raise_error:
            greenlets = self.greenlets.copy()
            self._empty_event.wait(timeout=timeout)
            for greenlet in greenlets:
                if greenlet.exc_info is not None:
                    greenlet.get(timeout=timeout)
        else:
            self._empty_event.wait(timeout=timeout)
