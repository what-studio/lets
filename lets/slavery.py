# -*- coding: utf-8 -*-
"""
   lets.slavery
   ~~~~~~~~~~~~

   Links 2 greenlets by the slavery.

   :copyright: (c) 2013-2018 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
import sys

from gevent import GreenletExit, killall
from gevent.event import Event
import six


__all__ = ['MasterGreenletExit', 'join_slaves', 'link_slave', 'spawn_slave',
           'spawn_slave_later', 'link_partner', 'spawn_partner',
           'spawn_partner_later']


try:
    killall(set([None]))
except AttributeError:
    # The expected error.
    pass
except TypeError:
    # killall() of gevent<=1.1a1 couldn't accept an arbitrary iterable.
    # https://github.com/gevent/gevent/issues/404
    _killall = killall
    def killall(greenlets, *args, **kwargs):
        return _killall(list(greenlets), *args, **kwargs)


class MasterGreenletExit(GreenletExit):
    """Slave greenlet should exit when the master greenlet finishes execution.
    """

    pass


def join_slaves(greenlets, timeout=None, exception=MasterGreenletExit):
    """Waits for the greenlets to finish just like :func:`gevent.joinall`.  But
    the greenlets are treated as slave greenlets.

    When it gets an exception during waiting, it kills the greenlets.  If
    timeout is not given, it waits for them to finish again before raising the
    exception.  So after calling it without timeout, always all the greenlets
    are ready.

    With timeout, it raises the exception immediately without waiting for the
    killed greenlets.

    :returns: a list of the ready greenlets.

    """
    if not greenlets:
        return []
    active, done, empty_event = set(), [], Event()
    def callback(g):
        active.discard(g)
        done.append(g)
        if not active:
            empty_event.set()
    try:
        for greenlet in greenlets:
            active.add(greenlet)
            greenlet.link(callback)
        try:
            empty_event.wait(timeout)
        except:
            exc_info = sys.exc_info()
            killall(active, exception, block=False)
            if timeout is None:
                empty_event.wait()
            six.reraise(*exc_info)
    finally:
        for greenlet in greenlets:
            greenlet.unlink(callback)
    return done


def link_slave(greenlet, slave, exception=MasterGreenletExit):
    """Links a greenlet greenlet and a slave greenlet.  Slave greenlet will be
    killed when the greenlet finishes execution.
    """
    def punish(greenlet):
        slave.unlink(liberate)
        slave.kill(exception, block=False)
    def liberate(slave):
        greenlet.unlink(punish)
    greenlet.link(punish)
    slave.link(liberate)


def spawn_slave(greenlet, func, *args, **kwargs):
    """Spawns a slave greenlet.  Slave greenlet will be killed when the
    greenlet finishes execution.
    """
    slave = greenlet.spawn(func, *args, **kwargs)
    link_slave(greenlet, slave)
    return slave


def spawn_slave_later(greenlet, seconds, func, *args, **kwargs):
    """Spawns a slave greenlet the given seconds later.  Slave greenlet will be
    killed when the greenlet finishes execution.
    """
    slave = greenlet.spawn_later(seconds, func, *args, **kwargs)
    link_slave(greenlet, slave)
    return slave


def link_partner(greenlet, partner, exception=MasterGreenletExit):
    """The greenlets will be killed when another greenlet finishes execution.
    """
    link_slave(greenlet, partner, exception=exception)
    link_slave(partner, greenlet, exception=exception)


def spawn_partner(greenlet, func, *args, **kwargs):
    """Spawns a partner greenlet.  The greenlet and partner greenlets will die
    when another greenlet finishes execution.
    """
    partner = greenlet.spawn(func, *args, **kwargs)
    link_partner(greenlet, partner)
    return partner


def spawn_partner_later(greenlet, seconds, func, *args, **kwargs):
    """Spawns a partner greenlet the given seconds later.  The greenlet and
    partner greenlets will die when another greenlet finishes execution.
    """
    partner = greenlet.spawn_later(seconds, func, *args, **kwargs)
    link_partner(greenlet, partner)
    return partner
