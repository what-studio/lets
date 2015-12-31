# -*- coding: utf-8 -*-
"""
   lets.slavery
   ~~~~~~~~~~~~

   Links 2 greenlets by the slavery.

   :copyright: (c) 2013-2015 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
from gevent import GreenletExit


__all__ = ['MasterGreenletExit', 'link_slave', 'spawn_slave',
           'spawn_slave_later', 'link_partner', 'spawn_partner',
           'spawn_partner_later']


class MasterGreenletExit(GreenletExit):
    """Slave greenlet should exit when the master greenlet is ready."""

    pass


def link_slave(greenlet, slave):
    """Links a greenlet greenlet and a slave greenlet.  Slave greenlet will be
    killed when the greenlet is ready.
    """
    def punish(greenlet):
        slave.unlink(liberate)
        slave.kill(MasterGreenletExit, block=False)
    def liberate(slave):
        greenlet.unlink(punish)
    greenlet.link(punish)
    slave.link(liberate)


def spawn_slave(greenlet, func, *args, **kwargs):
    """Spawns a slave greenlet.  Slave greenlet will be killed when the greenlet
    is ready.
    """
    slave = greenlet.spawn(func, *args, **kwargs)
    link_slave(greenlet, slave)
    return slave


def spawn_slave_later(greenlet, seconds, func, *args, **kwargs):
    """Spawns a slave greenlet the given seconds later.  Slave greenlet will be
    killed when the greenlet is ready.
    """
    slave = greenlet.spawn_later(seconds, func, *args, **kwargs)
    link_slave(greenlet, slave)
    return slave


def link_partner(greenlet, partner):
    """The greenlets will be killed when another greenlet is ready."""
    link_slave(greenlet, partner)
    link_slave(partner, greenlet)


def spawn_partner(greenlet, func, *args, **kwargs):
    """Spawns a partner greenlet. The greenlet and partner greenlets will die when
    another greenlet is ready.
    """
    partner = greenlet.spawn(func, *args, **kwargs)
    link_partner(greenlet, partner)
    return partner


def spawn_partner_later(greenlet, seconds, func, *args, **kwargs):
    """Spawns a partner greenlet the given seconds later. The greenlet and partner
    greenlets will die when another greenlet is ready.
    """
    partner = greenlet.spawn_later(seconds, func, *args, **kwargs)
    link_partner(greenlet, partner)
    return partner
