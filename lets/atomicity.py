# -*- coding: utf-8 -*-
"""
   lets.atomicity
   ~~~~~~~~~~~~~~

   :copyright: (c) 2013-2016 by Heungsub Lee
   :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from .utils import hub_replacer, HubWrapper


__all__ = ['atomic']


ATOMICITY_ERROR = AssertionError('Impossible to call blocking '
                                 'function on the atomic hub')


class AtomicHub(HubWrapper):

    def switch(self):
        raise ATOMICITY_ERROR


@hub_replacer
def atomic(hub):
    """Raises an :exc:`AssertionError` when a gevent blocking function called
    in the context.
    """
    yield AtomicHub(hub)
