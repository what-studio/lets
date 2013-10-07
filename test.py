# -*- coding: utf-8 -*-
import time

import gevent
import pytest

from lets import Processlet, Transparentlet, TransparentGroup


def busy_waiting():
    t = time.time()
    while time.time() - t < 0.1:
        pass
    return 1


def divide_by_zero():
    1989 / 12 / 12 / 0


def test_processlet():
    t = time.time()
    jobs = [gevent.spawn(busy_waiting) for x in range(5)]
    gevent.joinall(jobs)
    assert time.time() - t > 0.5
    t = time.time()
    jobs = [Processlet.spawn(busy_waiting) for x in range(5)]
    gevent.joinall(jobs)
    assert time.time() - t < 0.2


def test_transparentlet():
    job = Transparentlet.spawn(divide_by_zero)
    with pytest.raises(ZeroDivisionError) as e:
        job.get()
    assert e.traceback[-1].name == 'divide_by_zero'


def test_transparent_group():
    group = TransparentGroup()
    group.spawn(divide_by_zero)
    group.spawn(divide_by_zero)
    with pytest.raises(ZeroDivisionError) as e:
        group.join(raise_error=True)
    assert e.traceback[-1].name == 'divide_by_zero'
