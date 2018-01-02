# -*- coding: utf-8 -*-
import os
import signal

import gevent
from gevent.pool import Group
import psutil
import pytest

import lets


def pytest_addoption(parser):
    parser.addoption('--lets-timeout', default=10, metavar='SECONDS',
                     help='timeout for each lets test (default: 10 sec)')


def pytest_runtest_setup(item):
    lets_timeout = item.config.getoption('lets_timeout')
    item.timeout = gevent.Timeout.start_new(float(lets_timeout))


def pytest_runtest_teardown(item, nextitem):
    try:
        timeout = item.timeout
    except AttributeError:
        pass
    else:
        timeout.cancel()
    children = proc().children()
    for p in children:
        os.kill(p.pid, signal.SIGKILL)
    assert not proc().children()


group_names = ['greenlet_group', 'process_group']


@pytest.fixture(params=group_names)
def group(request):
    if request.param == 'greenlet_group':
        return Group()
    elif request.param == 'process_group':
        process_group = Group()
        process_group.greenlet_class = lets.Processlet
        return process_group


@pytest.fixture
def proc():
    return psutil.Process(os.getpid())


@pytest.fixture
def pipe():
    with lets.pipe() as (left, right):
        yield (left, right)


@pytest.fixture
def count_greenlets():
    # calibrate
    zero = gevent.get_hub().loop.activecnt
    def f():
        return gevent.get_hub().loop.activecnt - zero
    return f
