# -*- coding: utf-8 -*-
import os
import signal

from gevent.pool import Group
import psutil
import pytest

import lets


def pytest_runtest_teardown(item):
    children = proc().children()
    for p in children:
        os.kill(p.pid, signal.SIGKILL)
    assert not children


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
