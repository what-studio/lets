# -*- coding: utf-8 -*-
import gc
import os
import random
import signal
import sys
import time
import weakref

import gevent
from gevent.event import Event
from gevent.pool import Group
import gipc
import psutil
import pytest

from lets import Processlet, Transparentlet, TransparentGroup


@pytest.fixture
def proc():
    return psutil.Process(os.getpid())


def return_args(*args, **kwargs):
    return args, kwargs


def kill_itself():
    os.kill(os.getpid(), signal.SIGKILL)


def busy_waiting(seconds=0.1):
    t = time.time()
    while time.time() - t < seconds:
        pass
    return 0


def divide_by_zero():
    1989 / 12 / 12 / 0


class Killed(BaseException):

    pass


def raise_when_killed(exception=Killed):
    try:
        while True:
            gevent.sleep(0)
    except gevent.GreenletExit:
        raise exception


def test_processlet_spawn_child_process():
    job = Processlet.spawn(os.getppid)
    assert job.exit_code is None
    assert job.pid != os.getpid()
    assert job.get() == os.getpid()
    assert job.exit_code == 0


def test_processlet_parellel_execution():
    t = time.time()
    jobs = [gevent.spawn(busy_waiting) for x in range(5)]
    gevent.joinall(jobs)
    delay = time.time() - t
    assert delay > 0.5
    for job in jobs:
        assert job.get() == 0
    t = time.time()
    jobs = [Processlet.spawn(busy_waiting) for x in range(5)]
    gevent.joinall(jobs)
    delay = time.time() - t
    assert delay < 0.2
    for job in jobs:
        assert job.get() == 0


def test_processlet_exception():
    job = Processlet.spawn(divide_by_zero)
    with pytest.raises(ZeroDivisionError):
        job.get()
    assert job.exit_code == 1


def test_processlet_exit():
    job = Processlet.spawn(kill_itself)
    with pytest.raises(SystemExit) as e:
        job.get()
    assert e.value.code == -signal.SIGKILL
    assert job.exit_code == -signal.SIGKILL
    job = Processlet.spawn(busy_waiting, 10)
    job.send(signal.SIGTERM)
    with pytest.raises(SystemExit) as e:
        job.get()
    assert e.value.code == -signal.SIGTERM
    assert job.exit_code == -signal.SIGTERM


def test_processlet_args():
    args = (1, 2)
    kwargs = {'x': 3, 'y': 4}
    job = Processlet.spawn(return_args, *args, **kwargs)
    assert job.get() == (args, kwargs)


def test_processlet_pipe_arg():
    with gipc.pipe() as (r, w):
        job = Processlet.spawn(type(w).put, w, 1)
        assert r.get() == 1
        job.join()


def test_processlet_without_start():
    job = Processlet(os.getppid)
    assert job.exit_code is None
    assert job.pid is None
    with pytest.raises(gevent.hub.LoopExit):
        job.join()
    with pytest.raises(gevent.hub.LoopExit):
        job.get()


def test_processlet_start_twice():
    job = Processlet(random.random)
    job.start()
    r1 = job.get()
    job.start()
    r2 = job.get()
    assert r1 == r2


def test_kill_processlet(proc):
    job = Processlet.spawn(raise_when_killed)
    job.join(0)
    assert len(proc.get_children()) == 1
    job.kill()
    assert len(proc.get_children()) == 0
    with pytest.raises(Killed):
        job.get()
    assert job.exit_code == 1


def test_kill_processlet_group(proc):
    group = Group()
    group.greenlet_class = Processlet
    group.spawn(raise_when_killed)
    group.spawn(raise_when_killed)
    group.spawn(raise_when_killed)
    group.join(0)
    assert len(proc.get_children()) == 3
    group.kill()
    assert len(proc.get_children()) == 0
    for job in group:
        with pytest.raises(Killed):
            job.get()
        assert job.exit_code == 1


def test_transparentlet():
    job = Transparentlet.spawn(divide_by_zero)
    with pytest.raises(ZeroDivisionError) as e:
        job.get()
    assert e.traceback[-1].name == 'divide_by_zero'


def test_killed_transparentlet():
    job = Transparentlet.spawn(divide_by_zero)
    job.kill(RuntimeError)
    with pytest.raises(RuntimeError):
        job.get()


def test_transparentlet_no_leak():
    ref = weakref.ref(Transparentlet.spawn(divide_by_zero))
    gc.collect()
    assert isinstance(ref(), Transparentlet)
    gevent.wait()
    gc.collect()
    assert ref() is None
    job = Transparentlet(divide_by_zero)
    assert sys.getrefcount(job) == 2  # variable 'job' (1) + argument (1)
    job.start()
    assert sys.getrefcount(job) == 3  # + hub (1)
    job.join()
    assert sys.getrefcount(job) == 7  # + gevent (3) + traceback (1)
    gevent.sleep(0)
    assert sys.getrefcount(job) == 3  # - gevent (3) - hub (1)
    ref = weakref.ref(job)
    del job
    gc.collect()
    assert ref() is None


def test_transparent_group():
    group = TransparentGroup()
    group.spawn(divide_by_zero)
    group.spawn(divide_by_zero)
    with pytest.raises(ZeroDivisionError) as e:
        group.join(raise_error=True)
    assert e.traceback[-1].name == 'divide_by_zero'
