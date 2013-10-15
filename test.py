# -*- coding: utf-8 -*-
from contextlib import contextmanager
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

from lets import Processlet, ProcessPool, Transparentlet, TransparentGroup


@pytest.fixture
def proc():
    return psutil.Process(os.getpid())


@contextmanager
def killing(obj, exception=gevent.GreenletExit, block=True, timeout=None):
    try:
        yield obj
    finally:
        obj.kill(exception, block, timeout)


def return_args(*args, **kwargs):
    return args, kwargs


def kill_itself():
    os.kill(os.getpid(), signal.SIGKILL)


def busy_waiting(seconds=0.1):
    t = time.time()
    while time.time() - t < seconds:
        pass
    return seconds


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


def get_pid_anyway(*args, **kwargs):
    return os.getpid()


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
        assert job.get() == 0.1
    t = time.time()
    jobs = [Processlet.spawn(busy_waiting) for x in range(5)]
    gevent.joinall(jobs)
    delay = time.time() - t
    assert delay < 0.2
    for job in jobs:
        assert job.get() == 0.1


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


def test_processlet_kills_group(proc):
    group = Group()
    group.greenlet_class = Processlet
    def kill_group(greenlet):
        group.kill()
    group.spawn(gevent.sleep, 1)
    group.spawn(gevent.sleep, 1)
    group.spawn(divide_by_zero).link_exception(kill_group)
    group.join(0)
    assert len(group) == 3
    assert len(proc.get_children()) == 3
    group.join(0.5)
    assert len(group) == 0
    assert len(proc.get_children()) == 0


def test_process_pool_recycles_child_process(proc):
    assert len(proc.get_children()) == 0
    pool = ProcessPool(1)
    with killing(pool):
        pids = set()
        for x in xrange(10):
            pids.add(pool.spawn(os.getpid).get())
        assert len(pids) == 1
        assert next(iter(pids)) != os.getpid()
        assert len(proc.get_children()) == 1
    assert len(proc.get_children()) == 0


def test_process_pool_waits_worker_available(proc):
    assert len(proc.get_children()) == 0
    pool = ProcessPool(2)
    with killing(pool):
        with gevent.Timeout(0.1):
            pool.spawn(busy_waiting, 0.5)
            pool.spawn(busy_waiting, 0.5)
        with pytest.raises(gevent.Timeout):
            with gevent.Timeout(0.1):
                pool.spawn(busy_waiting, 0.5)
        pool.join()
        assert len(proc.get_children()) == 2
    assert len(proc.get_children()) == 0


def test_process_pool_apply(proc):
    assert len(proc.get_children()) == 0
    pool = ProcessPool(2)
    with killing(pool):
        pool.apply_async(busy_waiting, (0.2,))
        pool.apply_async(busy_waiting, (0.2,))
        pool.apply_async(busy_waiting, (0.2,))
        with gevent.Timeout(0.5):
            pool.join()
        assert len(proc.get_children()) == 2
    assert len(proc.get_children()) == 0


def test_process_pool_map(proc):
    assert len(proc.get_children()) == 0
    pool = ProcessPool(3)
    with killing(pool):
        assert pool.map(busy_waiting, [0.1] * 5) == [0.1] * 5
        assert len(set(pool.map(get_pid_anyway, range(100)))) == 3
        assert len(proc.get_children()) == 3
    assert len(proc.get_children()) == 0


@pytest.mark.xfail
def test_process_pool_without_size(proc):
    assert len(proc.get_children()) == 0
    pool = ProcessPool()
    with killing(pool):
        for x in range(10):
            pool.map(get_pid_anyway, range(x + 1))
            assert len(proc.get_children()) == x + 1
        for x in reversed(range(10)):
            pool.map(get_pid_anyway, range(x + 1))
            assert len(proc.get_children()) == 10
    assert len(proc.get_children()) == 0


def test_transparentlet():
    job = Transparentlet.spawn(divide_by_zero)
    with pytest.raises(ZeroDivisionError) as e:
        job.get()
    assert e.traceback[-1].name == 'divide_by_zero'


def test_transparentlet_doesnt_print_exception(capsys):
    job = Transparentlet.spawn(divide_by_zero)
    job.join()
    out, err = capsys.readouterr()
    assert not out
    assert not err


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


def test_transparent_group_ends_immediately_when_systemexit_occured():
    def f1():
        raise SystemExit(42)
    def f2():
        gevent.sleep(0.5)
    group = TransparentGroup()
    group.spawn(f1)
    group.spawn(f2)
    with gevent.Timeout(0.3):
        with pytest.raises(SystemExit) as e:
            group.join()
    assert e.value.code == 42
