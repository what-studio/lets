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
from gevent import Greenlet, GreenletExit, Timeout
from gevent.pool import Group
from gevent.queue import Full
import gipc
import psutil
import pytest

from lets import (
    JobQueue, ObjectPool, Processlet, ProcessExit, ProcessPool,
    Transparentlet, TransparentGroup)
from lets.transparentlet import no_error_handling


def pytest_generate_tests(metafunc):
    if 'group' in metafunc.fixturenames:
        greenlet_group = Group()
        process_group = Group()
        process_group.greenlet_class = Processlet
        transparent_group = TransparentGroup()
        groups = [greenlet_group, process_group, transparent_group]
        ids = ['greenlet_group', 'process_group', 'transparent_group']
        metafunc.parametrize('group', groups, ids=ids)


@pytest.fixture
def proc():
    return psutil.Process(os.getpid())


@contextmanager
def killing(obj, exception=GreenletExit, block=True, timeout=None):
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


def throw(exception):
    raise exception


class Killed(BaseException):

    pass


def raise_when_killed(exception=Killed):
    try:
        while True:
            gevent.sleep(0)
    except GreenletExit:
        raise exception


def get_pid_anyway(*args, **kwargs):
    # ProcessGroup.map may don't spawn enough processlets when calling too fast
    # function.
    gevent.sleep(0.1)
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
    assert len(proc.children()) == 1
    job.kill()
    assert len(proc.children()) == 0
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
    assert len(proc.children()) == 3
    group.kill()
    assert len(proc.children()) == 0
    for job in group:
        with pytest.raises(Killed):
            job.get()
        assert job.exit_code == 1


def test_process_pool_recycles_child_process(proc):
    assert len(proc.children()) == 0
    pool = ProcessPool(1)
    with killing(pool):
        pids = set()
        for x in xrange(10):
            pids.add(pool.spawn(os.getpid).get())
        assert len(pids) == 1
        assert next(iter(pids)) != os.getpid()
        assert len(proc.children()) == 1
    assert len(proc.children()) == 0


def test_process_pool_size_limited(proc):
    assert len(proc.children()) == 0
    pool = ProcessPool(2)
    with killing(pool):
        pool.spawn(busy_waiting, 0.1)
        pool.spawn(busy_waiting, 0.2)
        pool.spawn(busy_waiting, 0.1)
        pool.spawn(busy_waiting, 0.2)
        pool.spawn(busy_waiting, 0.1)
        pool.join(0)
        assert len(proc.children()) == 2
        pool.join()
    assert len(proc.children()) == 0


def test_process_pool_waits_worker_available(proc):
    assert len(proc.children()) == 0
    pool = ProcessPool(2)
    with killing(pool):
        with Timeout(0.1):
            pool.spawn(busy_waiting, 0.5)
            pool.spawn(busy_waiting, 0.5)
        with pytest.raises(Timeout):
            with Timeout(0.1):
                pool.spawn(busy_waiting, 0.5)
        pool.join()
        assert len(proc.children()) == 2
    assert len(proc.children()) == 0


def test_process_pool_apply(proc):
    assert len(proc.children()) == 0
    pool = ProcessPool(2)
    with killing(pool):
        pool.apply_async(busy_waiting, (0.2,))
        pool.apply_async(busy_waiting, (0.2,))
        pool.apply_async(busy_waiting, (0.2,))
        with Timeout(0.5):
            pool.join()
        assert len(proc.children()) == 2
    assert len(proc.children()) == 0


def test_process_pool_map(proc):
    assert len(proc.children()) == 0
    pool = ProcessPool(3)
    with killing(pool):
        assert pool.map(busy_waiting, [0.1] * 5) == [0.1] * 5
        assert len(set(pool.map(get_pid_anyway, range(10)))) == 3
        assert len(proc.children()) == 3
    assert len(proc.children()) == 0


def test_process_pool_unlimited(proc):
    assert len(proc.children()) == 0
    pool = ProcessPool()
    with killing(pool):
        for x in range(5):
            pids = pool.map(get_pid_anyway, range(x + 1))
            assert len(pids) == x + 1
            assert len(proc.children()) == x + 1
        for x in reversed(range(5)):
            pids = pool.map(get_pid_anyway, range(x + 1))
            assert len(pids) == x + 1
            assert len(proc.children()) == 5
    assert len(proc.children()) == 0


def test_process_pool_respawns_worker(proc):
    assert len(proc.children()) == 0
    pool = ProcessPool(2)
    with killing(pool):
        pids1 = pool.map(get_pid_anyway, range(2))
        pool.kill()
        pids2 = pool.map(get_pid_anyway, range(2))
        assert not set(pids1).intersection(pids2)
    assert len(proc.children()) == 0


def test_process_pool_raises(proc):
    assert len(proc.children()) == 0
    pool = ProcessPool(1)
    with killing(pool):
        pid1 = pool.spawn(os.getpid).get()
        g = pool.spawn(divide_by_zero)
        with pytest.raises(ZeroDivisionError):
            g.get()
        pid2 = pool.spawn(os.getpid).get()
        assert pid1 == pid2
    assert len(proc.children()) == 0


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


def test_kill_transparentlet():
    job = Transparentlet.spawn(divide_by_zero)
    job.kill()
    assert isinstance(job.get(), GreenletExit)
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


def test_greenlet_exit(group):
    g = group.spawn(gevent.sleep, 1)
    g.kill()
    assert g.ready()
    assert g.successful()
    assert isinstance(g.get(), GreenletExit)


def test_task_kills_group(proc, group):
    def f1():
        gevent.sleep(0.1)
        raise RuntimeError
    def f2():
        try:
            gevent.sleep(10)
        except RuntimeError:
            raise Killed
    def f3():
        gevent.sleep(10)
    g1 = group.spawn(f1)
    g1.link_exception(lambda g: group.kill(g.exception))
    g2 = group.spawn(f2)
    g3 = group.spawn(f3)
    with pytest.raises((RuntimeError, Killed)):
        group.join(raise_error=True)
    assert len(proc.children()) == 0
    assert not group.greenlets
    assert g1.ready()
    assert g2.ready()
    assert g3.ready()
    assert not g1.successful()
    assert not g2.successful()
    assert not g3.successful()
    assert isinstance(g1.exception, RuntimeError)
    assert isinstance(g2.exception, Killed)
    assert isinstance(g3.exception, RuntimeError)


def test_no_error_handling(capsys):
    # print exception
    gevent.spawn(divide_by_zero).join()
    out, err = capsys.readouterr()
    assert 'ZeroDivisionError' in err
    # don't print
    with no_error_handling():
        gevent.spawn(divide_by_zero).join()
    out, err = capsys.readouterr()
    assert not err
    # don't print also
    gevent.spawn(divide_by_zero)
    with no_error_handling():
        gevent.wait()
    out, err = capsys.readouterr()
    assert not err


def test_greenlet_system_exit():
    gevent.spawn(sys.exit)
    with pytest.raises(SystemExit):
        gevent.spawn(gevent.sleep, 0.1).join()


def test_processlet_system_exit():
    job = Processlet.spawn(kill_itself)
    gevent.spawn(gevent.sleep, 0.1).join()
    with pytest.raises(ProcessExit) as e:
        job.get()
    assert e.value.code == -signal.SIGKILL
    assert job.exit_code == -signal.SIGKILL
    job = Processlet.spawn(busy_waiting, 10)
    job.send(signal.SIGTERM)
    with pytest.raises(ProcessExit) as e:
        job.get()
    assert e.value.code == -signal.SIGTERM
    assert job.exit_code == -signal.SIGTERM
    job = Processlet.spawn(raise_when_killed, SystemExit(42))
    job.join(0)
    job.kill()
    with pytest.raises(ProcessExit) as e:
        job.get()
    assert e.value.code == 42
    assert job.exit_code == 42


def test_transparentlet_system_exit():
    job = Transparentlet.spawn(sys.exit)
    gevent.spawn(gevent.sleep, 0.1).join()
    job.join()
    assert job.ready()
    assert not job.successful()
    assert isinstance(job.exception, SystemExit)


def test_object_pool():
    # getting object blocks
    pool = ObjectPool(2, object)
    assert pool.available()
    o1 = pool.get()
    assert pool.available()
    o2 = pool.get()
    assert not pool.available()
    with pytest.raises(gevent.hub.LoopExit):
        pool.get()
    assert o1 is not o2
    assert len(pool.objects) == 2
    # release and get again
    pool.release(o1)
    assert pool.available()
    o3 = pool.get()
    assert not pool.available()
    assert o1 is o3
    assert len(pool.objects) == 2
    # discard
    pool.discard(o2)
    o4 = pool.get()
    assert not pool.available()
    assert o2 is not o4
    pool.discard(o4)
    assert pool.available()


def test_object_pool_unlimited():
    # getting object blocks
    pool = ObjectPool(None, object)
    assert pool.available()
    o1 = pool.get()
    assert pool.available()
    o2 = pool.get()
    assert pool.available()
    assert o1 is not o2
    assert len(pool.objects) == 2
    # release and get again
    pool.release(o1)
    o3 = pool.get()
    assert o1 is o3
    o4 = pool.get()
    assert o4 is not o1
    assert o4 is not o2
    assert len(pool.objects) == 3
    # discard
    pool.discard(o2)
    o5 = pool.get()
    assert o2 is not o5


def test_object_pool_context():
    pool = ObjectPool(1, object)
    assert pool.available()
    with pool.reserve() as o:
        assert type(o) is object
        assert not pool.available()
    assert pool.available()


def test_object_pool_wait_available():
    pool = ObjectPool(1, object)
    o = pool.get()
    waiting_avaiable = gevent.spawn(pool.wait_available)
    waiting_avaiable.join(0.1)
    assert not waiting_avaiable.ready()
    pool.release(o)
    waiting_avaiable.join(0.1)
    assert waiting_avaiable.ready()


def test_object_pool_rotation():
    counter = [0]
    def f():
        counter[0] += 1
        return counter[0]
    pool = ObjectPool(3, f)
    assert pool.get() == 1
    assert pool.get() == 2
    assert pool.get() == 3
    pool.release(1)
    pool.release(2)
    pool.release(3)
    assert pool.get() == 1
    pool.discard(2)
    assert pool.get() == 3
    assert pool.get() == 4
    pool.discard(1)
    pool.release(1)
    assert pool.get() == 5
    assert not pool.available()


def test_object_pool_with_slow_behaviors():
    def slow():
        gevent.sleep(0.1)
        return object()
    pool = ObjectPool(2, slow)
    def consume_obj_from_pool():
        with pool.reserve():
            gevent.sleep(0.1)
    gevent.joinall([gevent.spawn(consume_obj_from_pool) for x in range(10)])
    assert len(pool.objects) == 2


def test_job_queue():
    results = []
    def f(x, delay=0):
        gevent.sleep(delay)
        results.append(x)
    queue = JobQueue()
    with pytest.raises(ValueError):
        queue.put(Greenlet.spawn())
    queue.put(Greenlet(f, 1, 0.1))
    queue.put(Greenlet(f, 2, 0))
    queue.put(Greenlet(f, 3, 0.2))
    queue.put(Greenlet(f, 4, 0.1))
    queue.join()
    assert results == [1, 2, 3, 4]


def test_job_queue_with_multiple_workers():
    results = []
    def f(x, delay=0):
        gevent.sleep(delay)
        results.append(x)
    for workers, expected in [(1, [1, 2, 3, 4]), (2, [1, 3, 4, 2]),
                              (3, [1, 3, 4, 2]), (4, [1, 4, 3, 2])]:
        queue = JobQueue(workers=workers)
        queue.put(Greenlet(f, 1, 0.1))
        queue.put(Greenlet(f, 2, 0.5))
        queue.put(Greenlet(f, 3, 0.2))
        queue.put(Greenlet(f, 4, 0.1))
        queue.join()
        assert results == expected
        del results[:]


def test_job_queue_sized():
    results = []
    def f(x, delay=0):
        gevent.sleep(delay)
        results.append(x)
    queue = JobQueue(2)
    queue.put(Greenlet(f, 1, 0.1))
    queue.put(Greenlet(f, 2, 0.1))
    queue.put(Greenlet(f, 3, 0.1))
    with pytest.raises(Full):
        queue.put(Greenlet(f, 4, 0.1), timeout=0.01)
    with pytest.raises(Full):
        queue.put(Greenlet(f, 5, 0.1), block=False)
    queue.join()
    assert results == [1, 2, 3]


def test_job_queue_exited():
    results = []
    def f(x, delay=0):
        gevent.sleep(delay)
        results.append(x)
        return x
    queue = JobQueue()
    g1 = Greenlet(f, 1, 0.1)
    g2 = Greenlet(f, 2, 0.1)
    queue.put(g1)
    queue.put(g2)
    g1.join()
    queue.kill()
    queue.join()
    assert results == [1]
    assert g1.get() == 1
    assert isinstance(g2.get(), gevent.GreenletExit)
