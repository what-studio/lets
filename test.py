# -*- coding: utf-8 -*-
from contextlib import contextmanager
import gc
import multiprocessing
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

import lets
from lets.transparentlet import no_error_handling


group_names = ['greenlet_group', 'process_group', 'transparent_group']


@pytest.fixture(params=group_names)
def group(request):
    if request.param == 'greenlet_group':
        return Group()
    elif request.param == 'process_group':
        process_group = Group()
        process_group.greenlet_class = lets.Processlet
        return process_group
    elif request.param == 'transparent_group':
        return lets.TransparentGroup()


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


class ExpectedError(BaseException):

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
    job = lets.Processlet.spawn(os.getppid)
    job.join(0)
    assert job.exit_code is None
    assert job.pid != os.getpid()
    assert job.get() == os.getpid()
    assert job.exit_code == 0


def test_processlet_join_zero():
    job = lets.Processlet.spawn(busy_waiting)
    assert job.pid is None
    job.join(0)
    assert job.pid is not None
    job.join()


@pytest.mark.flaky(reruns=10)
def test_processlet_parellel_execution():
    t = time.time()
    jobs = [gevent.spawn(busy_waiting) for x in range(5)]
    gevent.joinall(jobs)
    delay = time.time() - t
    assert delay > 0.5
    for job in jobs:
        assert job.get() == 0.1
    t = time.time()
    jobs = [lets.Processlet.spawn(busy_waiting) for x in range(5)]
    gevent.joinall(jobs)
    delay = time.time() - t
    assert delay < 0.1 * multiprocessing.cpu_count()
    for job in jobs:
        assert job.get() == 0.1


def test_processlet_exception():
    job = lets.Processlet.spawn(divide_by_zero)
    with pytest.raises(ZeroDivisionError):
        job.get()
    assert job.exit_code == 1


def test_processlet_args():
    args = (1, 2)
    kwargs = {'x': 3, 'y': 4}
    job = lets.Processlet.spawn(return_args, *args, **kwargs)
    assert job.get() == (args, kwargs)


def test_processlet_pipe_arg():
    with gipc.pipe() as (r, w):
        job = lets.Processlet.spawn(type(w).put, w, 1)
        assert r.get() == 1
        job.join()


def test_processlet_without_start():
    job = lets.Processlet(os.getppid)
    assert job.exit_code is None
    assert job.pid is None
    with pytest.raises(gevent.hub.LoopExit):
        job.join()
    with pytest.raises(gevent.hub.LoopExit):
        job.get()


def test_processlet_start_twice():
    job = lets.Processlet(random.random)
    job.start()
    r1 = job.get()
    job.start()
    r2 = job.get()
    assert r1 == r2


def f_for_test_processlet_callback():
    gevent.sleep(0.5)
    0 / 0


def test_processlet_callback():
    pool = lets.ProcessPool(2)
    r = []
    with killing(pool):
        for x in range(10):
            job = pool.spawn(f_for_test_processlet_callback)
            job.link(lambda j: (j.join(), r.append(1)))
        pool.join()
    assert len(r) == 10


def test_kill_processlet(proc):
    job = lets.Processlet.spawn(raise_when_killed)
    job.join(0)
    assert len(proc.children()) == 1
    job.kill()
    assert len(proc.children()) == 0
    with pytest.raises(Killed):
        job.get()
    assert job.exit_code == 1


def test_kill_processlet_nonblock(proc):
    job = lets.Processlet.spawn(raise_when_killed)
    job.join(0)
    assert len(proc.children()) == 1
    job.kill(block=False)
    assert len(proc.children()) == 1
    with pytest.raises(Killed):
        job.get()
    assert len(proc.children()) == 0
    assert job.exit_code == 1


def test_kill_processlet_group(proc):
    group = Group()
    group.greenlet_class = lets.Processlet
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
    pool = lets.ProcessPool(1)
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
    pool = lets.ProcessPool(2)
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
    pool = lets.ProcessPool(2)
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
    pool = lets.ProcessPool(2)
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
    pool = lets.ProcessPool(3)
    with killing(pool):
        assert pool.map(busy_waiting, [0.1] * 5) == [0.1] * 5
        assert len(set(pool.map(get_pid_anyway, range(10)))) == 3
        assert len(proc.children()) == 3
    assert len(proc.children()) == 0


def test_process_pool_unlimited(proc):
    assert len(proc.children()) == 0
    pool = lets.ProcessPool()
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
    pool = lets.ProcessPool(2)
    with killing(pool):
        pids1 = pool.map(get_pid_anyway, range(2))
        pool.kill()
        pids2 = pool.map(get_pid_anyway, range(2))
        assert not set(pids1).intersection(pids2)
    assert len(proc.children()) == 0


def test_process_pool_raises(proc):
    assert len(proc.children()) == 0
    pool = lets.ProcessPool(1)
    with killing(pool):
        pid1 = pool.spawn(os.getpid).get()
        g = pool.spawn(divide_by_zero)
        with pytest.raises(ZeroDivisionError):
            g.get()
        pid2 = pool.spawn(os.getpid).get()
        assert pid1 == pid2
    assert len(proc.children()) == 0


def test_transparentlet():
    job = lets.Transparentlet.spawn(divide_by_zero)
    with pytest.raises(ZeroDivisionError) as e:
        job.get()
    assert e.traceback[-1].name == 'divide_by_zero'


def test_transparentlet_doesnt_print_exception(capsys):
    job = lets.Transparentlet.spawn(divide_by_zero)
    job.join()
    out, err = capsys.readouterr()
    assert not out
    assert not err


@pytest.mark.skipif(gevent.__version__ == '1.1a2',
                    reason='Killed greenlet of gevent-1.1a2 raises nothing')
def test_kill_transparentlet():
    job = lets.Transparentlet.spawn(divide_by_zero)
    job.kill()
    assert isinstance(job.get(), GreenletExit)
    job = lets.Transparentlet.spawn(divide_by_zero)
    job.kill(RuntimeError)
    with pytest.raises(RuntimeError):
        job.get()


def test_transparentlet_no_leak():
    ref = weakref.ref(lets.Transparentlet.spawn(divide_by_zero))
    gc.collect()
    assert isinstance(ref(), lets.Transparentlet)
    gevent.wait()
    gc.collect()
    assert ref() is None
    job = lets.Transparentlet(divide_by_zero)
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
    group = lets.TransparentGroup()
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
    job = lets.Processlet.spawn(kill_itself)
    gevent.spawn(gevent.sleep, 0.1).join()
    with pytest.raises(lets.ProcessExit) as e:
        job.get()
    assert e.value.code == -signal.SIGKILL
    assert job.exit_code == -signal.SIGKILL
    job = lets.Processlet.spawn(busy_waiting, 10)
    job.send(signal.SIGTERM)
    with pytest.raises(lets.ProcessExit) as e:
        job.get()
    assert e.value.code == -signal.SIGTERM
    assert job.exit_code == -signal.SIGTERM
    job = lets.Processlet.spawn(raise_when_killed, SystemExit(42))
    job.join(0)
    job.kill()
    with pytest.raises(lets.ProcessExit) as e:
        job.get()
    assert e.value.code == 42
    assert job.exit_code == 42


def test_processlet_exits_by_sigint():
    job = lets.Processlet.spawn(busy_waiting, 10)
    job.send(signal.SIGINT)
    job.join()
    assert isinstance(job.get(), gevent.GreenletExit)


def test_transparentlet_system_exit():
    job = lets.Transparentlet.spawn(sys.exit)
    gevent.spawn(gevent.sleep, 0.1).join()
    job.join()
    assert job.ready()
    assert not job.successful()
    assert isinstance(job.exception, SystemExit)


def test_object_pool():
    # getting object blocks
    pool = lets.ObjectPool(2, object)
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
    pool = lets.ObjectPool(None, object)
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
    pool = lets.ObjectPool(1, object)
    assert pool.available()
    with pool.reserve() as o:
        assert type(o) is object
        assert not pool.available()
    assert pool.available()


def test_object_pool_wait_available():
    pool = lets.ObjectPool(1, object)
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
    pool = lets.ObjectPool(3, f)
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
    pool = lets.ObjectPool(2, slow)
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
    queue = lets.JobQueue()
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
                              (3, [1, 4, 3, 2]), (4, [1, 4, 3, 2])]:
        queue = lets.JobQueue(workers=workers)
        queue.put(Greenlet(f, 1, 0.01))
        queue.put(Greenlet(f, 2, 0.06))
        queue.put(Greenlet(f, 3, 0.03))
        queue.put(Greenlet(f, 4, 0.01))
        queue.join()
        assert results == expected, '%d workers' % workers
        del results[:]


def test_job_queue_sized():
    results = []
    def f(x, delay=0):
        gevent.sleep(delay)
        results.append(x)
    queue = lets.JobQueue(2)
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
    queue = lets.JobQueue()
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


def test_job_queue_kill_with_error():
    def f():
        gevent.sleep(999)
    g = Greenlet(f)
    queue = lets.JobQueue()
    queue.put(g)
    queue.kill(ExpectedError)
    with pytest.raises(ExpectedError):
        g.get(block=False)


def test_job_queue_kill_before_working():
    def f():
        gevent.sleep(999)
    g = Greenlet(f)
    g.done = False
    g.link(lambda g: setattr(g, 'done', True))
    queue = lets.JobQueue()
    queue.put(g)
    assert not g.done
    queue.kill()
    assert g.done


def test_job_queue_guarantees_all_jobs():
    queue = lets.JobQueue()
    xs = []
    def f(x):
        gevent.sleep(0.01)
        xs.append(x)
    queue.put(Greenlet(f, 0))
    queue.put(Greenlet(f, 1))
    g = Greenlet(f, 2)
    queue.put(g)
    g.join()
    gevent.sleep(0)
    # the worker has done but the worker pool is still full.
    assert queue.worker_pool.full()
    # before 0.0.12, the final job won't be scheduled.
    queue.put(Greenlet(f, 3))
    queue.join()
    assert xs == [0, 1, 2, 3]


def test_link_slave():
    def slave():
        gevent.sleep(100)
    def master():
        master_g = gevent.getcurrent()
        slave_g = gevent.spawn(slave)
        lets.link_slave(master_g, slave_g)
        greenlets.append(slave_g)
        gevent.sleep(0.1)
        return 'success'
    greenlets = []
    greenlets.append(gevent.spawn(master))
    assert len(greenlets) == 1
    greenlets[0].join(0)
    assert len(greenlets) == 2
    gevent.joinall(greenlets)
    assert greenlets[0].value == 'success'
    assert isinstance(greenlets[1].value, lets.MasterGreenletExit)


def test_link_partner():
    def f(delay):
        gevent.sleep(delay)
        return 'ok'
    g1 = gevent.spawn(f, 10)
    g2 = gevent.spawn(f, 0.1)
    lets.link_partner(g1, g2)
    g2.join()
    assert g2.get() == 'ok'
    g1.join(timeout=0.1)
    assert g1.ready()
    assert isinstance(g1.get(), lets.MasterGreenletExit)
    g1 = gevent.spawn(f, 10)
    g2 = lets.spawn_partner(g1, f, 10)
    g1.kill()
    assert isinstance(g1.get(), gevent.GreenletExit)
    g2.join(timeout=0.1)
    assert g2.ready()
    assert isinstance(g2.get(), lets.MasterGreenletExit)


def test_spawn_slave():
    def slave():
        gevent.sleep(100)
    def master():
        master_g = gevent.getcurrent()
        greenlets.append(lets.spawn_slave(master_g, slave))
        gevent.sleep(0.1)
        return 'success'
    greenlets = []
    greenlets.append(gevent.spawn(master))
    assert len(greenlets) == 1
    greenlets[0].join(0)
    assert len(greenlets) == 2
    gevent.joinall(greenlets)
    assert greenlets[0].value == 'success'
    assert isinstance(greenlets[1].value, lets.MasterGreenletExit)


def test_spawn_slave_which_is_short_term():
    def slave():
        gevent.sleep(0.1)
        return 'slave'
    def master():
        master_g = gevent.getcurrent()
        greenlets.append(lets.spawn_slave(master_g, slave))
        gevent.sleep(0.3)
        return 'master'
    greenlets = []
    greenlets.append(gevent.spawn(master))
    assert len(greenlets) == 1
    greenlets[0].join(0)
    assert len(greenlets) == 2
    master_g, slave_g = greenlets
    assert len(master_g._links) == 1
    assert not master_g.ready()
    assert not slave_g.ready()
    slave_g.join()
    assert slave_g.ready()
    assert not master_g.ready()
    assert len(master_g._links) == 0
    assert slave_g.value == 'slave'
    master_g.join()
    assert master_g.value == 'master'


def test_spawn_slave_then_kill_master():
    def slave():
        gevent.sleep(100)
        return 'slave'
    def master():
        master_g = gevent.getcurrent()
        tmp.append(lets.spawn_slave(master_g, slave))
        gevent.sleep(100)
        return 'master'
    tmp = []
    master_g = gevent.spawn(master)
    master_g.join(0)
    slave_g = tmp[0]
    master_g.kill(ZeroDivisionError)
    slave_g.join()
    assert isinstance(master_g.exception, ZeroDivisionError)
    assert isinstance(slave_g.value, lets.MasterGreenletExit)


def test_spawn_slave_then_master_fails():
    def slave():
        gevent.sleep(100)
        return 'slave'
    def master():
        master_g = gevent.getcurrent()
        tmp.append(lets.spawn_slave(master_g, slave))
        gevent.sleep(0.1)
        0 / 0
    tmp = []
    master_g = gevent.spawn(master)
    master_g.join(0)
    slave_g = tmp[0]
    master_g.kill(ZeroDivisionError)
    slave_g.join()
    assert isinstance(master_g.exception, ZeroDivisionError)
    assert isinstance(slave_g.value, lets.MasterGreenletExit)


def nested_greenlets(structure, root_timeout=None, leaf_timeout=100):
    # Infer the root num.
    root_num_candidates = set(structure.keys())
    for slave_nums in structure.values():
        root_num_candidates -= set(slave_nums)
    if len(root_num_candidates) != 1:
        raise ValueError('Root number inference failed')
    num = list(root_num_candidates)[0]
    started, finished, greenlets = [], [], []
    def f(num, slaves=(), timeout=None):
        started.append(num)
        try:
            if slaves:
                return lets.join_slaves(slaves, timeout=timeout)
            else:
                gevent.sleep(leaf_timeout)
                return ()
        finally:
            finished.append(num)
    def spawn(num, timeout=None):
        slaves = []
        g = gevent.spawn(f, num, slaves, timeout)
        greenlets.append(g)
        try:
            slave_nums = structure[num]
        except KeyError:
            pass
        else:
            for num in slave_nums:
                slaves.append(spawn(num))
        return g
    # Spawn greenlets.
    g = spawn(num, root_timeout)
    g.join(0)
    return started, finished, greenlets


def test_join_slaves():
    # 1--> 2--> 3
    started, finished, (g1, g2, g3) = nested_greenlets({1: [2], 2: [3]})
    assert started == [1, 2, 3]
    assert not g3.ready()
    assert finished == []
    g1.join(0.1)
    assert not g3.ready()
    assert finished == []
    g1.kill()
    assert g3.ready()
    assert finished == [3, 2, 1]
    # 1-+-> 2--> 3
    #   |
    #   +-> 4--> 5
    started, finished, (g1, g2, g3, g4, g5) = \
        nested_greenlets({1: [2, 4], 2: [3], 4: [5]})
    assert started == [1, 2, 3, 4, 5]
    g2.kill()
    assert g3.ready()
    assert finished == [3, 2]
    assert not g1.ready()
    g4.kill()
    g1.join(0)
    assert g1.ready()
    assert finished == [3, 2, 5, 4, 1]
    # 1--> 2--> 3 but finishes normally.
    started, finished, (g1, g2, g3) = \
        nested_greenlets({1: [2], 2: [3]}, leaf_timeout=0.1)
    assert started == [1, 2, 3]
    assert finished == []
    g1.join()
    assert len(g1.get()) == 1
    assert finished == [3, 2, 1]
    # 1--> 2--> 3 but killed by another exception.
    started, finished, (g1, g2, g3) = nested_greenlets({1: [2], 2: [3]})
    assert started == [1, 2, 3]
    g1.kill(ZeroDivisionError)
    assert finished == [3, 2, 1]
    assert g1.ready()
    assert not g1.successful()
    with pytest.raises(ZeroDivisionError):
        g1.get()
    assert g2.successful()
    assert g3.successful()
    # 1--> 2--> 3 but with tiemout.
    started, finished, (g1, g2, g3) = \
        nested_greenlets({1: [2], 2: [3]}, root_timeout=0.1)
    assert len(g1.get()) == 0
    assert finished == [1]
    assert not g2.ready()
    assert not g3.ready()
    g2.kill()
    assert finished == [1, 3, 2]
    # 1--> 2--> 3 but killed within tiemout.
    started, finished, (g1, g2, g3) = \
        nested_greenlets({1: [2], 2: [3]}, root_timeout=1)
    g1.join(0.1)
    g1.kill(ZeroDivisionError)
    assert not g2.ready()
    assert not g3.ready()
    g2.join()
    assert g1.ready()
    assert g2.ready()
    assert g3.ready()
    with pytest.raises(ZeroDivisionError):
        g1.get()
    assert isinstance(g2.get(), lets.MasterGreenletExit)
    assert finished == [1, 3, 2]
