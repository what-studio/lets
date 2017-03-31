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
from gevent.event import AsyncResult, Event
from gevent.lock import Semaphore
from gevent.pool import Group
from gevent.queue import Channel, Full
import pytest

import lets
from lets.quietlet import quiet


@contextmanager
def takes(seconds, tolerance=0.1):
    t = time.time()
    yield
    e = time.time() - t
    if not (seconds - tolerance <= e <= seconds + tolerance):
        raise AssertionError('%r seconds expected but %r '
                             'seconds taken' % (seconds, e))


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


def spawn_again(f, *args, **kwargs):
    return gevent.spawn(lambda: gevent.spawn(f, *args, **kwargs).get())


class Killed(BaseException):

    pass


class ExpectedError(BaseException):

    pass


class Error1(Exception):
    pass


class Error2(Exception):
    pass


def notify_entry(hole, f, *args, **kwargs):
    g = lets.Quietlet.spawn(f, *args, **kwargs)
    g.join(0)
    try:
        hole.put(True)
        return g.get()
    except BaseException as exc:
        g.kill(exc)
        return g.get()


def raise_when_killed(exception=Killed):
    try:
        while True:
            gevent.sleep(0)
    except BaseException as exc:
        if exception is None:
            raise exc
        else:
            raise exception


def get_pid_anyway(*args, **kwargs):
    # ProcessGroup.map may don't spawn enough processlets when calling too fast
    # function.
    gevent.sleep(0.1)
    return os.getpid()


def test_processlet_spawn_child_process():
    job = lets.Processlet.spawn(os.getppid)
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
def test_processlet_parallel_execution():
    # NOTE: Here's a potential hang.
    cpu_count = multiprocessing.cpu_count()
    if cpu_count < 2:
        pytest.skip('CPU not enough')
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
    assert delay < 0.1 * cpu_count
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


# def test_processlet_pipe_arg():
#     with gipc.pipe() as (r, w):
#         job = lets.Processlet.spawn(type(w).put, w, 1)
#         assert r.get() == 1
#         job.join()


def test_processlet_without_start():
    job = lets.Processlet(os.getppid)
    assert job.exit_code is None
    assert job.pid is None
    with pytest.raises(gevent.Timeout), gevent.Timeout(0.1):
        job.join()
    with pytest.raises(gevent.Timeout):
        job.get(timeout=0.1)


def test_processlet_unref():
    assert gevent.get_hub().loop.activecnt == 0
    job = lets.Processlet.spawn(os.getppid)
    assert gevent.get_hub().loop.activecnt == 1
    job.join()
    assert gevent.get_hub().loop.activecnt == 0


def test_processlet_start_twice():
    job = lets.Processlet(random.random)
    job.start()
    r1 = job.get()
    job.start()
    r2 = job.get()
    assert r1 == r2


# def f_for_test_processlet_callback():
#     gevent.sleep(0.5)
#     0 / 0


# def test_processlet_callback():
#     pool = lets.ProcessPool(2)
#     r = []
#     with killing(pool):
#         for x in range(10):
#             job = pool.spawn(f_for_test_processlet_callback)
#             job.link(lambda j: (j.join(), r.append(1)))
#         pool.join()
#     assert len(r) == 10


def test_kill_processlet_before_starting(proc):
    job = lets.Processlet.spawn(raise_when_killed)
    assert len(proc.children()) == 0
    job.kill()
    assert isinstance(job.get(), GreenletExit)
    assert len(proc.children()) == 0


def test_kill_processlet_after_starting(proc, pipe):
    p, c = pipe
    job = lets.Processlet.spawn(notify_entry, c, raise_when_killed)
    assert len(proc.children()) == 0
    assert p.get() is True
    assert len(proc.children()) == 1
    job.kill()
    assert len(proc.children()) == 0
    with pytest.raises(Killed):
        job.get()
    assert job.exit_code == 1


def test_kill_processlet(proc):
    job = lets.Processlet.spawn(raise_when_killed)
    job.join(0)
    assert len(proc.children()) == 1
    job.kill()
    assert len(proc.children()) == 0
    with pytest.raises(Killed):
        job.get()
    assert job.exit_code == 1


def test_kill_processlet_busy(proc):
    # without killing signal
    job = lets.Processlet.spawn(busy_waiting, 60)
    job.join(0)
    assert len(proc.children()) == 1
    with pytest.raises(gevent.Timeout), gevent.Timeout(0.5):
        job.kill()
    assert len(proc.children()) == 1
    job.send(signal.SIGKILL)
    job.join()
    assert len(proc.children()) == 0
    # with killing signal
    job = lets.Processlet.spawn(signal.SIGUSR1, busy_waiting, 60)
    job.join(0)
    assert len(proc.children()) == 1
    job.kill(Killed)
    assert len(proc.children()) == 0
    with pytest.raises(Killed):
        job.get()
    assert job.exit_code == 1


def test_kill_processlet_after_join_another_processlet(proc):
    job1 = lets.Processlet.spawn(gevent.sleep, 1)
    job2 = lets.Processlet.spawn(gevent.sleep, 3)
    job1.join(0.1)
    with gevent.Timeout(5, AssertionError('job2.kill() timed out')):
        job2.kill()
    assert job2.ready()
    job1.join()
    assert job1.ready()


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


def test_joinall_processlets():
    p1 = lets.Processlet.spawn(lambda: gevent.sleep(1))
    p2 = lets.Processlet.spawn(lambda: 0 / 0)
    with pytest.raises(ZeroDivisionError):
        gevent.joinall([p1, p2], raise_error=True)
    assert not p1.ready()
    assert p2.ready()
    with takes(1):
        assert p1.get() is None


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


@contextmanager
def raises_from(exc_type, code_name):
    try:
        yield
    except exc_type:
        __, __, tb = sys.exc_info()
        while tb.tb_next is not None:
            tb = tb.tb_next
        assert tb.tb_frame.f_code.co_name == code_name
    else:
        raise AssertionError('Expected error not raised')


def test_quietlet():
    job = lets.Quietlet.spawn(divide_by_zero)
    with raises_from(ZeroDivisionError, 'divide_by_zero'):
        job.get()


def test_quietlet_doesnt_print_exception(capsys):
    job = lets.Quietlet.spawn(divide_by_zero)
    job.join()
    out, err = capsys.readouterr()
    assert not out
    assert not err


@pytest.mark.skipif(gevent.__version__ == '1.1a2',
                    reason='Killed greenlet of gevent-1.1a2 raises nothing')
def test_kill_quietlet():
    job = lets.Quietlet.spawn(divide_by_zero)
    job.kill()
    assert isinstance(job.get(), GreenletExit)
    job = lets.Quietlet.spawn(divide_by_zero)
    job.kill(RuntimeError)
    with pytest.raises(RuntimeError):
        job.get()


@pytest.fixture
def ref_count():
    # try:
    #     # CPython
    #     return sys.getrefcount
    # except AttributeError:
    # PyPy
    def count_referrers(x):
        gc.collect()
        refs = gc.get_referrers(x)
        refs = reduce(lambda refs, r: refs.append(r) or refs
                      if r not in refs else refs, refs, [])
        return len(refs)
    return count_referrers


def _test_quietlet_no_leak(ref_count):
    ref = weakref.ref(lets.Quietlet.spawn(divide_by_zero))
    gc.collect()
    assert isinstance(ref(), lets.Quietlet)
    gevent.wait()
    gc.collect()
    assert ref() is None
    job = lets.Quietlet(divide_by_zero)
    assert ref_count(job) == 2  # variable 'job' (1) + argument (1)
    job.start()
    assert ref_count(job) == 3  # + hub (1)
    job.join()
    assert ref_count(job) == 6  # + gevent (3)
    gevent.sleep(0)
    assert ref_count(job) == 2  # - gevent (3) - hub (1)
    ref = weakref.ref(job)
    del job
    gc.collect()
    assert ref() is None


def test_quiet_group():
    from gevent.pool import Group
    group = Group()
    group.spawn(divide_by_zero)
    group.spawn(divide_by_zero)
    with raises_from(ZeroDivisionError, 'divide_by_zero'):
        group.join(raise_error=True)


def test_greenlet_exit(group):
    g = group.spawn(gevent.sleep, 1)
    g.kill()
    assert g.ready()
    assert g.successful()
    assert isinstance(g.get(), GreenletExit)


def test_task_kills_group(proc, group):
    def f1():
        gevent.sleep(0.1)
        raise Error1
    def f2():
        try:
            gevent.sleep(100)
        except Error1:
            raise Error2
    def f3():
        gevent.sleep(100)
    g1 = group.spawn(f1)
    g1.link_exception(lambda g: group.kill(g.exception))
    g2 = group.spawn(f2)
    g3 = group.spawn(f3)
    with pytest.raises((Error1, Error2)):
        group.join(raise_error=True)
    assert len(proc.children()) == 0
    assert not group.greenlets
    assert g1.ready()
    assert g2.ready()
    assert g3.ready()
    assert not g1.successful()
    assert not g2.successful()
    assert not g3.successful()
    assert isinstance(g1.exception, Error1)
    assert isinstance(g2.exception, Error2)
    assert isinstance(g3.exception, Error1)


def _test_quiet_context(capsys):
    # Print the exception.
    gevent.spawn(divide_by_zero).join()
    out, err = capsys.readouterr()
    assert 'ZeroDivisionError' in err
    # Don't print.
    with quiet():
        gevent.spawn(divide_by_zero).join()
    out, err = capsys.readouterr()
    assert not err
    # Don't print also.
    # gevent.spawn(divide_by_zero)
    # with quiet():
    #     gevent.wait()
    # out, err = capsys.readouterr()
    # assert not err
    # quiet() context in a greenlet doesn't print also.
    def f():
        with quiet():
            gevent.spawn(divide_by_zero).join()
    gevent.spawn(f).join()
    out, err = capsys.readouterr()
    assert not err
    # Out of quiet() context in a greenlet prints.
    def f():
        with quiet():
            gevent.spawn(divide_by_zero).join()
        0 / 0
    gevent.spawn(f).join()
    out, err = capsys.readouterr()
    assert 'ZeroDivisionError' in err
    # quiet() context with a greenlet doesn't print.
    g = gevent.spawn(divide_by_zero)
    with quiet(g):
        g.join()
    out, err = capsys.readouterr()
    assert not err
    # Remaining greenlet.
    with quiet():
        g = gevent.spawn(gevent.sleep, 0.1)
    gevent.sleep(1)


def test_greenlet_system_exit():
    gevent.spawn(sys.exit)
    with pytest.raises(SystemExit):
        gevent.spawn(gevent.sleep, 0.1).join()


def test_processlet_system_exit(pipe):
    job = lets.Processlet.spawn(kill_itself)
    gevent.spawn(gevent.sleep, 0.1).join()
    with pytest.raises(lets.ProcessExit) as e:
        job.get()
    assert e.value.code == -signal.SIGKILL
    assert job.exit_code == -signal.SIGKILL
    gevent.sleep(1)
    job = lets.Processlet.spawn(busy_waiting, 10)
    job.join(0)
    os.kill(job.pid, signal.SIGTERM)
    with pytest.raises(lets.ProcessExit) as e:
        job.get()
    assert e.value.code == -signal.SIGTERM
    assert job.exit_code == -signal.SIGTERM
    p, c = pipe
    job = lets.Processlet.spawn(notify_entry, c, raise_when_killed,
                                SystemExit(42))
    p.get()
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


def test_processlet_pause_and_resume(pipe):
    p, c = pipe
    job = lets.Processlet.spawn(notify_entry, c, lambda: gevent.sleep(2) or 42)
    p.get()
    os.kill(job.pid, signal.SIGSTOP)
    gevent.sleep(1)
    with pytest.raises(gevent.Timeout), gevent.Timeout(3):
        job.join()
    os.kill(job.pid, signal.SIGCONT)
    assert job.get() == 42


def test_processlet_kill_kill():
    job = lets.Processlet.spawn(raise_when_killed, exception=None)
    job.join(0)
    job.kill(Error1, block=False)
    job.kill(Error2, block=False)
    with pytest.raises(Error1):
        job.get()


def test_quietlet_system_exit():
    job = lets.Quietlet.spawn(sys.exit)
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
    with pytest.raises(gevent.Timeout):
        pool.get(timeout=0.1)
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
    # before 0.0.23, the worker has done but the worker pool is still full.
    # before 0.0.12, the final job won't be scheduled.
    queue.put(Greenlet(f, 3))
    queue.join()
    assert xs == [0, 1, 2, 3]


def test_job_queue_close_and_forget():
    def f(xs, x):
        gevent.sleep(1)
        xs.append(x)
    # close, join.
    queue1, xs1 = lets.JobQueue(), []
    queue1.put(Greenlet(f, xs1, 0))
    queue1.put(Greenlet(f, xs1, 1))
    queue1.put(Greenlet(f, xs1, 2))
    queue1.put(Greenlet(f, xs1, 3))
    queue1.close()
    with pytest.raises(RuntimeError):
        queue1.put(Greenlet(f, xs1, 4))
    queue1.join()
    assert xs1 == [0, 1, 2, 3]
    # close, forget, join.
    queue2, xs2 = lets.JobQueue(), []
    queue2.put(Greenlet(f, xs2, 0))
    queue2.put(Greenlet(f, xs2, 1))
    queue2.put(Greenlet(f, xs2, 2))
    queue2.put(Greenlet(f, xs2, 3))
    queue2.close()
    with pytest.raises(RuntimeError):
        queue2.put(Greenlet(f, xs2, 4))
    queue2.join(0)
    queue2.forget()
    queue2.join()
    assert xs2 == [0]


def test_job_queue_join_workers():
    ok = Event()
    q = lets.JobQueue()
    g = q.put(Greenlet(gevent.sleep, 0.1))
    g.link(lambda g: ok.set())
    # Before 0.0.24, JobQueue.join() doesn't guarantee finish of all workers.
    q.join()
    assert ok.is_set()


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


def test_join_slaves_without_greenlets():
    # It was timed out at lets-0.0.17
    with gevent.Timeout(0.1, AssertionError('lets.join_slaves() hangs')):
        lets.join_slaves([], timeout=1)


def _test_atomic():
    # NOTE: Nested context by comma is not available in Python 2.6.
    # o -- No gevent.
    with lets.atomic():
        1 + 2 + 3
    # x -- gevent.sleep()
    with pytest.raises(AssertionError):
        with lets.atomic():
            gevent.sleep(0.1)
    # x -- gevent.sleep() with 0 seconds.
    with pytest.raises(AssertionError):
        with lets.atomic():
            gevent.sleep(0)
    # o -- Greenlet.spawn()
    with lets.atomic():
        gevent.spawn(gevent.sleep, 0.1)
    # x -- Greenlet.join()
    with pytest.raises(AssertionError):
        with lets.atomic():
            g = gevent.spawn(gevent.sleep, 0.1)
            g.join()
    # x -- Greenlet.get()
    with pytest.raises(AssertionError):
        with lets.atomic():
            g = gevent.spawn(gevent.sleep, 0.1)
            g.get()
    # x -- gevent.joinall()
    with pytest.raises(AssertionError):
        with lets.atomic():
            g = gevent.spawn(gevent.sleep, 0.1)
            gevent.joinall([g])
    # o -- Event.set(), AsyncResult.set()
    with lets.atomic():
        Event().set()
        AsyncResult().set()
    # x -- Event.wait()
    with pytest.raises(AssertionError):
        with lets.atomic():
            Event().wait()
    # x -- Event.wait()
    with pytest.raises(AssertionError):
        with lets.atomic():
            AsyncResult().wait()
    # x -- Channel.put()
    with pytest.raises(AssertionError):
        with lets.atomic():
            ch = Channel()
            ch.put(123)
    # o -- First Semaphore.acquire()
    with lets.atomic():
        lock = Semaphore()
        lock.acquire()
    # x -- Second Semaphore.acquire()
    with pytest.raises(AssertionError):
        with lets.atomic():
            lock = Semaphore()
            lock.acquire()
            lock.acquire()
    # Back to normal.
    gevent.sleep(1)


def test_process_local():
    local = lets.ProcessLocal()
    local.hello = 'world'
    def assert_hello_world():
        assert local.hello == 'world'
    def assert_no_hello():
        assert not hasattr(local, 'hello')
    def assert_no_hello_after_access():
        local.__dict__
        assert not hasattr(local, 'hello')
    for spawn, f in [(gevent.spawn, assert_hello_world),
                     (lets.Processlet.spawn, assert_no_hello),
                     (lets.Processlet.spawn, assert_no_hello_after_access),
                     (spawn_again, assert_hello_world)]:
        spawn(f).get()


def test_earliest():
    ear = lets.Earliest()
    assert not ear.ready()
    assert ear.wait(0.1) is None
    t = time.time()
    assert ear.set(t + 10)
    assert ear.set(t + 5)
    assert not ear.set(t + 6)
    assert ear.set(t + 1)
    assert ear.wait() == t + 1
    assert ear.wait() == t + 1
    assert ear.ready()
    ear.clear()
    assert not ear.ready()
    assert ear.wait(0.1) is None
    with pytest.raises(Timeout):
        ear.get(timeout=0.1)
    t = time.time()
    ear.set(t + 1, 123)
    ear.set(t + 10, 456)
    ear.set(t + 0.1, 42)
    with pytest.raises(Timeout):
        ear.get(block=False)
    assert ear.get(timeout=1) == (t + 0.1, 42)
    with pytest.raises(TypeError):
        ear.set(None)
    assert ear.get() == (t + 0.1, 42)
    assert ear.get(block=False) == (t + 0.1, 42)
