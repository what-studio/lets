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
import gipc
import pytest

from lets import Processlet, Transparentlet, TransparentGroup


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


def wait_then(event, callback):
    event.wait()
    callback()


def test_processlet_spawn_child_process():
    proc = Processlet.spawn(os.getppid)
    assert proc.exit_code is None
    assert proc.pid != os.getpid()
    assert proc.get() == os.getpid()
    assert proc.exit_code == 0


def test_processlet_parellel_execution():
    t = time.time()
    jobs = [gevent.spawn(busy_waiting) for x in range(5)]
    gevent.joinall(jobs)
    assert time.time() - t > 0.5
    assert jobs[0].get() == 0
    t = time.time()
    jobs = [Processlet.spawn(busy_waiting) for x in range(5)]
    gevent.joinall(jobs)
    assert time.time() - t < 0.2
    assert jobs[0].get() == 0


def test_processlet_exception():
    with pytest.raises(ZeroDivisionError):
        Processlet.spawn(divide_by_zero).get()


def test_processlet_exit():
    proc = Processlet.spawn(kill_itself)
    with pytest.raises(SystemExit) as e:
        proc.get()
    assert e.value.code == -signal.SIGKILL
    assert proc.exit_code == -signal.SIGKILL
    proc = Processlet.spawn(busy_waiting, 10)
    proc.send_signal(signal.SIGTERM)
    with pytest.raises(SystemExit) as e:
        proc.get()
    assert e.value.code == -signal.SIGTERM
    assert proc.exit_code == -signal.SIGTERM


def test_processlet_args():
    args = (1, 2)
    kwargs = {'x': 3, 'y': 4}
    proc = Processlet.spawn(return_args, *args, **kwargs)
    assert proc.get() == (args, kwargs)


def test_processlet_pipe_arg():
    with gipc.pipe() as (r, w):
        proc = Processlet.spawn(type(w).put, w, 1)
        assert r.get() == 1
        proc.join()


def test_processlet_without_start():
    proc = Processlet(os.getppid)
    assert proc.exit_code is None
    assert proc.pid is None
    with pytest.raises(gevent.hub.LoopExit):
        proc.join()
    with pytest.raises(gevent.hub.LoopExit):
        proc.get()


def test_processlet_start_twice():
    proc = Processlet(random.random)
    proc.start()
    r1 = proc.get()
    proc.start()
    r2 = proc.get()
    assert r1 == r2


def test_transparentlet():
    job = Transparentlet.spawn(divide_by_zero)
    with pytest.raises(ZeroDivisionError) as e:
        job.get()
    assert e.traceback[-1].name == 'divide_by_zero'


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
