import os
import multiprocessing
import gevent
import gipc
import time
from lets.processlet import Processlet2
from lets.quietlet import Quietlet


def f():
    while True:
        gevent.sleep(0.5)
        print os.getpid()
def g():
    gevent.sleep(3)
def spin(sec, sleep=False):
    t = time.time()
    while time.time() - t < sec:
        if sleep:
            gevent.sleep(0.01)
gevent.spawn(f)
gevent.sleep(1)

# gevent.sleep(1)
# p = gipc.start_process(g)
# p.join()
# gevent.sleep(3)
# p = multiprocessing.Process(target=g)
# p.start()
# print 'started'
# p.join()
# print 'joined'

# if gevent.fork() == 0:
#     print 'child', os.getpid()
#     gevent.get_hub().destroy(destroy_loop=True)
# else:
#     print 'parent', os.getpid()

class QuietProcesslet(Processlet2, Quietlet):

    pass

p = QuietProcesslet.spawn(spin, 3, sleep=True)
print '[1] spawned'
p.join(timeout=1)
p.kill(ValueError, block=False)
print '[1] killed'
p.join()
print '[1] joined', p.successful()

p = QuietProcesslet.spawn(spin, 3, sleep=False)
print '[2] spawned'
p.join(timeout=1)
p.kill(ValueError, block=False)
print '[2] killed'
p.join()
print '[2] joined', p.successful()

gevent.sleep(3)
