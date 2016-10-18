import os
import multiprocessing
import gevent
import gipc
import time
from lets.processlet import Processlet2


def f():
    while True:
        gevent.sleep(0.5)
        print os.getpid()
def g():
    gevent.sleep(3)
def spin(sec):
    t = time.time()
    while time.time() - t < sec:
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

p = Processlet2.spawn(spin, 3)
print 'spawned'
p.join(timeout=1)
print 'kill'
p.kill(ValueError)
print 'killed'
print `p.get()`
print 'joined'

gevent.sleep(3)
