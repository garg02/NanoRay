import os
import ray
from numpy import random
from time import sleep

ray.init()
from ray.util.queue import Queue, Empty

@ray.remote(max_restarts=0)
class Actor:
    def __init__(self, x):
        self.id = x
    def test(self, queue):
        while not queue.empty():
            try: 
                number = queue.get(block=False)
            except Empty:
                return self.id
            sleeptime = random.uniform(2, 4)
            sleep(sleeptime) 
            print(f'finished processing {number}!!')
        try: 
            number = queue.get(block=False)
        except Empty:
            return self.id

@ray.remote(max_retries=3)
def fail():
    print("hello")
    os._exit(2)

x = fail.remote()

ray.get(x)
print(2)
# numbers = list(range(5))
# myqueue = Queue(maxsize=5)
# myqueue.put_nowait_batch(numbers)
# actors = [Actor.remote(x) for x in range(1)]

# counter = ray.get([actor.test.remote(myqueue) for actor in actors])
# print(counter)