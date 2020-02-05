import worker
import multiprocessing
from color import style

class Spawner:
    count_workers = 3
    def __init__(self, count: int):
        if count is not None:
            self.count_workers = count
        else:

        
    def execute(self):
        print('Worker spawner called.')
        self.spawn_workers(3)

    def spawn_workers(self, count = 5):
        print('Spawning ', count, " workers")
        jobs = []
        for i in range(count):
            p = multiprocessing.Process(target=worker.Worker)
            jobs.append(p)
            p.start()
        return jobs