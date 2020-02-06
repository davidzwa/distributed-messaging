import asyncio
from worker import Worker
from utils.color import style
import concurrent.futures import ProcessPoolExecutor


class Spawner:
    count_workers = 3
    task_loop = None
    workers = []
    tasks = []

    def __init__(self, count: int = None):
        self.task_loop = asyncio.get_event_loop()

        if count is not None:
            self.count_workers = count
        else:
            print('Default amount of workers: ' + str(self.count_workers))
        if type(self.count_workers) is not int:
            print(type(self.count_workers), self.count_workers)
            exit("Amount of workers (Count argument) is not of type 'int'")

    # Synchronize on own event loop
    def execute(self):
        if self.task_loop is not None:
            self.task_loop.run_until_complete(self.__async__execute())
        else:
            print(style)

    async def __async__execute(self):
        print('Task spawner called.')
        futures = []
        for i in range(self.count_workers):
            self.workers.append(Worker())

        return asyncio.gather(*[w.run() for w in self.workers])
