import asyncio
from worker import Worker
from utils.color import style
from concurrent.futures import ProcessPoolExecutor


class Spawner:
    count_workers = 3

    def __init__(self, count: int = None):
        if count is not None:
            self.count_workers = count
        else:
            print('Default amount of workers: ' + str(self.count_workers))
        if type(self.count_workers) is not int:
            print(type(self.count_workers), self.count_workers)
            exit("Amount of workers (Count argument) is not of type 'int'")

    # Synchronize on own event loop
    def execute_sync(self):
        self.task_loop = asyncio.get_event_loop()
        self.task_loop.run_until_complete(self.__sync__execute())

    def execute_concurrent(self):
        self.__concurrent_execute()

    async def __sync__execute(self):
        print('Task spawner called (sync).')
        workers = []
        for i in range(self.count_workers):
            workers.append(Worker())

        return asyncio.gather(*[w.run() for w in self.workers])

    def __concurrent_execute(self):
        print('Task spawner called (concurrent).')
        loop = asyncio.get_event_loop()

        workers = []
        for i in range(self.count_workers):
            workers.append(Worker())

        for w in workers:
            result = loop.create_task(w.run())
            print(result)

        loop.run_forever()
