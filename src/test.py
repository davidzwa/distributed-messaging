import asyncio
from concurrent.futures import ProcessPoolExecutor

print('running async test')


def say_boo():
    i = 0
    while True:
        print('...boo {0}'.format(i))
        i += 1


def say_baa():
    i = 0
    while True:
        print('...baa {0}'.format(i))
        i += 1


if __name__ == "__main__":
    executor = ProcessPoolExecutor(2)
    loop = asyncio.get_event_loop()
    boo = asyncio.ensure_future(loop.run_in_executor(executor, say_boo))
    baa = asyncio.ensure_future(loop.run_in_executor(executor, say_baa))

    loop.run_forever()
