#!/usr/bin/env python
import pika
import time

from utils.util import Spawner
import asyncio

async def main():
    print('Hello ...')
    await asyncio.sleep(1)
    print('... World!')

if __name__ == '__main__':
    # asyncio.run(main())
    
    pikaConnection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    print('Connection open? ', pikaConnection.is_open) # ,channel.connection

    Spawner(pikaConnection).execute()

    # canceled = True
    # while canceled == False:
    #     time.sleep(1)

    print('Program exited after user cancellation.')
