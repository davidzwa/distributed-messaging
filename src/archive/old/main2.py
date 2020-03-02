#!/usr/bin/env python
import pika
import time
import asyncio
from utils.color import style
from spawner import Spawner

if __name__ == '__main__':
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost'))
    if connection.is_closed:
        print("The RabbitMQ server was not found (check connection localhost:5672). Quitting simulation.")
    channel = connection.channel()
    if channel.is_closed:
        print("The RabbitMQ server was not found. Quitting simulation.")
    connection.close()

    task_spawner = Spawner()
    task_spawner.execute_concurrent()
    # asyncio.run(task_spawner.execute_concurrent())

    # canceled = False
    # while canceled == False:
    #     time.sleep(1)

    print(style.GREEN('Program exited normally.'))
