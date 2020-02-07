#!/usr/bin/env python
from time import sleep
import multiprocessing
import time
import pika
from worker import Worker, ThreadPool
import logging
from async_recv import ReconnectingExampleConsumer, LOG_FORMAT


def callback2(body):
    print('Received: ', body)


def consume(args):
    logging.basicConfig(level=logging.WARNING, format=LOG_FORMAT)
    amqp_url = 'amqp://guest:guest@localhost:5672/%2F'
    consumer = ReconnectingExampleConsumer(
        amqp_url, on_message_callback=callback2)
    consumer.run()


if __name__ == '__main__':
    pool = ThreadPool(2)

    for i in range(len(pool.workers)):
        pool.add_task(consume, i)
    pool.wait_completion()
