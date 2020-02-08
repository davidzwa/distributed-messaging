#!/usr/bin/env python

"""
Create multiple RabbitMQ connections from a single thread, using Pika and multiprocessing.Pool.
Based on tutorial 2 (http://www.rabbitmq.com/tutorials/tutorial-two-python.html).
"""

from time import sleep
import multiprocessing
import time
import pika
from random import randrange
from worker import *
import logging
from async_recv import ReconnectingConsumer, LOG_FORMAT


def callback(ch, method, properties, body):
    print(" [x] %r received %r" % (multiprocessing.current_process(), body,))
    # time.sleep(body.count('.'))
    print(" [x] Message processed")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def callback2(body):
    print('Received: ', body)


def consume(args):
    logging.basicConfig(level=logging.WARNING, format=LOG_FORMAT)
    amqp_url = 'amqp://guest:guest@localhost:5672/%2F'
    consumer = ReconnectingConsumer(
        amqp_url, on_message_callback=callback2)
    consumer.run()

    # connection = pika.BlockingConnection(pika.ConnectionParameters(
    #     'localhost'))
    # channel = connection.channel()
    # queue = 'task_queue' + str(args)
    # channel.queue_declare(queue=queue, durable=True, auto_delete=True)

    # print(' [*] Creating queue ' + 'task_queue' + str(args))
    # channel.basic_consume(
    #     on_message_callback=callback,
    #     queue=queue)

    # print(' [*] Waiting for messages. To exit press CTRL+C')
    # try:
    #     print(args)
    #     channel.start_consuming()
    # except KeyboardInterrupt:
    #     pass


if __name__ == '__main__':

    delays = [randrange(1, 10) for i in range(30)]

    pool = ThreadPool(2)
    for i, d in enumerate(delays):
        pool.add_task(consume, d)
    pool.wait_completion()

    # workers = 5
    # pool = multiprocessing.Pool(processes=workers)
    # for i in range(0, workers):
    #     print('Starting consumer')
    #     pool.apply_async(consume, [i])

    # # Stay alive
    # try:
    #     while True:
    #         time.sleep(0.5)
    #         continue
    # except KeyboardInterrupt:
    #     print(' [*] Exiting...')
    #     pool.terminate()
    #     pool.join()
