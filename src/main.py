#!/usr/bin/env python
import logging
from consumer import ReconnectingConsumer
from threadpool import ThreadPool, Worker
import pika
import time
import multiprocessing
from time import sleep

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
# disable propagation of pika messages
logging.getLogger("pika").propagate = False


def debug_msg_callback(body):
    print('Received: ', body)


def consume(args):
    logging.basicConfig(level=logging.WARNING, format=LOG_FORMAT)
    amqp_url = 'amqp://guest:guest@localhost:5672/%2F'
    consumer = ReconnectingConsumer(
        amqp_url, on_message_callback=debug_msg_callback)
    consumer.run()


if __name__ == '__main__':
    LOGGER.info("Booting algorithm simulator for 2 workers")
    pool = ThreadPool(2)

    for i in range(len(pool.workers)):
        pool.add_task(consume, i)
    pool.wait_completion()
