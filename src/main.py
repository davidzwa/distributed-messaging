#!/usr/bin/env python
import logging
import pika
import time
import multiprocessing
import random
from aio_pika import IncomingMessage
from algorithm_node import AlgorithmNode
from utils.threadpool import ThreadPool, Worker

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
# disable propagation of pika messages
logging.getLogger("pika").propagate = False
logging.getLogger("aio_pika").propagate = False


def on_message_callback_debug(node_identifier, message: IncomingMessage):
    # See messages arriving in this simulation (optional)
    LOGGER.info('Received: ' + str(message.body) +
                ' at node ' + node_identifier)


def start_async_node(index):
    # Default AMQP url to RabbitMQ broker
    amqp_url = 'amqp://guest:guest@localhost:5672/%2F'

    # Create algorithm node
    node_identifier = "MulticastNode" + str(index)
    consumer = AlgorithmNode(
        node_identifier,
        amqp_url=amqp_url,
        on_message_receive_debug=on_message_callback_debug)

    # Start it and if correct 'run_core()' should be called in it.
    consumer.start()


if __name__ == '__main__':
    # Run custom ThreadPool object with 'start_async_node' as target

    num_workers = 2
    LOGGER.info("Booting algorithm simulator for {} workers".format(num_workers))
    pool = ThreadPool(num_workers)
    for i in range(len(pool.workers)):
        # Start one threaded & async node (connection => thread, async handling => coroutine)
        pool.add_task(start_async_node, i)

        # Sleep if you require
        # time.sleep(random.random())
    pool.wait_completion()
