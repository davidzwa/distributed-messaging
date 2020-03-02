#!/usr/bin/env python
import logging
import pika
import time
import multiprocessing
from aio_pika import IncomingMessage
from time import sleep
from async_node import AsyncNode
from node import AlgorithmNode
from threadpool import ThreadPool, Worker

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
# disable propagation of pika messages
logging.getLogger("pika").propagate = False
logging.getLogger("aio_pika").propagate = False


def on_message_callback_debug(node_identifier, message: IncomingMessage):
    # See messages arriving in this simulation
    print('Received:', message.body, 'at node', node_identifier)


# An algorithm node, usually with 2 threads each (messaging, main loop)
def start_node(index):
    amqp_url = 'amqp://guest:guest@localhost:5672/%2F'
    node_identifier = "MulticastNode" + str(index)
    consumer = AlgorithmNode(
        node_identifier,
        amqp_url=amqp_url, on_message_callback_debug=on_message_callback_debug)
    consumer.run()


def start_async_node(index):
    amqp_url = 'amqp://guest:guest@localhost:5672/%2F'
    node_identifier = "MulticastNode" + str(index)
    consumer = AsyncNode(
        node_identifier,
        amqp_url=amqp_url, on_message_receive_debug=on_message_callback_debug)
    consumer.__run()


if __name__ == '__main__':

    # Kickstart the node workers
    # feel free to introduce random delays like f.e. time.sleep(100) between each start

    LOGGER.info("Booting algorithm simulator for 2 workers")
    pool = ThreadPool(1)
    for i in range(len(pool.workers)):
        pool.add_task(start_async_node, i)
    pool.wait_completion()
