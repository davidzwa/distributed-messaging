#!/usr/bin/env python
import logging
import pika
import time
import multiprocessing
import random
from utils.timing import *
from utils.color import style
from aio_pika import IncomingMessage
from algorithm_node import AlgorithmNode
from algorithm_config import AlgorithmConfig, BaseConfig
from utils.threadpool import ThreadPool, Worker

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -15s %(funcName) '
              '-10s %(lineno) -5d: %(message)s')
RABBITMQ_CONNECTION_STRING = "amqp://guest:guest@localhost:5672/%2F"

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
# disable propagation of aio_pika messages as it will clog your terminal, only set True if you have little nodes
logging.getLogger("aio_pika").propagate = False


def log_main(log_message):
    LOGGER.info(style.BLUE(log_message))


def on_message_callback_debug(node_identifier, message: IncomingMessage):
    # See messages arriving in this simulation (optional)
    log_main('Received: ' + str(message.body) +
             ' at node ' + node_identifier)


def start_async_node(parameters: AlgorithmConfig):
    consumer = AlgorithmNode(
        parameters)
    # Start it and if correct 'consumer.run_core()' should be called, once RabbitMQ communication is setup.
    consumer.start()


def kickoff_simulation(default_config: BaseConfig):
    # semd kickoff message here, starting the node(s) with algorithm_initiator set to True.
    pass


if __name__ == '__main__':
    default_worker_colors = [style.GREEN, style.YELLOW]
    start_time = getTime()

    # Setup algorithm nodes and initiator
    num_nodes = 2
    algorithm_initiator_index = random.randint(0, num_nodes - 1)
    log_main("Booting algorithm simulator for {} nodes with {} as initiator".format(
        num_nodes, algorithm_initiator_index))

    # Run custom ThreadPool object with 'start_async_node' as target
    pool = ThreadPool(num_nodes)
    for index in range(len(pool.workers)):
        # Start one threaded & async node (connection => thread, async handling => coroutine)
        worker_color = style.GREEN
        try:
            worker_color = default_worker_colors[index]
        except:
            pass

        parameters = AlgorithmConfig(
            index,
            index == algorithm_initiator_index,
            amqp_url=RABBITMQ_CONNECTION_STRING,
            autodelete_queue=False,
            debug_messages=True)
        if algorithm_initiator_index == index:
            log_main("Booting initiator node index {} now.".format(
                algorithm_initiator_index))
        else:
            log_main("Booting normal node index {} (!= {}) now.".format(
                index, algorithm_initiator_index))
        pool.add_task(start_async_node, parameters)

        # Sleep if you require
        # time.sleep(random.random())

    # Await all workers to complete
    pool.wait_completion()
    delta_time = getElapsedTime(start_time)
    log_main("Simulation ended in {} seconds".format(
        round(delta_time, 3)))
