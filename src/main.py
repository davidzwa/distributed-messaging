#!/usr/bin/env python
import logging
import time
import multiprocessing
import random
import asyncio
import aio_pika
from utils.timing import *
from utils.color import style
from messages import BroadcastMessage
from aio_pika import IncomingMessage, Message
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


def spy_message(node_identifier, message: IncomingMessage):
    # See messages arriving in this simulation (optional)
    log_main('Message-spy detected: ' + str(message.body) +
             ' at node ' + node_identifier)


def start_async_node(config: AlgorithmConfig):
    consumer = AlgorithmNode(
        config,
        on_message_receive_debug=spy_message)
    # Start it and if correct 'consumer.run_core()' should be called, once RabbitMQ communication is setup.
    consumer.start()


async def kickoff_simulation(default_config: BaseConfig, initiator_index: bool):
    # send kickoff message here, starting the node(s) with algorithm_initiator set to True.
    loop = asyncio.new_event_loop()
    connection = await aio_pika.connect_robust(
        RABBITMQ_CONNECTION_STRING, loop=loop
    )

    channel = await connection.channel()
    # Declare exchange passively (if created, assume properties set elswhere, like within AlgorithmNode)
    exchange = await channel.declare_exchange(
        default_config.exchange_name, passive=True)

    broadcastMessage = BroadcastMessage(
        node_name="AlgorithmManager",
    )
    broadcastMessage.set_initiation_message(initiator_index)
    msg = Message(
        bytes(broadcastMessage.serialize(), encoding='utf8'))
    await exchange.publish(msg, routing_key="")


if __name__ == '__main__':
    default_worker_colors = [style.GREEN, style.YELLOW]
    start_time = getTime()

    # Setup algorithm nodes and initiator
    num_nodes = 2
    algorithm_initiator_index = random.randint(0, num_nodes - 1)
    log_main("Booting algorithm simulator for {} nodes with {} as initiator".format(
        num_nodes, algorithm_initiator_index))

    # TODO create exchange here instead of at every node so we know what is gonna happen

    # Run custom ThreadPool object with 'start_async_node' as target
    pool = ThreadPool(num_nodes)
    for index in range(len(pool.workers)):
        # Start one threaded & async node (connection => thread, async handling => coroutine)
        worker_color = style.GREEN
        try:
            worker_color = default_worker_colors[index]
        except:
            pass

        initiating_node = index == algorithm_initiator_index
        parameters = AlgorithmConfig(
            index,
            initiating_node,
            amqp_url=RABBITMQ_CONNECTION_STRING,
            autodelete_queue=True,
            debug_messages=True)
        if algorithm_initiator_index == index:
            log_main("Booting initiator node index {} now.".format(
                algorithm_initiator_index))
        else:
            log_main("Booting normal node index {} (!= {}) now.".format(
                index, algorithm_initiator_index))
        pool.add_task(start_async_node, parameters)

    # Wait for at least one node to setup the exchange
    time.sleep(1)
    asyncio.run(kickoff_simulation(BaseConfig(), algorithm_initiator_index))
    # Await all workers to complete
    pool.wait_completion()
    delta_time = getElapsedTime(start_time)
    log_main("Simulation ended in {} seconds".format(
        round(delta_time, 3)))
