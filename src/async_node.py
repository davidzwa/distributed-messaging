# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import functools
import logging
import time
import asyncio
import abc
from aio_pika import connect_robust, Message, IncomingMessage, ExchangeType

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
# logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
LOGGER = logging.getLogger(__name__)


class AsyncNode(object):
    """ This is an example consumer that will connect with asynchronous connection
        Find documentation on aio_pika here https://aio-pika.readthedocs.io/en/latest/quick-start.html
        Find documentation on asyncio here https://docs.python.org/3/library/asyncio.html
    """
    _amqp_url = 'amqp://guest:guest@localhost:5672/%2F'
    _identifier = "GenericNode"
    _connection = None
    _channel = None
    _queue = None
    _exchange = None

    def __init__(self, identifier, amqp_url="", on_message_receive_debug=None):
        self._identifier = identifier
        if callable(on_message_receive_debug):
            self._on_message_callback_debug = on_message_receive_debug

        LOGGER.info("Started async_node " + self._identifier)
        if amqp_url:
            self._amqp_url = amqp_url

    @abc.abstractmethod
    async def run_core(cls):
        return cls()

    def start(self):
        # Create asyncio event-loop for co-routines
        loop = asyncio.new_event_loop()
        loop.run_until_complete(self.run_async(loop))

    async def run_async(self, loop):
        # Connect to setup [exchange => (topic) => queue]
        queue_name = "test_queue." + self._identifier
        await self.init_connection(loop=loop)
        await self.init_topic_messaging(queue_name, exchange_name='davids_exchange')

        # Call main loop, implemented by user
        if not self.run_core or not callable(self.run_core):
            LOGGER.error(
                "Provide a run_core() implementation in your extending class, because run_core() is not callable right now!")
        else:
            # Blocking wait
            await self.run_core()
        # await asyncio.sleep(1)
        LOGGER.debug(self._identifier + " finished. Cleaning up.")

        # Cleanup work
        await self.close_connection()
        LOGGER.info(self._identifier + " done and cleaned up after it.")

    async def __receive_message(self, message: IncomingMessage):
        async with message.process():
            LOGGER.info(
                self._identifier + " received message on exchange '{}'".format(message.exchange))
            if callable(self._on_message_callback_debug):
                self._on_message_callback_debug(self._identifier, message)

    async def init_connection(self, loop):
        self._connection = await connect_robust(
            self._amqp_url, loop=loop
        )
        if self._connection and not self._connection.is_closed:
            self._channel = await self._connection.channel()
        else:
            LOGGER.error(
                "Connection is closed, or was never initiated. Check your RabbitMQ connection.")

    async def init_fanout_messaging(self, queue_name, exchange_name='main_fanout_messaging'):
        # Declaring exchange & bind queue to it
        await self.bind_queue_exchange(
            queue_name, exchange_name, exchange_type=ExchangeType.FANOUT)

    async def init_topic_messaging(self, queue_name, exchange_name='main_topic_messaging'):
        """
        Only reach a subset of nodes by topic
        """
        # Declaring exchange & bind queue to it
        await self.bind_queue_exchange(
            queue_name, exchange_name, exchange_type=ExchangeType.TOPIC)

    async def init_direct_messaging(self, queue_name, exchange_name='main_direct_messaging'):
        # Declaring exchange & bind queue to it
        await self.bind_queue_exchange(
            queue_name, exchange_name, exchange_type=ExchangeType.DIRECT)

    async def bind_queue_exchange(self, queue_name, exchange_name, exchange_type=ExchangeType.DIRECT):
        if not self._channel or self._channel.is_closed:
            LOGGER.error(
                "Channel is closed, or was never initiated. Did you call 'init_connection()'? Also, check your RabbitMQ connection or check for previous channel errors.")
        exchange = await self._channel.declare_exchange(exchange_name, type=exchange_type, auto_delete=True)
        queue = await self._channel.declare_queue(queue_name, auto_delete=True)
        await queue.bind(exchange)
        await queue.consume(self.__receive_message)

        self._exchange = exchange
        self._queue = queue

    async def close_connection(self, delete_queue=True):
        if self._queue:
            await self._queue.unbind(self._exchange)
            if delete_queue:
                # await self._queue.purge()
                await self._queue.delete(if_unused=False, if_empty=False)
        if self._exchange:
            await self._exchange.delete()
        await self._connection.close()


def main():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    amqp_url = 'amqp://guest:guest@localhost:5672/%2F'
    consumer = AsyncNode(amqp_url)
    consumer.__run()


if __name__ == '__main__':
    main()
