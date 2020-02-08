# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import functools
import logging
import time
import asyncio
from aio_pika import connect_robust, Message, IncomingMessage

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOGGER = logging.getLogger(__name__)


class AlgorithmAsyncNode(object):
    """This is an example consumer that will reconnect if the nested
    Manager indicates that a reconnect is necessary.
    """
    _identifier = "GenericNode"

    def __init__(self, identifier, amqp_url="", on_message_callback_debug=None):
        self._identifier = identifier
        if callable(on_message_callback_debug):
            self._on_message_callback_debug = on_message_callback_debug

        LOGGER.info("Started node " + self._identifier)
        if amqp_url:
            self._amqp_url = amqp_url
        else:
            self._amqp_url = 'amqp://guest:guest@localhost:5672/%2F'

    def on_message(self, message_body):
        # consume message here, RabbitMQ ACK is taken care of in manager
        LOGGER.info(self._identifier + " received " + str(message_body))
        if callable(self._on_message_callback_debug):
            self._on_message_callback_debug(self._identifier, message_body)

    def run(self):
        loop = asyncio.new_event_loop()
        # loop = asyncio.get_event_loop()
        loop.run_until_complete(self.main(loop))

    async def receive_message(self, message: IncomingMessage):
        async with message.process():
            # Receiving message
            # incoming_message = await queue.get(timeout=5)
            LOGGER.info(self._identifier + " got " + str(message))
            # Confirm message
            # await incoming_message.ack()

    async def main(self, loop):
        # Do algorithm stuff here
        connection = await connect_robust(
            self._amqp_url, loop=loop
        )
        queue_name = "test_queue"
        routing_key = "test_queue"
        # Creating channel
        channel = await connection.channel()
        # Declaring exchange
        exchange = await channel.declare_exchange('direct', auto_delete=True)
        # Declaring queue
        queue = await channel.declare_queue(queue_name, auto_delete=True)
        # Binding queue
        await queue.bind(exchange, routing_key)
        # Receiving message on declared queue
        await queue.consume(self.receive_message)

        for i in range(5):
            await exchange.publish(
                Message(
                    bytes('Hello', 'utf-8'),
                    content_type='text/plain',
                    headers={'foo': 'bar'}
                ),
                routing_key
            )
            await asyncio.sleep(1)

        while True:
            await asyncio.sleep(1)
            LOGGER.info(self._identifier + " still alive. Sleeping main loop.")

        await queue.unbind(exchange, routing_key)
        await queue.purge()
        await queue.delete()
        await exchange.delete()
        await connection.close()

        LOGGER.info(self._identifier + " done and cleaned up after it.")


def main():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    amqp_url = 'amqp://guest:guest@localhost:5672/%2F'
    consumer = AlgorithmAsyncNode(amqp_url)
    consumer.run()


if __name__ == '__main__':
    main()
