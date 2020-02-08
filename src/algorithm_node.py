import abc
import asyncio
from async_node import AsyncNode
from aio_pika import IncomingMessage, Message


class AlgorithmNode(AsyncNode):
    # Set by AsyncNode
    _identifier = None

    # Intercept message receipt, init base class
    def __init__(self, node_identifier, amqp_url, on_message_receive_debug=None):
        self.report_message_callback = on_message_receive_debug
        super().__init__(node_identifier, amqp_url, self.receive_message)

    # Asynchronous message receipt
    def receive_message(self, node_identifier, message: IncomingMessage):
        print('AlgorithmNode got message ', message)

        if callable(self.report_message_callback):
            self.report_message_callback(node_identifier, message)

    # Run core of algorithm with a proper connection to RabbitMq setup for you
    async def run_core(self):
        print('Running loop for {}'.format(self._identifier))

        # Do your algorithm here
        for i in range(5):
            await self.publish_message({'foo': 'bar'})
            await asyncio.sleep(1)

    async def publish_message(self, content):
        await self._exchange.publish(
            Message(
                bytes('Hello', 'utf-8'),
                content_type='text/plain',
                headers=content
            ),
            routing_key="asd"
        )
