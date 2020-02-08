from aio_pika import IncomingMessage, Message
from async_node import AsyncNode
from messages import BroadcastMessage
import abc
import asyncio
import random
from utils.color import style


class AlgorithmNode(AsyncNode):
    # Set by AsyncNode
    _identifier = None
    color = style.GREEN

    # Intercept message receipt, init base class
    def __init__(self, node_identifier, amqp_url, on_message_receive_debug=None):
        self.report_message_callback = on_message_receive_debug
        super().__init__(node_identifier, amqp_url, self.receive_message)

    # Asynchronous message receipt
    def receive_message(self, node_identifier, message: IncomingMessage):
        msg: BroadcastMessage = BroadcastMessage(message.body)

        if not msg.node_name == self._identifier:
            print_message = ': Got message {} from node {}'.format(
                msg.uuid, msg.node_name)
            if callable(self.color):
                print(self._identifier, self.color(print_message))
            else:
                print(self._identifier, print_message)

            if callable(self.report_message_callback):
                self.report_message_callback(node_identifier, message)
        # else
        # skipping own message

    # Implement abstract function of base class
    async def setup_connection(self, loop):
        print('Setup connection as fanout')
        queue_name = "test_queue." + self._identifier
        await self.init_connection(loop=loop)
        await self.init_fanout_messaging(queue_name, exchange_name='main_exchange')

    # Implement abstract function of base class:
    #   Run core of algorithm with a proper connection to RabbitMq setup for you
    async def run_core(self):
        print('Running loop for {}'.format(self._identifier))

        # Send 50 test messages with random delay
        for i in range(50):
            broadcastMessage = BroadcastMessage(node_name=self._identifier)
            msg = Message(bytes(broadcastMessage.serialize(), encoding='utf8'))

            await self.publish_message(msg, routing_key="")

            delay = random.random() / 10.0
            await asyncio.sleep(delay)
