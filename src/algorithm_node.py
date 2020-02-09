from aio_pika import IncomingMessage, Message
from base_node import BaseNode
from messages import BroadcastMessage
import abc
import asyncio
import random
import logging
from algorithm_config import AlgorithmConfig
from utils.color import style

LOGGER = logging.getLogger(__name__)


class AlgorithmNode(BaseNode):
    config: AlgorithmConfig

    # Intercept message receipt, init base class
    def __init__(self, config: AlgorithmConfig, on_message_receive_debug=None):
        self.config = config
        self.report_message_callback = on_message_receive_debug
        super().__init__(config, self.receive_message)

    # Implement abstract function of base class
    async def setup_connection(self, loop):
        await self.init_connection(loop=loop)
        self.log('Setup exchange as fanout')
        await self.init_fanout_messaging(self.config.identifier, exchange_name=self.config.exchange_name)

    # Implement abstract function of base class:
    #   Run core of algorithm with a proper connection to RabbitMq setup for you
    async def run_core(self):
        self.log('Running core loop for {}'.format(self._identifier))

        # Send 50 test messages with random delay
        for i in range(2):
            broadcastMessage = BroadcastMessage(node_name=self._identifier)
            msg = Message(bytes(broadcastMessage.serialize(), encoding='utf8'))

            await self.publish_message(msg, routing_key="")

            delay = random.random() / 10.0
            await asyncio.sleep(delay)

    # Asynchronous message receipt, synchronous handler
    def receive_message(self, node_identifier, message: IncomingMessage):
        msg: BroadcastMessage = BroadcastMessage(message.body)

        if not msg.node_name == self._identifier:
            print_message = ' : Got message {} from node {}'.format(
                msg.uuid, msg.node_name)
            if callable(self.config.color):
                self.log(self._identifier + self.config.color(print_message))
            else:
                self.log(self._identifier + print_message)

            if callable(self.report_message_callback):
                self.report_message_callback(node_identifier, message)
        # else:
            # skipping own message

    def log(self, message):
        if self.config.debug_messages:
            LOGGER.info(style.YELLOW(message))
