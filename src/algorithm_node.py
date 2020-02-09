import abc
import asyncio
import random
import logging
from aio_pika import IncomingMessage, Message
from base_node import BaseNode
from messages import BroadcastMessage
from algorithm_config import AlgorithmConfig
from utils.color import style

LOGGER = logging.getLogger(__name__)


class AlgorithmNode(BaseNode):
    _config: AlgorithmConfig
    _is_running: bool = False

    # Intercept message receipt, init base class
    def __init__(self, config: AlgorithmConfig, on_message_receive_debug=None):
        self._config = config
        self.report_message_callback = on_message_receive_debug
        super().__init__(self._config, self.receive_message)

    # Implement abstract function of base class
    async def setup_connection(self, loop):
        await self.init_connection(loop=loop)
        self.log('Setup exchange as fanout')
        await self.init_fanout_messaging(self._config.identifier, exchange_name=self._config.exchange_name)

    # Implement abstract function of base class:
    #   Run core of algorithm with a proper connection to RabbitMQ setup for you
    async def run_core(self, loop):
        while True:
            if not self._is_running:
                self._is_running = True
                self.log('Running core loop for {}'.format(self._identifier))

                # Send x test messages with random delay
                for i in range(2):
                    broadcastMessage = BroadcastMessage(
                        node_name=self._identifier)
                    msg = Message(
                        bytes(broadcastMessage.serialize(), encoding='utf8'))
                    await self.publish_message(msg, routing_key="")

                delay = random.random() / 10.0
                await asyncio.sleep(delay)

            self.sleep_cancellation_event = asyncio.Event()
            await asyncio.wait([self.sleep_cancellation_event.wait()],
                               return_when=asyncio.FIRST_COMPLETED)
            break

        self.log("Waiting 0.5 seconds to cleanup")
        await asyncio.sleep(0.5)
        self._is_running = False

    # Asynchronous message receipt, synchronous handler
    def receive_message(self, node_identifier, message: IncomingMessage):
        msg: BroadcastMessage = BroadcastMessage(message.body)
        if msg.has_error:
            self.log("Invalid message received, skipping this.")
            return

        # if msg.initiation_message:
        #     print("Initiation received")

        if not msg.node_name == self._identifier:
            print_message = 'Got message {} from node {}'.format(
                msg.uuid, msg.node_name)
            if callable(self.report_message_callback):
                # Forwarding message to __main__
                self.report_message_callback(node_identifier, message)
            else:
                # Keep message here, dont bubble up
                self.log(print_message)
        # else:
            # skipping own message

    def log(self, message):
        if self._config.debug_messages:
            if callable(self._config.color):
                LOGGER.info("[{}] {}".format(self._identifier,
                                             self._config.color(message)))
        else:
            LOGGER.info("[{}] {}".format(self._identifier, message))
