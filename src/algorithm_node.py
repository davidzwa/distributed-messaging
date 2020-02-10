import abc
import asyncio
import random
import logging
from aio_pika import IncomingMessage, Message
from base_node import BaseNode
from messages import AlgorithmMessage, MessageType
from algorithm_config import AlgorithmConfig
from utils.color import style

LOGGER = logging.getLogger(__name__)


class AlgorithmNode(BaseNode):
    _config: AlgorithmConfig
    _is_running: bool = False

    known_nodes: list = []

    balance: int = 0                    # Fictional state, a balance of sorts
    local_state_recorded = None         # Local state, at last moment of recording
    last_recording_sequence = None      # Last recorded sequence number
    global_state_sequence: int = None   # Currently processed recording sequence

    _recorded_channels: dict = {}

    # Intercept message receipt, init base class
    def __init__(self, config: AlgorithmConfig, on_message_receive_debug=None):
        self._config = config
        self.report_message_callback = on_message_receive_debug
        self.balance = self._config.algorithm_init_state
        super().__init__(self._config, self.receive_message)

    # Implement abstract function of base class
    async def setup_connection(self, loop):
        await self.init_connection(loop=loop)
        self.log('Setup exchange as fanout')
        await self.init_fanout_messaging(self._config.identifier, exchange_name=self._config.exchange_name)

    # Implement abstract function of base class:
    #   Run core of algorithm with a proper connection to RabbitMQ setup for you
    async def run_core(self, loop):
        self.sleep_cancellation_event = asyncio.Event(loop=loop)
        if not self._is_running:
            self._is_running = True
            while True:
                # self.log("Going to sleep")
                await self.sleep_cancellation_event.wait()
                # self.log("Awaking from sleep")
                # self.sleep_cancellation_event.clear()
                await asyncio.wait([self.sleep_cancellation_event.wait(), asyncio.sleep(0.5 + random.random())],
                                   return_when=asyncio.FIRST_COMPLETED)

                if random.random() <= self._config.messaging_probability and self.known_nodes and self.balance >= 1:
                    balance = random.randint(1, self.balance)
                    target = random.choice(self.known_nodes)
                    await self.transfer_balance(balance, target)

                    # if self._config.is_algorithm_initiator

                # In case something fails, we dont burn CPU time
                delay = random.random() / 10.0
                await asyncio.sleep(delay)

                # Await infinitely long, until we get interrupted in our slumber
                # self.sleep_cancellation_event.clear()
                

                # Send x test messages with random delay
                # for i in range(2):
                #     broadcastMessage = BroadcastMessage(
                #         self.global_state_sequence,
                #         sender_node_name=self._identifier)
                #     msg = Message(
                #         bytes(broadcastMessage.serialize(), encoding='utf8'))
                #     await self.publish_message(msg, routing_key="")

        self.log("Waiting 0.5 seconds for cleaning up/closing connection.")
        await asyncio.sleep(0.5)
        self._is_running = False

    # Generic balance transfer message
    async def transfer_balance(self, balance: int, target: str):
        if self.known_nodes and len(self.known_nodes) > 0:
            broadcastMessage = AlgorithmMessage(
                sender_node_name=self._identifier)
            broadcastMessage.set_transfer_message(transfers=[balance])
            msg = Message(
                bytes(broadcastMessage.serialize(), encoding='utf8'))
            if balance <= self.balance:
                self.log(
                    "Transferred {} to target {}.".format(balance, target))
                self.balance -= balance
                await super().publish_message(msg, target_node_queue=target, default_exchange=True)
            else:
                self.log(
                    "Balance not high enough to transfer {}. Cancelled.".format(balance))
        else:
            self.log(
                "Node not connected. Balance transfer cancelled.".format(balance))

    # Asynchronous message receipt, synchronous handler
    def receive_message(self, node_identifier, message: IncomingMessage):
        msg: AlgorithmMessage = AlgorithmMessage(message.body)
        if msg.has_error:
            self.log(
                "Invalid message received (deserialization failed), skipping this.")
            return

        if msg.is_node_pre_initiation():
                if len(msg.payload) <= 5:
                    self.log(
                        "received initiation list: ({}) {}".format(len(msg.payload), msg.payload))
                else:
                    self.log(
                        "received initiation list of length ({})".format(len(msg.payload)))
                msg.payload.remove(self._identifier)
                self.known_nodes = msg.payload
        # Skipping own message
        if not msg.sender_node_name == self._identifier:
            if msg.is_node_initiation(self._config.node_index):
                if not self._is_running:
                    self.log(
                        "We were activated, but not running... Maybe we died?")
                    raise Exception(
                        "Unsure what to do. 'run_core(...)' is not sleeping. Erroneous state.")
                elif self.sleep_cancellation_event:
                    # Awake from slumber
                    self.global_state_sequence = msg.marker_sequence
                    self.log("Awaking from sleep for initiation with initiation sequence {}".format(
                        msg.marker_sequence))
                    self.sleep_cancellation_event.set()
                else:
                    print("We were running, but no sleeping event was defined.")
                    raise Exception("No sleeping event known. Illegal state.")
            else:
                if msg.is_balance_transfer():
                    self.balance += msg.payload[0]
                    self.log("Received transfer of {}, total: {}".format(msg.payload[0], self.balance))
                    self.sleep_cancellation_event.set()
                print_message = 'Got message {} from node {}'.format(
                    msg.uuid, msg.sender_node_name)
                if callable(self.report_message_callback):
                    # Forwarding message to __main__
                    self.report_message_callback(node_identifier, message)
                else:
                    # Keep message here, dont bubble up
                    self.log(print_message)

    # def record_local_state(self):
    #     # Record local state
    #     self.local_state_recorded = True
    #     # Broadcast marker
    #     super().publish_message

    def log(self, message):
        if self._config.debug_messages:
            if callable(self._config.color):
                LOGGER.info("[{}] {}".format(self._identifier,
                                             self._config.color(message)))
        else:
            LOGGER.info("[{}] {}".format(self._identifier, message))
