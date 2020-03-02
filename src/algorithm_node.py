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
    # Configuration and knowledge
    _config: AlgorithmConfig
    _known_nodes: list = []             # Safe to define here, overwritten dynamically

    # State variables
    _is_running: bool = False           # Sanity check
    balance: int = 0                    # Fictional state, a balance of sorts
    local_state_record = None           # Local state, at last moment of recording
    local_state_recorded = False        # Indiciation if local_state_record was recorded
    last_recording_sequence = None      # Last recorded sequence number
    global_state_sequence: int = None   # Currently processed recording sequence

    
    def __init__(self, config: AlgorithmConfig, on_message_receive_debug=None):
        """
            Define empty channels/buffers and states
        """
        # DONT DEFINE THESE IN CLASS SPACE, they will get shared across classes that way!
        self._awaiting_channels: list = []
        self._recorded_channels: list = []
        self._channel_buffers: dict = {}
        self._is_complete: bool = False

        self._config = config
        self.report_message_callback = on_message_receive_debug
        self.balance = self._config.algorithm_init_state
        
        # Lets base inititate as well
        super().__init__(self._config, self.receive_message)

    async def setup_connection(self, loop):
        """
            Implement abstract function of base class:
            Setup connection type, choose exchange type
        """
        await self.init_connection(loop=loop)
        await self.init_fanout_messaging(self._config.identifier, exchange_name=self._config.exchange_name)

    async def run_core(self, loop):
        """
        Implement abstract function of base class:
            Run core of algorithm with a proper connection to RabbitMQ setup for you
        """
        self.cancellation_event = asyncio.Event(loop=loop)
        if not self._is_running:
            self._is_running = True
            while True:
                # Await until 1 node starts spamming the system with messages, after that the event is never set again expliticly.
                await self.cancellation_event.wait()
                await asyncio.wait([self.cancellation_event.wait(), asyncio.sleep(0.5 + random.random())],
                                   return_when=asyncio.FIRST_COMPLETED)

                if random.random() <= self._config.messaging_probability and self._known_nodes and self.balance >= 1:
                    balance = random.randint(1, self.balance)
                    target = random.choice(self._known_nodes)
                    await self.transfer_balance(balance, target)

                # In case something fails, we dont burn CPU time
                delay = random.random() / 10.0
                await asyncio.sleep(delay)

                if self.cancellation_event.is_set():
                    break
        else:
            raise Exception(
                "'run_core()' was already running. That's a problem.")
        self._is_running = False

    async def transfer_balance(self, balance: int, target: str):
        """ Do Work: 
            Sending generic balance transfer message 
        """
        if self._known_nodes and len(self._known_nodes) > 0:
            broadcastMessage = AlgorithmMessage(
                sender_node_name=self._identifier)
            broadcastMessage.set_transfer_message(transfers=[balance])
            if balance <= self.balance:
                # self.log(
                #     "Transferred {} to target {}.".format(balance, target))
                self.balance -= balance
                await super().publish_message(broadcastMessage.tobytes(), target_node_queue=target, default_exchange=True)
            else:
                self.log(
                    "Balance not high enough to transfer {}. Cancelled.".format(balance))
        else:
            self.log(
                "Node not connected. Balance transfer cancelled.".format(balance))

    # Asynchronous message receipt, synchronous handler
    async def receive_message(self, node_identifier, message: IncomingMessage):
        msg: AlgorithmMessage = AlgorithmMessage(message.body)
        if msg.has_error:
            self.log(
                "Invalid message received (deserialization failed), skipping this.")
            return

        # Skipping own message
        if not msg.sender_node_name == self._identifier:
            if msg.is_node_pre_initiation():
                self.pre_ititiation_received(msg)
            elif msg.is_node_initiation(self._config.node_index):
                await self.initiation_received(msg)
            elif msg.is_marker_message():
                if self.local_state_recorded == False:
                    self.global_state_sequence = msg.marker_sequence
                await self.receive_marker(msg)
            elif msg.is_balance_transfer():
                self.balance_transfer_received(msg)
            else:
                print_message = 'Got unknown message {} from node {}'.format(
                    msg.uuid, msg.sender_node_name)
            
            # Choose to bubble up to main or not
            # if callable(self.report_message_callback):
            #     # Forwarding message to __main__
            #     self.report_message_callback(node_identifier, message)
            # else:
            #     # Keep message here, dont bubble up
            #     self.log(print_message)

    def pre_ititiation_received(self, msg):
        #     self.log(
        #         "received initiation list of length ({})".format(len(msg.payload)))
        msg.payload.remove(self._identifier)
        self._known_nodes = msg.payload

    async def initiation_received(self, msg):
        if not self._is_running:
            self.log(
                "We were activated, but not running... Maybe we died?")
            raise Exception(
                "Unsure what to do. 'run_core(...)' is not sleeping. Erroneous state.")
        elif self.cancellation_event:
            # Awake from slumber, albeit a short one (see run_core() timeout|sleep)
            self.global_state_sequence = msg.marker_sequence
            self.log("Awaking from sleep for initiation with initiation sequence {}".format(
                msg.marker_sequence))
            await self.record_and_send_markers(msg.marker_sequence)
            self.cancellation_event.set()
        else:
            self.log("We were running, but no sleeping event was defined.")
            raise Exception("No sleeping event known. Illegal state.")

    def balance_transfer_received(self, msg: AlgorithmMessage):
        # Normal correspondence
        transferred_balance = msg.payload[0]
        self.balance += transferred_balance
        self._channel_buffers.setdefault(
            msg.sender_node_name, []).append(transferred_balance)
        # self.log("Received transfer of {}, total: {}".format(
        #     transferred_balance, self.balance))

    async def record_and_send_markers(self, global_state_sequence, incoming_channel=None):
        # Blindly accept new sequence for now, TODO decide whether that requires a reset
        self.global_state_sequence = global_state_sequence
        self.record_local_state()

        self._awaiting_channels = self._known_nodes[:]
        for channel in self._awaiting_channels:
            # self.log("Clearing channel: {}".format(channel))
            self._channel_buffers = dict((channel, [])
                                         for channel in self._awaiting_channels)
        
        # Again, we blindly accept the sequence number for now as-is
        self.last_recording_sequence = self.global_state_sequence
        copy_list = self._awaiting_channels.copy()
        # Incoming vs spontaneous start
        if incoming_channel is not None:
            self._recorded_channels.append(incoming_channel)
            self._awaiting_channels.remove(incoming_channel)
        
        # Send to all outgoing
        self.log(copy_list)
        await self.send_markers(copy_list, global_state_sequence)

    async def receive_marker(self, msg: AlgorithmMessage):
        self.log("Received marker from {} in state: {}".format(msg.sender_node_name, self.local_state_recorded))
        if self.local_state_recorded == False:
            # Record as empty, or rather: clear it
            self._channel_buffers.setdefault(msg.sender_node_name, [])
            await self.record_and_send_markers(msg.marker_sequence, incoming_channel=msg.sender_node_name)
        else:
            # Record state of channel c as contents of Bc
            if msg.sender_node_name in self._awaiting_channels:
                self.record_channel_state(msg.sender_node_name)
            else:
                raise Exception("Channel {} was not expected in awaiting_channels as it was retrieved earlier!".format(
                    msg.sender_node_name))
            # Only here should we be able to check for completeness
            self.check_if_finished()

    def record_local_state(self):
        # Record local state
        self.local_state_record = self.balance
        self.local_state_recorded = True
        self.log("Recorded LOCAL state {}".format(self.local_state_record))

    def record_channel_state(self, channel):
        # Sanity checks
        if channel == self._identifier:
            raise Exception("Problem, this is our own channel we are trying to record: {} == {}".format(
                channel, self._identifier))
        if self._identifier in self._recorded_channels:
            raise Exception("Problem: {} is already in recorded channels {}".format(
                self._identifier, self._recorded_channels))

        # Record channel state and move over to _recorded_channels for handy reference
        self._awaiting_channels.remove(channel)
        self._recorded_channels.append(channel)
        channel_buffer = self._channel_buffers.get(channel)

        self.log("Recorded CHANNEL state {}".format(channel_buffer))

    def check_if_finished(self):
        if self._is_complete == True:
            self.log("Already complete, but checking again??")
        if len(self._awaiting_channels) == 0 and self.local_state_recorded:
            self._is_complete = True
            self.log("Algorithm seemingly complete on my side ({} known, {} recorded)!".format(
                len(self._known_nodes), len(self._recorded_channels)))
            self.cancellation_event.set()  # Cancel and cleanup
        else:
            self.log("Algorithm awaiting ({} known, {} recorded, {} awaiting) ({})!".format(
                len(self._known_nodes), len(self._recorded_channels), len(self._awaiting_channels), self._awaiting_channels))

    async def send_markers(self, channel_list: list, sequence_number):
        for target_queue in channel_list:
            # We send unicast (=direct) messages to default_exchange (and not to self._exchange which is broadcast)
            broadcastMessage = AlgorithmMessage(
                sender_node_name=self._identifier)
            broadcastMessage.set_marker_message(sequence_number)
            self.log('Sent marker to {}'.format(target_queue))
            await super().publish_message(broadcastMessage.tobytes(), target_queue, default_exchange=True)

    def log(self, message, style_colorizer=style.GREEN):
        if self._config.debug_messages:
            if callable(self._config.color):
                LOGGER.info("[{}] {}".format(self._identifier,
                                             self._config.color(message)))
        else:
            LOGGER.info("[{}] {}".format(self._identifier, message))
