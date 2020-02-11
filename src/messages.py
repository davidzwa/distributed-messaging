import json
from uuid import uuid4
from enum import IntEnum
from aio_pika import Message


class MessageType(IntEnum):
    PRE_INITIATION = 0  # Receive list of node-queues to work with
    INITIATION = 1      # Select initiator
    MARKER = 2          # Marker messages
    GENERIC_TRANSFER = 3         # Generic balance transfer traffic


class AlgorithmMessage(object):
    # Serializable message
    sender_node_name: str = ""
    uuid: str = ""

    _type: int = -1
    payload: list = None    # Pre-initiation list of known nodes
    initiator_index: int = -1
    marker_sequence: int = 0

    has_error = False
    parsing_error = None

    def __init__(self, byte_data=None, sender_node_name=""):
        # Deserialize or serialize?
        if byte_data is not None:
            try:
                try_dict = json.loads(byte_data)
                if type(try_dict) is dict:
                    self.__dict__.update(try_dict)
            except json.JSONDecodeError as e:
                print(e, "The message received was not of correct type")
                self.has_error = True
                self.parsing_error = "The message received was not of correct type"
                return
        else:
            self.uuid = str(uuid4())[:4]
            self.sender_node_name = sender_node_name

    def is_node_pre_initiation(self):
        if self._type == MessageType.PRE_INITIATION:
            if self.payload is not None and len(self.payload) > 0:
                return True
            else:
                self.has_error = True
                self.parsing_error = "The pre-initiation message had an empty list as payload."
                raise Exception(self.parsing_error)
        else:
            return False

    def set_pre_initiation_message(self, payload: list):
        self._type = MessageType.PRE_INITIATION
        self.payload = payload

    def is_node_initiation(self, node_index):
        if self._type == MessageType.INITIATION and self.initiator_index == node_index:
            return True
        else:
            return False

    def set_initiation_message(self, initiator_index):
        self._type = MessageType.INITIATION
        self.payload = None
        self.initiator_index = initiator_index

    def is_marker_message(self):
        return self._type == MessageType.MARKER

    def is_expected_marker(self, expected_marker_sequence):
        if self._type == MessageType.MARKER:
            if expected_marker_sequence and self.marker_sequence == expected_marker_sequence:
                return True
            else:
                raise Exception(
                    "The marker was not of the right sequence {} vs given {}. No way to deal with that yet.".format(expected_marker_sequence, self.marker_sequence))
        else:
            return False

    def set_marker_message(self, marker_sequence):
        self.marker_sequence = marker_sequence
        self.payload = None
        self._type = MessageType.MARKER

    def is_balance_transfer(self):
        if self._type == MessageType.GENERIC_TRANSFER:
            # We expect the list to contain only 1 element.
            if self.payload and len(self.payload) == 1:
                return True
            else:
                raise Exception(
                    "The list didnt contain only 1 element or itwas empty.")
        else:
            return False

    def set_transfer_message(self, transfers: list):
        self.payload = transfers
        self._type = MessageType.GENERIC_TRANSFER

    def serialize(self):
        return json.dumps(self.__dict__)

    def tobytes(self):
        return Message(
            bytes(self.serialize(), encoding='utf8'))
