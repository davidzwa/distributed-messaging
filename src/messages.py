import json
from uuid import uuid4
from enum import IntEnum


class MessageType(IntEnum):
    INITIATION = 0
    MARKER = 1
    GENERIC = 2


class BroadcastMessage(object):
    # Serializable message
    node_name: str = ""
    uuid: str = ""

    _type: int = -1
    initiator_index: int = -1

    has_error = False
    parsing_error = None

    def __init__(self, byte_data=None, node_name=""):
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
            self.node_name = node_name

    def is_node_initiation(self, node_index):
        if self._type == MessageType.INITIATION and self.initiator_index == node_index:
            return True
        else:
            return False

    def set_initiation_message(self, initiator_index):
        self._type = MessageType.INITIATION
        self.initiator_index = initiator_index

    def set_marker_message(self):
        self._type = MessageType.MARKER

    def set_generic_message(self):
        self._type = MessageType.GENERIC

    def serialize(self):
        return json.dumps(self.__dict__)
