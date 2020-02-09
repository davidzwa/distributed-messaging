import json
from uuid import uuid4


class BroadcastMessage(object):
    # Serializable message
    node_name: str = ""
    uuid: str
    initiation_message: bool

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

    def serialize(self):
        return json.dumps(self.__dict__)
