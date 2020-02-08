import json
from uuid import uuid4


class BroadcastMessage(object):
    # Serializable message
    node_name: str = ""
    uuid: str
    token: int = 0

    def __init__(self, byte_data=None, node_name=""):
        if byte_data is not None:
            self.__dict__.update(json.loads(byte_data))
        else:
            self.uuid = str(uuid4())[:4]
            self.node_name = node_name

    def serialize(self):
        return json.dumps(self.__dict__)
