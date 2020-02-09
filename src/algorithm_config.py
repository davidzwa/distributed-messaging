from utils.color import style


class BaseConfig(object):
    # Preset exchange, as we only need one exchange
    exchange_name: str = "main_nodes_exchange"
    debug_messages: bool = True
    identifier: str = "AlgorithmManager"

    def __init__(self, delete_exchange, amqp_url="amqp://guest:guest@localhost:5672/%2F"):
        # Connection string
        self.amqp_url = amqp_url

        # Note: autodelete fires when connection(/all channels) close
        self.autodelete_exchange = True
        # Cleanup properties, note auto-delete in RabbitMQ above setup overrides this
        self.delete_exchange = delete_exchange


class AlgorithmConfig(BaseConfig):
    """ Easy parameter object for each ThreadPool worker, interface to AlgorithmNode. """
    # Used as base of identifier name
    _identifier_base: str = "AlgorithmNode"

    def __init__(self,
                 index, is_algorithm_initiator,
                 amqp_url=None,
                 color=style.GREEN, debug_messages=True, delete_exchange=False, delete_queue=False):

        # Algorithm initiator, will listen for the 'kick-off' message
        self.is_algorithm_initiator = is_algorithm_initiator
        # Save index, will be handy as tie-breaker
        self.index = index
        # Used as indicator and as queue name
        self.identifier = self._identifier_base + str(self.index)

        # Debugging style, TODO change to RGB
        if callable(color):
            self.color = color
        else:
            self.color = style.GREEN
        self.debug_messages = debug_messages

        # RabbitMQ setup properties
        self.autodelete_queues = True
        # Explicit delete (after algorithm node finishes). Beware this means you need to delete them yourself (if False and autodelete_queues = False)!
        self.delete_queue = delete_queue
        # Purge queue before starting algorithm, you never know...
        self.prepurge_queue = True

        super().__init__(delete_exchange, amqp_url=amqp_url)
