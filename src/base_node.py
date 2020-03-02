import functools
import logging
import time
import asyncio
import abc
from algorithm_config import AlgorithmConfig
from aio_pika import connect_robust, Message, IncomingMessage, ExchangeType

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
# logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
LOGGER = logging.getLogger(__name__)


class BaseNode(object):
    """
        This is an example consumer that will connect with asynchronous connection
        Find documentation on aio_pika here https://aio-pika.readthedocs.io/en/latest/quick-start.html
        Find documentation on asyncio here https://docs.python.org/3/library/asyncio.html
    """
    _amqp_url = "amqp://guest:guest@localhost:5672/%2F"
    _identifier = "UnsetIdentifier"
    _exchange_name = "main_default_exchange"
    _connection = None
    _channel = None
    _queue = None
    _exchange = None

    def __init__(self, config: AlgorithmConfig, on_message_receive_debug=None):
        self._config = config
        self._identifier = config.identifier
        self._exchange_name = config.exchange_name
        LOGGER.debug("Started async_node " + self._identifier)
        if self._config.amqp_url:
            self._amqp_url = self._config.amqp_url
        if callable(on_message_receive_debug):
            self._on_message_callback_debug = on_message_receive_debug

    def get_identifier(self):
        return self._identifier

    @abc.abstractmethod
    async def setup_connection(self, loop):
        """
        # Suggestion:
        queue_name = self._identifier
        await self.init_connection(loop=loop)
        # Choose fanout, topic or direct here
        await self.init_topic_messaging(queue_name, exchange_name=self._exchange_name)
        """
        raise NotImplementedError(self._identifier +
                                  ": Class function setup_connection() is not implemented, but is abstract.")

    @abc.abstractmethod
    async def run_core(self):
        raise NotImplementedError(self._identifier +
                                  ": Class function run_core() is not implemented, but is abstract.")

    def start(self):
        try:
            # Create asyncio event-loop for co-routines
            loop = asyncio.new_event_loop()
            loop.run_until_complete(self.run_async(loop))
        except RuntimeError as e:
            print("Loop exception " + str(e))

    async def run_async(self, loop):
        await self.setup_connection(loop)

        # Call main loop, implemented by user
        if not self.run_core or not callable(self.run_core):
            LOGGER.error(
                "Provide a run_core() implementation in your extending class, because run_core() is not callable right now!")
        else:
            # Blocking wait
            await self.run_core(loop)

        # Cleanup
        await self.close_connection(delete_queue=self._config.delete_queue)
        LOGGER.info(self._identifier + " done and cleaned up after it.")

    async def __receive_message(self, message: IncomingMessage):
        """ Left private so we can explicitly bubble up the message any extending class """
        async with message.process():
            if asyncio.iscoroutinefunction(self._on_message_callback_debug):
                await self._on_message_callback_debug(self._identifier, message)
            elif callable(self._on_message_callback_debug):
                self._on_message_callback_debug(self._identifier, message)
            else:
                LOGGER.info(
                    self._identifier + " received (unhandled) message on exchange '{}'".format(message.exchange))

    async def publish_message(self, message: Message, target_node_queue, default_exchange=False):
        if self._exchange is None:
            LOGGER.error(
                "Exchange is None or is_closed() is true. Have you initialized the exchange and queue by calling init_fanout_messaging, init_topic_messaging or init_direct_messaging?")
        if default_exchange == False:
            await self._exchange.publish(message, target_node_queue)
        else:
            # Direct messaging (default_exchange is always available)!
            # Routing key = target queue
            await self._channel.default_exchange.publish(message, target_node_queue)

    async def init_connection(self, loop):
        self._connection = await connect_robust(
            self._amqp_url, loop=loop
        )
        if self._connection and not self._connection.is_closed:
            self._channel = await self._connection.channel()
        else:
            LOGGER.error(
                "Connection is closed, or was never initiated. Check your RabbitMQ connection.")

    async def init_fanout_messaging(self, queue_name, exchange_name):
        """
        Reach the whole set of nodes by exchange-queue bindings (multicast, easiest). 
        """
        # Declaring exchange & bind queue to it
        await self.bind_queue_exchange(
            queue_name, exchange_name, exchange_type=ExchangeType.FANOUT)

    async def init_topic_messaging(self, queue_name, exchange_name):
        """
        Only reach a subset of nodes by topic (multicast), you need to book-keep the topic(s). 
        Note: topic has optional wildcard options, f.e. 'node_topic.*' or inversely 'node_topic.id_1' making it quite handy.
        """
        # Declaring exchange & bind queue to it
        await self.bind_queue_exchange(
            queue_name, exchange_name, exchange_type=ExchangeType.TOPIC)

    async def init_direct_messaging(self, queue_name, exchange_name):
        """
        Only reach one node by explicit name (unicast).
        Note: in order to know which queue's there are you need to do some book-keeping 
            Options:
                - (dynamic) pyrabbit RabbitMQ-management plugin query on f.e. 'localhost:15672'
                - (static) book-keeping of queue-names
        """
        # Declaring exchange & bind queue to it
        await self.bind_queue_exchange(
            queue_name, exchange_name, exchange_type=ExchangeType.DIRECT)

    async def bind_queue_exchange(self, queue_name, exchange_name, exchange_type=ExchangeType.FANOUT):
        if not self._channel or self._channel.is_closed:
            LOGGER.error(
                "Channel is closed, or was never initiated. Did you call 'init_connection()'? Also, check your RabbitMQ connection or check for previous channel errors.")
        exchange = await self._channel.declare_exchange(exchange_name, type=exchange_type, auto_delete=self._config.autodelete_exchange)
        queue = await self._channel.declare_queue(queue_name, auto_delete=self._config.autodelete_queue)
        # Purges the queue, handy if previous simulations left behind stuff (and autodelete = False)
        if self._config.prepurge_queue:
            await queue.purge()
        await queue.bind(exchange)
        await queue.consume(self.__receive_message)

        self._exchange = exchange
        self._queue = queue

    async def close_connection(self, delete_queue):
        if self._queue:
            await self._queue.unbind(self._exchange)
            if self._config.delete_queue:
                # await self._queue.purge()
                await self._queue.delete(if_unused=False, if_empty=False)
        if self._exchange and self._config.delete_exchange:
            await self._exchange.delete()
        await self._connection.close()


def main():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    amqp_url = 'amqp://guest:guest@localhost:5672/%2F'
    consumer = BaseNode(amqp_url)
    consumer.__run()


if __name__ == '__main__':
    main()
