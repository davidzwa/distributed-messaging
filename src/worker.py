import uuid
import time
import pika
import asyncio
import random
from utils.color import style
from utils import humanhash


class Worker:
    identifier = ""
    connection: pika.BaseConnection
    channel = None

    def __init__(self):
        """worker function"""
        self.identifier = humanhash.get_unique_name(
            str(uuid.uuid4())[:4].upper())
        self.__log_info(style.GREEN(
            "Created [Worker {}]".format(self.identifier)))

    async def run(self):
        # Getting connection & channel to RabbitMQ
        self.set_rabbit_channel()

        # Run code here
        await asyncio.sleep(1 + 2.0 * random.random())

        self.channel.queue_declare(queue='hello')
        self.channel.basic_consume(queue='hello',
                                   auto_ack=True,
                                   on_message_callback=self.callback)

        # self.channel.start_consuming()
        asyncio.create_task(self.start_consuming())

        await asyncio.sleep(1 + 2.0 * random.random())
        self.channel.basic_publish(exchange='',
                                   routing_key='hello',
                                   body='Hello World from ' + self.identifier)
        # print(" [x] Sent 'Hello World!'")

        # Quitting, cleaning up
        await asyncio.sleep(1 + 2.0 * random.random())
        # self.connection.close()
        self.__log_info(style.UNDERLINE("Done"))

    async def start_consuming(self):
        self.__log_info('Waiting for messages.')
        # self.channel.start_consuming()

    def callback(self, ch, method, properties, body):
        self.__log_info("Received %r" % body)

    def set_rabbit_channel(self):
        self.connection = pika.SelectConnection(
            # pika.ConnectionParameters('localhost'),
            on_open_callback=self.on_open)

        self.connection.ioloop.start()
        # self.connection.ioloop.start()
        # self.channel = self.connection.channel(on_open_callback=self.on_open)
        self.__log_info('Channel open?', style.BLUE(self.channel.is_open))

    def on_open(connection):
        print('connection open')
        connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(channel):
        print('channel open')
        channel.basic_publish('exchange_name',
                              'routing_key',
                              'Test Message',
                              pika.BasicProperties(content_type='text/plain',
                                                   type='example'))

    def __log_info(self, *values: object):
        print(style.YELLOW('[Worker {}]'.format(self.identifier)), end=' ')
        print(*values)
