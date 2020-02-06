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
        self.get_rabbit_channel()

        # Run code here
        await asyncio.sleep(1 + random.random())

        self.__log_info(style.UNDERLINE("Done"))
        return

    def get_rabbit_channel(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.__log_info('Channel open?', style.BLUE(self.channel.is_open))
        return

    def __log_info(self, *values: object):
        print(style.YELLOW('[Worker {}]'.format(self.identifier)), end=' ')
        print(*values)
