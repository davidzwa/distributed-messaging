import uuid
import time
import pika

class Worker:
    connection: pika.BaseConnection
    channel = None

    def __init__(self):
        """worker function"""

        # Generate short UUID
        identifier = str(uuid.uuid4())[:8]
        print('Worker GUID', identifier)

        # Getting connection & channel to RabbitMQ
        self.get_rabbit_channel()        

        time.sleep(15)
        print("Worker",identifier,"done")
        return

    def get_rabbit_channel(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        print('RabbitMQ Runner. Press Ctrl+C to cancel...')
        print('Channel open? ', self.channel.is_open) # ,channel.connection
        return