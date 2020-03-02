# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import functools
import logging
import time
import pika
from connection import Manager

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOGGER = logging.getLogger(__name__)


class AlgorithmNode(object):
    """This is an example consumer that will reconnect if the nested
    Manager indicates that a reconnect is necessary.
    """
    _identifier = "GenericNode"

    def __init__(self, identifier, amqp_url="", on_message_callback_debug=None):
        self._identifier = identifier
        if callable(on_message_callback_debug):
            self._on_message_callback_debug = on_message_callback_debug

        self._reconnect_delay = 0
        LOGGER.info("Started node " + self._identifier)
        if amqp_url:
            self._amqp_url = amqp_url
        else:
            self._amqp_url = 'amqp://guest:guest@localhost:5672/%2F'
        self._consumer = Manager(self._amqp_url, self.on_message)

    def on_message(self, message_body):
        # consume message here, RabbitMQ ACK is taken care of in manager
        LOGGER.info(self._identifier + " received " + str(message_body))
        if callable(self._on_message_callback_debug):
            self._on_message_callback_debug(self._identifier, message_body)

    def run(self):
        while True:
            try:
                self._consumer.run()
            except KeyboardInterrupt:
                self._consumer.stop()
                break
            self._maybe_reconnect()

    def _maybe_reconnect(self):
        # You see when something reconnects here
        # Look into the Manager child class if you need more info
        if self._consumer.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            LOGGER.debug('Reconnecting after %d seconds', reconnect_delay)
            time.sleep(reconnect_delay)
            self._consumer = Manager(self._amqp_url)

    def _get_reconnect_delay(self):
        if self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay


def main():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    amqp_url = 'amqp://guest:guest@localhost:5672/%2F'
    consumer = ReconnectingConsumer(amqp_url)
    consumer.run()


if __name__ == '__main__':
    main()
