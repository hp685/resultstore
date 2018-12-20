
from exceptions import ChannelClosedError

import json
import pickle

from pika import BlockingConnection
from dill import dill


class AMQPStore(object):

    def __init__(self, exchange=None, routing_key=None, serialization='dill', ack=True):
        self.ack = ack
        self.connection = BlockingConnection()
        self.channel = self.connection.channel()
        self.exchange = exchange
        self.routing_key = routing_key
        self.message = None
        self.serialization_fmt = serialization
        self.body = dict(message=None, serialization_fmt=self.serialization_fmt)

    def _serialize(self):
        if self.serialization_fmt == 'dill':
            return dill.dumps(self.message)
        elif self.serialization_fmt == 'pickle':
            return pickle.dumps(self.message)
        elif self.serialization_fmt == 'json':
            return json.dumps(self.message)

    def send_message(self, message):
        self.message = message
        self.body['message'] = self._serialize()

        if not self.channel.is_open:
            raise ChannelClosedError('Cannot send message {0} on a closed channel'.format(self.message))

        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=self.routing_key,
            body=self.body
        )


class AMQPClient(object):

    def __init__(self, queue_id, **connection_params):
        self.connection = BlockingConnection(**connection_params)
        self.channel = self.connection.channel()
        self.queue_id = queue_id
        self.channel.basic_consume(self.queue_id)

    def get(self):
        self.start_consuming()
        if self.ack and self.channel.is_open:
            self.channel.basic_ack(delivery_tag)