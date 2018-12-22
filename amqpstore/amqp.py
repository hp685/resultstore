"""
Defines Blocking & Async  style Producers and Consumers.
"""
import dill
import json
import pickle
import pika.exceptions
import uuid

from pika import BlockingConnection, SelectConnection, TornadoConnection


def uid():
    return str(uuid.uuid4())


class BaseProducer(object):
    def __init__(self, serialization='dill', ack=True):
        self.ack = ack
        self.serialization_fmt = serialization

    def _serialize(self, body):
        if self.serialization_fmt == 'dill':
            return dill.dumps(body)
        elif self.serialization_fmt == 'pickle':
            return pickle.dumps(body)
        elif self.serialization_fmt == 'json':
            return json.dumps(body)


class BaseConsumer(object):
    def __init__(self, serialization='dill', ack=True):
        self.ack = ack
        self.serialization_fmt = serialization

    def _deserialize(self, body):
        if self.serialization_fmt == 'dill':
            return dill.loads(body)
        elif self.serialization_fmt == 'pickle':
            return pickle.loads(body)
        elif self.serialization_fmt == 'json':
            return json.loads(body)


class BlockingProducer(BaseProducer):

    def __init__(self, exchange=None, routing_key=None, serialization='dill', ack=True):
        self.connection = BlockingConnection()
        self.channel = self.connection.channel()
        self.exchange = exchange or 'amqp-store'
        self.channel.exchange_declare(exchange=self.exchange, exchange_type='direct')
        self.routing_key = routing_key
        self.body = None
        super(BlockingProducer, self).__init__(serialization=serialization, ack=ack)

    def send_message(self, message):
        self.body = self._serialize(message)
        if not self.channel.is_open:
            raise pika.exceptions.ChannelClosed('Cannot send on a closed channel')
        self.channel.publish(
            exchange=self.exchange,
            routing_key=self.routing_key,
            body=self.body
        )

    def __del__(self):
        if self.channel.is_open:
            self.channel.close()
        if self.connection.is_open:
            self.connection.close()


class BlockingConsumer(BaseConsumer):

    def __init__(self, queue_id, ack=True, exchange=None, serialization='dill', connection_params={}):
        self.exchange = exchange or 'amqp-store'
        self.connection_params = connection_params
        self.connection = BlockingConnection(**self.connection_params)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=self.exchange,
            exchange_type='direct'
        )
        self.queue_id = queue_id
        self.channel.queue_declare(
            self.queue_id,
            auto_delete=False
        )
        self.channel.queue_bind(
            exchange=self.exchange,
            queue=self.queue_id
        )
        super(BlockingConsumer, self).__init__(serialization=serialization, ack=ack)

    def _cleanup(self):
        if self.channel.is_open:
            self.channel.queue_unbind(self.queue_id,
                                      exchange=self.exchange,
                                      routing_key=self.queue_id
                                      )
        self.channel.queue_delete(queue=self.queue_id)

        if self.connection.is_open:
            self.connection.close()

    def get(self):
        body = None
        try:
            for method_frame, props, body in self.channel.consume(self.queue_id):

                body = self._deserialize(body)
                if self.ack:
                    self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        finally:
            self._cleanup()
            return body

    def __del__(self):
        if self.channel.is_open:
            self.channel.queue_unbind(exchange=self.exchange, queue=self.queue_id)
            self.channel.queue_delete(queue=self.queue_id)
            self.channel.close()

        if self.connection.is_open:
            self.connection.close()


class AsyncoreProducer(BaseProducer):
    pass


class AsyncoreConsumer(BaseConsumer):
    pass
