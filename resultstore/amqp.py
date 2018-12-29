"""
Defines Blocking & Async  style Producers and Consumers.
"""
import pika.exceptions
import uuid

from pika import BlockingConnection

from resultstore.base import BaseConsumer, BaseProducer
from collections import deque
from contextlib import contextmanager
from time import sleep


def uid():
    return str(uuid.uuid4())


class PublisherPool(object):
    def __init__(self, max_connections=10, connection_params={}):
        """
        :param max_connections:
        :param connection_params:
        """
        self.num_connections = max_connections
        self.connection_params = connection_params
        self.publishers = deque()
        for _ in range(self.num_connections):
            self.publishers.append(BlockingConnection(**self.connection_params))
        self.used = set()

    @contextmanager
    def acquire(self):
        """
        :return:
        """
        connection = None
        while True:
            if self.publishers:
                connection = self.publishers.popleft()
            if connection:
                if connection not in self.used:
                    break
                else:
                    self.publishers.appendleft(connection)
            sleep(.01)

        try:
            self.used.add(connection)
            yield connection
        finally:
            self.release(connection)

    def release(self, connection):
        """
        :param connection:
        :return:
        """
        self.used.remove(connection)
        self.publishers.append(connection)

    def __del__(self):
        for publisher in self.publishers:
            channel = publisher.channel()
            if channel.is_open:
                channel.close()


class BlockingProducer(BaseProducer):

    def __init__(self, task_id, ack=True, exchange=None, serialization='dill', pool=None):
        """
        :param task_id:
        :param ack:
        :param exchange:
        :param serialization:
        :param pool:
        """
        self.pool = pool
        self.ack = ack
        self.connection = BlockingConnection() if not self.pool else None
        self.channel = self.connection.channel() if not self.pool else None
        self.exchange = exchange or 'amqp-store'
        if self.channel:
            self.channel.exchange_declare(exchange=self.exchange, exchange_type='direct')
        self.routing_key = task_id
        self.body = None
        super(BlockingProducer, self).__init__(serialization=serialization)

    def send_message(self, message):
        """
        :param message:
        :return:
        """
        self.body = self._serialize(message)
        if self.channel and not self.channel.is_open:
            raise pika.exceptions.ChannelClosed('Cannot send on a closed channel')
        if self.pool:
            with self.pool.acquire() as connection:
                connection.channel().basic_publish(
                    exchange=self.exchange,
                    routing_key=self.routing_key,
                    body=self.body
                )
        else:
            self.channel.basic_publish(
                exchange=self.exchange,
                routing_key=self.routing_key,
                body=self.body
            )

    def __del__(self):
        #TODO exchange unbind
        if not self.pool:
            if self.channel.is_open:
                self.channel.close()
            if self.connection.is_open:
                self.connection.close()


class BlockingConsumer(BaseConsumer):

    def __init__(self, task_id, ack=True, exchange=None, serialization='dill', connection_params={}, pool=None):
        """
        :param task_id:
        :param ack:
        :param exchange:
        :param serialization:
        :param connection_params:
        :param pool:
        """
        self.pool = pool
        self.ack = ack
        self.exchange = exchange or 'amqp-store'
        self.connection_params = connection_params
        self.queue_id = task_id
        if not self.pool:
            self._init()
        super(BlockingConsumer, self).__init__(serialization=serialization)

    def _init(self):
        """
        :return:
        """
        self.connection = BlockingConnection(**self.connection_params)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=self.exchange,
            exchange_type='direct'
        )
        self.channel.queue_declare(
            self.queue_id,
            auto_delete=False
        )
        self.channel.queue_bind(
            exchange=self.exchange,
            queue=self.queue_id
        )

    def _cleanup(self):
        """
        :return:
        """
        if self.channel.is_open:
            self.channel.queue_unbind(self.queue_id,
                                      exchange=self.exchange,
                                      routing_key=self.queue_id
                                      )
        self.channel.queue_delete(queue=self.queue_id)

        if self.connection.is_open:
            self.connection.close()

    def get(self):
        """
        :return:
        """
        body = None
        try:
            for method_frame, props, body in self.channel.consume(self.queue_id):
                body = self._deserialize(body)
                if self.ack:
                    self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                break

        finally:
            if not self.pool:
                self._cleanup()
            return body

    def __del__(self):
        if self.channel.is_open:
            self.channel.queue_unbind(exchange=self.exchange, queue=self.queue_id)
            self.channel.queue_delete(queue=self.queue_id)
            self.channel.close()

        if self.connection.is_open:
            self.connection.close()


