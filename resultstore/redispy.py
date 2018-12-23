
from redis import StrictRedis

from time import sleep

from base import BaseConsumer, BaseProducer

_DEFAULT_HOST = 'localhost'
_DEFAULT_PORT = 6379
_RESULT_STORE = 'result-store'


def _task_id(task_id):
    return '{0}_{1}'.format(_RESULT_STORE, task_id)


class RedisProducer(BaseProducer):

    def __init__(self, task_id, expiry=None, host=_DEFAULT_HOST, port=_DEFAULT_PORT, serialization='dill'):
        self.host = host
        self.port = port
        self.client = StrictRedis(host=self.host, port=self.port)
        self.task_id = _task_id(task_id)
        self.expiry = expiry
        self.body = None
        super(RedisProducer, self).__init__(serialization=serialization)

    def send_message(self, body):
        self.body = self._serialize(body)
        if self.expiry:
            self.client.setex(self.task_id, self.expiry, self.body)
        else:
            self.client.set(self.task_id, self.body)


class RedisConsumer(BaseConsumer):

    def __init__(self, task_id, host=_DEFAULT_HOST, port=_DEFAULT_PORT, serialization='dill'):
        self.host = host
        self.port = port
        self.client = StrictRedis(host=self.host, port=self.port)
        self.task_id = _task_id(task_id)
        super(RedisConsumer, self).__init__(serialization=serialization)

    def get(self, polling_interval=0.1):
        while True:
            result = self.client.get(self.task_id)
            if result:
                if self.client.ttl(self.task_id):
                    return self._deserialize(result)
                self.client.delete(self.task_id)
                return self._deserialize(result)
            sleep(polling_interval)
