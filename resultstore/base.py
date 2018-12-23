"""Base producer and consumer. """

import dill
import json
import pickle


class BaseProducer(object):
    def __init__(self, serialization='dill'):
        self.serialization_fmt = serialization

    def _serialize(self, body):
        if self.serialization_fmt == 'dill':
            return dill.dumps(body)
        elif self.serialization_fmt == 'pickle':
            return pickle.dumps(body)
        elif self.serialization_fmt == 'json':
            return json.dumps(body)


class BaseConsumer(object):
    def __init__(self, serialization='dill'):
        self.serialization_fmt = serialization

    def _deserialize(self, body):
        if self.serialization_fmt == 'dill':
            return dill.loads(body)
        elif self.serialization_fmt == 'pickle':
            return pickle.loads(body)
        elif self.serialization_fmt == 'json':
            return json.loads(body)