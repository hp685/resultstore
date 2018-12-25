"""Tests blocking producer-client."""

from resultstore.amqp import BlockingProducer, BlockingConsumer, uid, PublisherPool
from resultstore.pyredis import RedisConsumer, RedisProducer

import pytest
import threading


@pytest.fixture
def publisher_pool():
    return PublisherPool(max_connections=30)


def test_send_get(publisher_pool):
    id = uid()
    p = BlockingProducer(task_id=id, pool=publisher_pool)
    c = BlockingConsumer(task_id=id)
    message = 'Hello world!'
    p.send_message(message)
    assert message == c.get()


def test_multiple_send_get(publisher_pool):
    num_requests = 30
    message = 'hello'
    consumers = [BlockingConsumer(task_id=str(i), ack=False) for i in range(num_requests)]
    producers = [BlockingProducer(task_id=str(i), ack=False, pool=publisher_pool) for i in range(num_requests)]
    threads = []

    def get(c):
        assert c.get() == message

    def send(p):
        p.send_message(message)

    for c in consumers:
        threads.append(threading.Thread(target=get, args=(c,)))

    for p in producers:
        threads.append(threading.Thread(target=send, args=(p,)))

    for t in threads:
        t.start()

    for t in threads:
        t.join()


def test_send_get_redis():
    id = uid()
    p = RedisProducer(task_id=id)
    c = RedisConsumer(task_id=id)
    message = 'Hello World!'
    p.send_message(message)
    assert c.get() == message


def test_multiple_send_get_redis():
    num_requests = 30
    message = 'hello'
    consumers = [RedisConsumer(task_id=str(i)) for i in range(num_requests)]
    producers = [RedisProducer(task_id=str(i)) for i in range(num_requests)]
    threads = []

    def get(c):
        assert c.get() == message

    def send(p):
        p.send_message(message)

    for c in consumers:
        threads.append(threading.Thread(target=get, args=(c,)))

    for p in producers:
        threads.append(threading.Thread(target=send, args=(p,)))

    for t in threads:
        t.start()

    for t in threads:
        t.join()

