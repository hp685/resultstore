"""Tests blocking producer-client."""

from amqpstore.amqp import BlockingProducer, BlockingConsumer, uid
from multiprocessing.pool import ThreadPool


def test_send_get():
    id = uid()
    p = BlockingProducer(routing_key=id)
    c = BlockingConsumer(queue_id=id)
    message = 'Hello world!'
    p.send_message(message)
    assert message == c.get()


def test_multiple_send_get():
    num_requests = 30
    result = []
    message = 'hello'
    consumers = [BlockingConsumer(queue_id=str(i)) for i in range(num_requests)]
    producers = [BlockingProducer(routing_key=str(i)) for i in range(num_requests)]
    pool = ThreadPool(num_requests)

    def get(c):
        assert c.get() == message

    def send(p):
        p.send_message(message)
    for c in consumers:
        pool.apply_async(get, (c,))
        assert c.get() == message

    for p in producers:
        pool.apply_async(send, (p,))
