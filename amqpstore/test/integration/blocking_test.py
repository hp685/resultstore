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
    pool = ThreadPool(100)
    ids = [uid() for _ in range(100)]
    result = []
    producers = [BlockingProducer(routing_key=ids[i], ack=False) for i in range(100)]
    consumers = [BlockingConsumer(queue_id=ids[i], ack=False) for i in range(100)]
    for idx, c in enumerate(consumers):
        def get(consumer):
            return consumer.get()
        result.append(pool.apply_async(get, (c,)))

    for idx, p in enumerate(producers):
        def send_message(p, idx):
            p.send_message(str(idx))
        pool.apply_async(send_message, (p, idx))

    for idx, r in enumerate(result):
        assert r.get() == str(idx)
