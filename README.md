
An AMQP and Redis, producer-consumer result store that facilitates IPC between client and celery worker process.
Altenatively, it may also be used instead of a result backend to communicate results back to the client.
Can be used a stand alone result store for producer-consumer style applications.

Usage: 

AMQP producer-consumer

Client code (consumer) that calls a celery task in an async manner.
Blocking consumer that blocks for message from celery worker process.


```python
    from amqp import BlockingConsumer, uid
    task_id = uid()
    consumer = BlockingConsumer(task_id)
    # pass task_id along to celery task
    async_result = my_celery_task.apply_async(args=(), kwargs=dict(task_id=task_id))
    # Block on a message from Producer
    message = consumer.get()
```

Task code (producer) 
```python
    from amqp import BlockingProducer

    # app is a celery.Celery() instance

    @app.task
    def hello_world(*args, **kwargs):
    task_id = kwargs.get('task_id')
    producer = BlockingProducer(task_id=task_id)
    # continue with task computation
    #...
    # communicate with client
    producer.send_message('hello world!')    
    
```

Note: Depending on the direction of message passing, producer-consumer may be reversed.
In other words, task may be a consumer while client code can be a producer. 

Redis producer-consumer
```python
    from redispy import RedisConsumer
    task_id = uid()
    consumer = RedisConsumer(task_id, poll_interval=0.5)
    # pass task_id along to celery task
    async_result = my_celery_task.apply_async(args=(), kwargs=dict(task_id=task_id))
    # Block on a message from Producer
    message = consumer.get()
```

```python
      from redispy import RedisProducer

    # app is a celery.Celery() instance

    @app.task
    def hello_world(*args, **kwargs):
    task_id = kwargs.get('task_id')
    producer = RedisProducer(task_id=task_id)
    # continue with task computation
    #...
    # communicate with client
    producer.send_message('hello world!')    
    
```