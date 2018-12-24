
An AMQP and Redis, producer-consumer result store that facilitates IPC between client and celery worker process.
Can be used a stand alone result store for producer-consumer style applications.

Usage: 

Client code (consumer) using custom task-id

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
    @app.task
    def hello_world(*args, **kwargs):
    task_id = kwargs.get('task_id')
    producer = BlockingProducer(task_id=task_id)
    # continue with task computation
    ...
    # communicate with client
    producer.send_message('hello world!')    
    
```

