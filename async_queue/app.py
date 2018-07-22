import asyncio
import inspect
import json
import logging
import time
import uuid
from threading import Thread

import aioamqp
from aioamqp.channel import Channel
from aioamqp.envelope import Envelope
from aioamqp.properties import Properties

_log = logging.getLogger('async_queue')


class Signature:
    def __init__(self, sig_id=None, *, app, name, args, kwargs):
        self.sig_id = sig_id or str(uuid.uuid4())

        self.name = name
        self.args = args
        self.kwargs = kwargs
        self._app = app

    async def apply_async(self):
        return await self._app.send(self)

    @property
    def payload(self):
        return json.dumps({
            'name': self.name,
            'args': self.args or [],
            'kwargs': self.kwargs or {}
        }, ensure_ascii=False)


class Task:

    def __init__(self, app: 'App', fn: callable):
        self._fn = fn
        self.app = app

    def __call__(self, *args, **kwargs):
        return self._fn(*args, **kwargs)

    def signature(self, args: tuple=None, kwargs: dict=None):
        return Signature(
            app=self.app,
            name=self._fn.__name__,
            args=args,
            kwargs=kwargs
        )

    async def apply_async(self, *args, **kwargs):
        return await self.signature(args=args, kwargs=kwargs).apply_async()


class App:
    def __init__(self):
        self._protocol: aioamqp.AmqpProtocol = None
        self._transport = None
        self._callback_queue = None
        self._channel: Channel = None
        self._corr_ids = dict()
        self._loop = asyncio.get_event_loop()
        self._id = str(uuid.uuid4()).replace('-', '')

        self._tasks = {}
        self._queues = set()

    async def connect(self):
        self._transport, self._protocol = await aioamqp.connect(loop=self._loop)

        self._channel = await self._protocol.channel()

    async def _on_response(self, channel: Channel, body, envelope: Envelope, properties: Properties):

        _log.debug('Got response for task id %s, queue=%s', properties.correlation_id, envelope.routing_key)

        if properties.correlation_id in self._corr_ids:
            _log.debug('Task id %s, queue=%s acknowledged.', properties.correlation_id, envelope.routing_key)
            await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

            correlation = self._corr_ids[properties.correlation_id]

            self._loop.call_soon_threadsafe(correlation['waiter'].set_result, json.loads(body))
        else:
            _log.debug("It seems this wasn't for us...")
            await channel.basic_client_nack(delivery_tag=envelope.delivery_tag)

    async def _on_request(self, channel: Channel, body, envelope: Envelope, properties):
        payload = json.loads(body)
        task_name = payload['name']

        _log.info('Received task: %s (%s)', task_name, properties.correlation_id)

        try:
            task_def = self._tasks[task_name]

            func = task_def['function']

            def call_function():

                if inspect.iscoroutinefunction(func):
                    loop: asyncio.BaseEventLoop = asyncio.new_event_loop()

                    result = loop.run_until_complete(func(*payload['args'], **payload['kwargs']))

                    loop.close()
                else:
                    result = func(*payload['args'], **payload['kwargs'])

                return result

            async def publish_result(result):
                _log.debug('Task %s succeeded with result: %s', task_name, result)

                await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

                await channel.basic_publish(
                    payload=json.dumps(result, ensure_ascii=False),
                    exchange_name='',
                    routing_key=properties.reply_to,
                    properties={
                        'correlation_id': properties.correlation_id
                    }
                )

            def run(parent_loop: asyncio.BaseEventLoop):
                result = call_function()

                asyncio.run_coroutine_threadsafe(publish_result(result), loop=parent_loop)

            Thread(target=run, args=(self._loop, )).start()
        except KeyError:
            await channel.basic_client_nack(delivery_tag=envelope.delivery_tag)
            _log.warning(f'Task "{task_name}" not found')

    async def _send_and_wait(self, sig: Signature):

        start = time.monotonic()

        if not self._protocol:
            await self.connect()

        queue = sig.name

        callback_queue = f'{queue}_callbacks@{self._id}'

        if callback_queue not in self._queues:
            await self._channel.queue_declare(callback_queue, no_wait=True, auto_delete=True)
            await self._channel.basic_consume(callback=self._on_response, queue_name=callback_queue, no_wait=True)

            _log.debug("Created callback queue %s for task %s", callback_queue, sig.name)

        _log.debug(
            'Sending task %s (%s) with args=%s, kwargs=%s on queue=%s',
            sig.name, sig.sig_id, sig.args, sig.kwargs, queue
        )

        f = asyncio.Future(loop=self._loop)

        self._corr_ids[sig.sig_id] = {
            'waiter': f
        }

        await self._channel.basic_publish(
            payload=sig.payload,
            exchange_name='',
            routing_key=sig.name,
            properties={
                'reply_to': callback_queue,
                'correlation_id': sig.sig_id
            }
        )

        _log.debug('Waiting for task %s with id=%s...', sig.name, sig.sig_id)

        result = await f

        end = time.monotonic()

        _log.debug('Task %s (%s) finished in %s seconds: %s', sig.name, sig.sig_id, end - start, result)

        del self._corr_ids[sig.sig_id]

        return result

    def send(self, sig: Signature):

        fut = asyncio.run_coroutine_threadsafe(self._send_and_wait(sig), self._loop)

        return asyncio.wrap_future(fut)

    async def start(self):
        if not self._protocol:
            await self.connect()

        # await self._channel.basic_qos(prefetch_count=1, prefetch_size=0, connection_global=False)

        for task in self._tasks:
            queue = self._tasks[task]['queue']

            _log.debug('Creating queue %s', queue)

            await self._channel.queue_declare(queue_name=queue, no_wait=True)

            self._queues.add(queue)

            await self._channel.basic_consume(self._on_request, queue_name=queue, no_wait=True)

            _log.debug('Queue %s created.', queue)

    def task(self, task_fn: callable):
        self._tasks[task_fn.__name__] = {
            'queue': task_fn.__name__,
            'function': task_fn
        }

        return Task(self, task_fn)
