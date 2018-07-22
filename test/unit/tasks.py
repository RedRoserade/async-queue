import asyncio
import logging
from random import random

from .app import app

_log = logging.getLogger('tasks')


@app.task
async def fail():
    raise Exception("oops!")


@app.task
async def mul(n1, n2):
    sleep_time = random()

    _log.debug('Multiplying %s * %s after %s secs', n1, n2, sleep_time)

    await asyncio.sleep(sleep_time)

    return n1 * n2


@app.task
async def add(n1, n2):
    _log.debug('Adding %s + %s', n1, n2)

    tasks = [mul.apply_async(n1 + i, n2 + i + 1) for i in range(1000)]

    results = await asyncio.gather(*tasks)

    return sum((n1, n2, *results))
