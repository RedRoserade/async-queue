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
    sleep_time = 10

    _log.debug('Multiplying %s * %s after %s secs', n1, n2, sleep_time)

    await asyncio.sleep(sleep_time)

    return n1 * n2


@app.task
async def add(n1, n2):
    _log.debug('Adding %s + %s', n1, n2)

    results = await asyncio.gather(
        mul.apply_async(n1 + 1, n2 + 2),
        fail.apply_async(),
        mul.apply_async(n1 + 5, n2 + 6)
    )

    return sum((n1, n2, *results))
