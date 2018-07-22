import asyncio
import logging

from .app import app
from . import tasks

logging.basicConfig(level=logging.DEBUG, format='[%(process)d %(threadName)s - %(asctime)s] %(name)s %(levelname)s - %(message)s')

_log = logging.getLogger('server')


async def main():
    await app.start()

    _log.info('started')

if __name__ == '__main__':
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(main())
    event_loop.run_forever()
