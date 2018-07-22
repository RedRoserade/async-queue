import asyncio
import logging

from test.unit.app import app
from .tasks import add

logging.basicConfig(level=logging.DEBUG, format='[%(process)d %(threadName)s - %(asctime)s] %(name)s %(levelname)s - %(message)s')

_log = logging.getLogger(__name__)


async def main():
    result = await add.apply_async(2, 2)

    _log.info('add result is is %s', result)

if __name__ == '__main__':
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(main())
