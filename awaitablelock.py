import asyncio
import threading
import functools


class AwaitableLock:
    def __init__(self, loop=None):
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._main_lock = threading.Lock()

        self._coroutine_lock = asyncio.Lock(loop=self._loop)
        self._condition = asyncio.Condition(lock=self._coroutine_lock, loop=self._loop)

    async def _notify_next(self):
        async with self._coroutine_lock:
            self._condition.notify()

    async def __aenter__(self):
        async with self._coroutine_lock:
            # try to acquire main lock
            if self._main_lock.acquire(blocking=False):
                return
            # we do not own the lock thus we should wait on condition until we get it
            predicate = functools.partial(self._main_lock.acquire, blocking=False)
            await self._condition.wait_for(predicate=predicate)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._main_lock.release()
        await self._notify_next()

    def __enter__(self):
        # just seize the internal lock
        self._main_lock.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        # release the lock
        self._main_lock.release()
        # after that, we need to notify some coroutine if waiting for the lock
        asyncio.run_coroutine_threadsafe(self._notify_next(), self._loop)
