import collections
import string
import random
import datetime
import asyncio
from contextlib import suppress


class UserManager:
    def __init__(self, config, bot, server):
        self._config_ds_token_timeout = datetime.timedelta(seconds=int(config['ds_token_timeout']))
        self._config_ds_notify_time = datetime.timedelta(seconds=int(config['ds_notify_time']))
        self._config_ds_remove_time = datetime.timedelta(seconds=int(config['ds_remove_time']))
        self._config_dj_notify_time = datetime.timedelta(seconds=int(config['dj_notify_time']))
        self._config_dj_remove_time = datetime.timedelta(seconds=int(config['dj_remove_time']))

        self._bot = bot
        self._server = server

        self._lock = asyncio.Lock(loop=bot.loop)

        self._tokens = dict()  # maps token (string) -> (timestamp, user)
        self._listeners = dict()  # maps discord_id (int) -> {'active': timestamp, 'direct': boolean, 'notified_dj'...}
        self._queue = collections.deque()

        self._timeout_task = None

        self._on_first_listener = None
        self._on_first_dj = None
        self._on_last_dj = None

    #
    # Resource management wrappers
    #
    def init(self):
        self._timeout_task = self._bot.loop.create_task(self._check_timeouts())

    async def cleanup(self):
        if self._timeout_task is not None:
            self._timeout_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._timeout_task

    #
    # API for displaying information
    #
    async def get_state(self):
        async with self._lock:
            return set(self._listeners.keys()), list(self._queue)

    def is_listening(self, discord_id):
        return discord_id in self._listeners

    #
    # API for the direct stream server
    #
    async def get_token_owner(self, token):
        async with self._lock:
            # check if token is valid
            if token not in self._tokens:
                return None
            timestamp, user = self._tokens[token]
            # only one connection is possible at the time
            # duplicate token will time out eventually
            if user in self._listeners:
                return None
            return user

    #
    # API for the player
    #
    def someone_listening(self):
        return len(self._listeners) > 0

    async def get_next_dj(self):
        async with self._lock:
            if len(self._queue) == 0:
                return None
            discord_id = self._queue.popleft()
            self._queue.append(discord_id)
            return discord_id

    async def clear_queue(self):
        async with self._lock:
            self._queue.clear()

    def on_first_listener(self, func):
        assert callable(func)
        self._on_first_listener = func
        return func

    def on_first_dj(self, func):
        assert callable(func)
        self._on_first_dj = func
        return func

    def on_last_dj(self, func):
        assert callable(func)
        self._on_last_dj = func
        return func

    #
    # API for user manipulation
    #
    async def add_listener(self, discord_id, token=None):
        current_time=datetime.datetime.now()
        async with self._lock:
            if discord_id in self._listeners and token is None and self._listeners[discord_id]['direct']:
                # someone using a direct stream switches over to the discord voice
                self._bot.loop.create_task(self._server.disconnect(discord_id))

            self._listeners[discord_id] = {'active': current_time, 'direct': token is None, 'notified_ds': False,
                                           'notified_dj': False}
            # invalidate the token
            if token is not None:
                try:
                    self._tokens.pop(token)
                except KeyError:
                    print('Warning: adding listener with invalid token, race condition occurred')

            if len(self._listeners) == 1 and self._on_first_listener is not None:
                self._on_first_listener()

    async def remove_listener(self, discord_id):
        async with self._lock:
            # remove the user from the queue, if present
            try:
                self._queue.remove(discord_id)
            except ValueError:
                pass
            try:
                self._listeners.pop(discord_id)
            except KeyError:
                raise ValueError('User is not listening')

    async def join_queue(self, discord_id):
        async with self._lock:
            if discord_id not in self._listeners:
                raise ValueError('User is not listening')
            self._queue.append(discord_id)

            if len(self._queue) == 1 and self._on_first_dj is not None:
                self._on_first_dj()

    async def leave_queue(self, discord_id):
        async with self._lock:
            try:
                self._queue.remove(discord_id)
            except ValueError:
                raise ValueError('User is not in queue')

            if len(self._queue) == 0 and self._on_last_dj is not None:
                self._on_last_dj()

    async def generate_url(self, discord_id):
        # limit time spent in the critical section -- get the time and generate the token in advance
        current_time = datetime.datetime.now()
        token = ''.join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(64))
        async with self._lock:
            # key collisions are possible, but should be negligible
            self._tokens[token] = (current_time, discord_id)
            return self._server.url_format.format(token)

    #
    # API for activity update
    #
    async def refresh_activity(self, discord_id):
        current_time = datetime.datetime.now()
        async with self._lock:
            if discord_id in self._listeners:
                attributes = self._listeners[discord_id]
                attributes['active'] = current_time
                attributes['notified_ds'] = False
                attributes['notified_dj'] = False

    #
    # Internal timeout checking task
    #
    async def _check_timeouts(self):
        while True:
            await asyncio.sleep(20, loop=self._bot.loop)
            current_time = datetime.datetime.now()
            async with self._lock:
                # check all tokens
                for token, (created, user) in self._tokens.items():
                    if current_time - created > self._config_ds_token_timeout:
                        self._tokens.pop(token)
                # check all djs
                for dj in self._queue:
                    attributes = self._listeners[dj]
                    time_diff = current_time - attributes['active']
                    if time_diff > self._config_dj_remove_time:
                        self._queue.remove(dj)
                        # TODO: write a message
                    elif time_diff > self._config_dj_notify_time and not attributes['notified_dj']:
                        # TODO: notify
                        attributes['notified_dj'] = True
                # and all the direct listeners too
                for listener, attributes in self._listeners.items():
                    if attributes['direct']:
                        time_diff = current_time - attributes['active']
                        if time_diff > self._config_ds_remove_time:
                            self._bot.loop.create_task(self._server.disconnect(listener))
                            try:
                                self._queue.remove(listener)
                            except ValueError:
                                pass
                            self._listeners.pop(listener)
                            # TODO: write a message
                        elif time_diff > self._config_ds_notify_time and not attributes['notified_ds']:
                            # TODO: notify
                            attributes['notified_ds'] = True
