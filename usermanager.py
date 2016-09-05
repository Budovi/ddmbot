import collections
import logging
import string
import random
import datetime
import asyncio
from contextlib import suppress

import discord.utils

# set up the logger
log = logging.getLogger('ddmbot.usermanager')


class UserManager:
    def __init__(self, config, bot, aac_server):
        self._config_ds_token_timeout = datetime.timedelta(seconds=int(config['ds_token_timeout']))
        self._config_ds_notify_time = datetime.timedelta(seconds=int(config['ds_notify_time']))
        self._config_ds_remove_time = datetime.timedelta(seconds=int(config['ds_remove_time']))
        self._config_dj_notify_time = datetime.timedelta(seconds=int(config['dj_notify_time']))
        self._config_dj_remove_time = datetime.timedelta(seconds=int(config['dj_remove_time']))

        self._bot = bot
        self._aac_server = aac_server

        self._lock = asyncio.Lock(loop=bot.loop)

        self._tokens = dict()  # maps token (string) -> (timestamp, user)
        self._listeners = dict()  # maps discord_id (int) -> {'active': timestamp, 'direct': boolean, 'notified_dj'...}
        self._queue = collections.deque()

        self._player = None
        self._timeout_task = None

    #
    # Resource management wrappers
    #
    def init(self, player):
        self._player = player
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
                log.debug('Token {} verification failed'.format(token))
                return None
            timestamp, user = self._tokens[token]
            # only one connection is possible at the time
            # duplicate token will time out eventually
            if user in self._listeners and not self._listeners[user]['direct']:
                log.debug('Token {} is valid for user {}, but the user is connected using discord'.format(token, user))
                return None
            log.debug('Token {} verification passed, associated user: {}'.format(token, user))
            return user

    #
    # API for the player
    #
    def someone_listening(self):
        return bool(self._listeners)

    async def get_next_dj(self):
        async with self._lock:
            if not self._queue:
                return None
            discord_id = self._queue.popleft()
            self._queue.append(discord_id)
            return discord_id

    async def clear_queue(self):
        async with self._lock:
            self._queue.clear()

    #
    # API for user manipulation
    #
    async def add_listener(self, discord_id, token=None):
        current_time = datetime.datetime.now()
        async with self._lock:
            if discord_id in self._listeners and token is None and self._listeners[discord_id]['direct']:
                # someone using a direct stream switches over to the discord voice
                log.debug('Switching user {} from direct to discord stream'.format(discord_id))
                self._bot.loop.create_task(self._aac_server.disconnect(discord_id))

            self._listeners[discord_id] = {'active': current_time, 'direct': token is not None, 'notified_ds': False,
                                           'notified_dj': False}

            self._player.users_changed()

    async def remove_listener(self, discord_id):
        async with self._lock:
            # remove the user from the queue, if present
            with suppress(ValueError):
                self._queue.remove(discord_id)
            try:
                self._listeners.pop(discord_id)
            except KeyError:
                raise ValueError('User is not listening')

            self._player.users_changed()

    async def join_queue(self, discord_id):
        async with self._lock:
            if discord_id not in self._listeners:
                raise ValueError('User is not listening')
            if discord_id in self._queue:
                return
            self._queue.append(discord_id)

            if len(self._queue) == 1:
                self._player.cooldown_set()
            self._player.users_changed()

    async def leave_queue(self, discord_id):
        async with self._lock:
            try:
                self._queue.remove(discord_id)
            except ValueError:
                raise ValueError('User is not in queue')

            if not self._queue:
                self._player.cooldown_reset()
            self._player.users_changed()

    async def generate_urls(self, discord_id):
        # limit time spent in the critical section -- get the time and generate the token in advance
        current_time = datetime.datetime.now()
        token = ''.join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(64))
        async with self._lock:
            # key collisions are possible, but should be negligible
            log.debug('Added token {} for user {}'.format(token, discord_id))
            self._tokens[token] = (current_time, discord_id)
            return self._aac_server.playlist_url_format.format(token), self._aac_server.stream_url_format.format(token)

    #
    # API for activity update
    #
    async def refresh_activity(self, discord_id):
        current_time = datetime.datetime.now()
        async with self._lock:
            if discord_id in self._listeners:
                log.debug('Refreshing activity for user {}'.format(discord_id))
                attributes = self._listeners[discord_id]
                attributes['active'] = current_time
                attributes['notified_ds'] = False
                attributes['notified_dj'] = False

    #
    # Internal timeout checking task
    #
    def _whisper(self, user_id, message):
        user = discord.utils.get(self._bot.get_all_members(), id=str(user_id))
        if user is None:
            return
        self._bot.loop.create_task(self._bot.send_message(user, message))

    async def _check_timeouts(self):
        while True:
            await asyncio.sleep(20, loop=self._bot.loop)
            current_time = datetime.datetime.now()
            log.debug('Starting inactivity check')
            # sets used to store users and tokens to remove
            remove_tokens = set()
            remove_djs = set()
            remove_listeners = set()
            async with self._lock:
                # check all tokens
                for token, (created, user) in self._tokens.items():
                    if current_time - created > self._config_ds_token_timeout:
                        remove_tokens.add(token)
                # check all djs
                for dj in self._queue:
                    attributes = self._listeners[dj]
                    time_diff = current_time - attributes['active']
                    if time_diff > self._config_dj_remove_time:
                        self._whisper(dj, 'You have been removed from the DJ queue due to inactivity')
                        remove_djs.add(dj)
                    elif time_diff > self._config_dj_notify_time and not attributes['notified_dj']:
                        log.info('DJ {} notified for being inactive'.format(dj))
                        self._whisper(dj, 'You\'re about to be removed from the DJ queue due to inactivity.\n'
                                          'Please reply to this message to prevent that.')
                        attributes['notified_dj'] = True
                # and all the direct listeners too
                for listener, attributes in self._listeners.items():
                    if not attributes['direct']:
                        continue
                    time_diff = current_time - attributes['active']
                    if time_diff > self._config_ds_remove_time:
                        self._whisper(listener, 'You have been disconnected from the stream due to inactivity')
                        remove_listeners.add(listener)
                    elif time_diff > self._config_ds_notify_time and not attributes['notified_ds']:
                        log.info('Listener {} notified for being inactive'.format(listener))
                        self._whisper(listener, 'You\'re about to be disconnected from the stream due to inactivity.\n'
                                                'Please reply to this message to prevent that.')
                        attributes['notified_ds'] = True

                # now it is save to edit lists / dictionaries
                for token in remove_tokens:
                    log.info('Token {} has timed out'.format(token))
                    self._tokens.pop(token)
                for dj in remove_djs:
                    log.info('DJ {} has timed out'.format(dj))
                    self._queue.remove(dj)
                for listener in remove_listeners:
                    log.info('Listener {} has timed out'.format(listener))
                    self._bot.loop.create_task(self._aac_server.disconnect(listener))
                    with suppress(ValueError):
                        self._queue.remove(listener)
                    self._listeners.pop(listener)

            # now update the player
            if remove_djs and not self._queue:
                    self._player.cooldown_reset()
            if remove_listeners or remove_djs:
                self._player.users_changed()
