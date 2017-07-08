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


class ListenerInfo:
    __slots__ = ['_last_activity', '_is_direct', 'notified_dj', 'notified_ds']

    def __init__(self, *, direct):
        self._last_activity = datetime.datetime.now()
        self._is_direct = direct
        self.notified_dj = False
        self.notified_ds = False

    def refresh(self):
        self._last_activity = datetime.datetime.now()
        self.notified_dj = False
        self.notified_ds = False

    @property
    def last_activity(self):
        return self._last_activity

    @property
    def is_direct(self):
        return self._is_direct


class UserManager:
    def __init__(self, bot):
        config = bot.config['ddmbot']
        self._config_ds_token_timeout = datetime.timedelta(seconds=int(config['ds_token_timeout']))
        self._config_ds_notify_time = datetime.timedelta(seconds=int(config['ds_notify_time']))
        self._config_ds_remove_time = datetime.timedelta(seconds=int(config['ds_remove_time']))
        self._config_dj_notify_time = datetime.timedelta(seconds=int(config['dj_notify_time']))
        self._config_dj_remove_time = datetime.timedelta(seconds=int(config['dj_remove_time']))

        self._bot = bot

        self._lock = asyncio.Lock(loop=bot.loop)

        self._tokens = dict()  # maps token (string) -> (timestamp, user)
        self._listeners = dict()  # maps discord_id (int) -> ListenerInfo
        self._anonymous = 0
        self._queue = collections.deque()

    #
    # API for displaying information
    #
    async def get_display_info(self):
        async with self._lock:
            direct_listeners = {key for key, value in self._listeners.items() if value.is_direct}
            return len(self._listeners), self._anonymous, direct_listeners, list(self._queue)

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
            if user in self._listeners and not self._listeners[user].is_direct and self._bot.direct is None:
                log.debug('Token {} is valid for user {}, but the user is connected using discord'.format(token, user))
                return None
            log.debug('Token {} verification passed, associated user: {}'.format(token, user))
            return user

    #
    # API for the player
    #
    def get_current_listeners(self):
        return set(self._listeners.keys()), self._anonymous

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
    async def add_listener(self, discord_id, *, direct):
        async with self._lock:
            if discord_id in self._listeners:
                info = self._listeners[discord_id]

                if info.is_direct and not direct:
                    # someone using a direct stream connected to the voice channel
                    log.debug('Switching user {} from direct stream to discord channel'.format(discord_id))
                    self._bot.loop.create_task(self._bot.stream.disconnect(discord_id))
                elif not info.is_direct and direct:
                    # some connected to a voice channel switched to the direct stream
                    if self._bot.direct is None:
                        log.error('User {} has connected to the direct stream while using voice channel, but this '
                                  'should not be possible in the current setup'.format(discord_id))
                        return
                    log.debug('Switching user {} from discord channel to direct stream'.format(discord_id))
                    member = discord.utils.get(self._bot.server.members, id=str(discord_id))
                    if member is None:
                        log.error('Cannot move user {} -- it\'s not a recognized server member'.format(discord_id))
                    else:
                        self._bot.loop.create_task(self._bot.client.move_member(member, self._bot.direct))
                elif not info.is_direct and not direct:
                    # something fishy is going on...
                    log.error('Tried to add user {} with the discord channel connection twice'.format(discord_id))
                    return
                # last combination is direct stream -> direct stream, there is nothing weird about that

            # now add the user to the listeners, rewriting previous entry if present
            self._listeners[discord_id] = ListenerInfo(direct=direct)

            self._bot.loop.create_task(self._bot.player.users_changed(set(self._listeners.keys()),
                                                                      bool(self._anonymous), bool(self._queue)))

    async def remove_listener(self, discord_id, *, direct):
        async with self._lock:
            # ignore "incompatible" removes
            if discord_id not in self._listeners:
                raise ValueError('User is not listening')
            if self._listeners[discord_id].is_direct != direct:
                log.debug('Ignoring incompatible remove for user {} (probably moved)'.format(discord_id))
                return
            # remove the user from the queue, if present
            with suppress(ValueError):
                self._queue.remove(discord_id)
            # remove the user from the listeners
            self._listeners.pop(discord_id)

            self._bot.loop.create_task(self._bot.player.users_changed(set(self._listeners.keys()),
                                                                      bool(self._anonymous), bool(self._queue)))

    async def join_queue(self, discord_id):
        async with self._lock:
            if discord_id not in self._listeners:
                raise ValueError('You have to be listening to join the DJ queue')
            if discord_id in self._queue:
                return
            self._queue.append(discord_id)

            self._bot.loop.create_task(self._bot.player.users_changed(set(self._listeners.keys()),
                                                                      bool(self._anonymous), bool(self._queue)))

    async def leave_queue(self, discord_id):
        async with self._lock:
            try:
                self._queue.remove(discord_id)
            except ValueError as e:
                raise ValueError('You are not in the DJ queue') from e

            self._bot.loop.create_task(self._bot.player.users_changed(set(self._listeners.keys()),
                                                                      bool(self._anonymous), bool(self._queue)))

    async def move_listener(self, discord_id, position):
        if position < 1:
            raise ValueError('Position must be positive')
        async with self._lock:
            if discord_id not in self._listeners:
                raise ValueError('User must be listening to join the DJ queue')

            inserted = True
            with suppress(ValueError):
                self._queue.remove(discord_id)
                inserted = False
            self._queue.insert(position - 1, discord_id)

            self._bot.loop.create_task(self._bot.player.users_changed(set(self._listeners.keys()),
                                                                      bool(self._anonymous), bool(self._queue)))
            return inserted, min(len(self._queue), position)

    async def update_anonymous(self, new_count):
        async with self._lock:
            self._anonymous = new_count
            self._bot.loop.create_task(self._bot.player.users_changed(set(self._listeners.keys()),
                                                                      bool(self._anonymous), bool(self._queue)))

    async def generate_token(self, discord_id):
        # limit time spent in the critical section -- get the time and generate the token in advance
        current_time = datetime.datetime.now()
        token = ''.join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(64))
        async with self._lock:
            # key collisions are possible, but should be negligible
            log.debug('Added token {} for user {}'.format(token, discord_id))
            self._tokens[token] = (current_time, discord_id)
            return token

    #
    # API for activity update
    #
    async def refresh_activity(self, discord_id):
        async with self._lock:
            if discord_id in self._listeners:
                log.debug('Refreshing activity for user {}'.format(discord_id))
                info = self._listeners[discord_id]
                if info.notified_dj or info.notified_ds:
                    self._whisper(discord_id, 'Your inactivity timer has been reset successfully')
                info.refresh()

    #
    # Internal timeout checking task
    #
    def _whisper(self, user_id, message):
        self._bot.loop.create_task(self._bot.whisper_id(user_id, message))

    async def task_check_timeouts(self):
        while True:
            await asyncio.sleep(20, loop=self._bot.loop)
            current_time = datetime.datetime.now()
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
                    info = self._listeners[dj]
                    time_diff = current_time - info.last_activity
                    if time_diff > self._config_dj_remove_time:
                        self._whisper(dj, 'You have been removed from the DJ queue due to inactivity')
                        remove_djs.add(dj)
                    elif time_diff > self._config_dj_notify_time and not info.notified_dj:
                        log.info('DJ {} notified for being inactive'.format(dj))
                        self._whisper(dj, 'You\'re about to be removed from the DJ queue due to inactivity.\n'
                                          'Please reply to this message to prevent that.')
                        info.notified_dj = True
                # and all the direct listeners too
                for listener, info in self._listeners.items():
                    if not info.is_direct:
                        continue
                    time_diff = current_time - info.last_activity
                    if time_diff > self._config_ds_remove_time:
                        self._whisper(listener, 'You have been disconnected from the stream due to inactivity')
                        remove_listeners.add(listener)
                    elif time_diff > self._config_ds_notify_time and not info.notified_ds:
                        log.info('Listener {} notified for being inactive'.format(listener))
                        self._whisper(listener, 'You\'re about to be disconnected from the stream due to inactivity.\n'
                                                'Please reply to this message to prevent that.')
                        info.notified_ds = True

                # now it is save to edit lists / dictionaries
                for token in remove_tokens:
                    log.info('Token {} has timed out'.format(token))
                    self._tokens.pop(token)
                for dj in remove_djs:
                    log.info('DJ {} has timed out'.format(dj))
                    self._queue.remove(dj)
                for listener in remove_listeners:
                    log.info('Listener {} has timed out'.format(listener))
                    self._bot.loop.create_task(self._bot.stream.disconnect(listener))
                    with suppress(ValueError):
                        self._queue.remove(listener)
                    self._listeners.pop(listener)

            # now update the player
            if remove_listeners or remove_djs:
                self._bot.loop.create_task(self._bot.player.users_changed(set(self._listeners.keys()),
                                                                          bool(self._anonymous), bool(self._queue)))
