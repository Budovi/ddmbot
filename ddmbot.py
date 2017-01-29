import argparse
import asyncio
import configparser
import errno
import logging
import os
import time
from aiohttp.errors import DisconnectedError
from contextlib import suppress
from logging.handlers import TimedRotatingFileHandler
from websockets.exceptions import InvalidState

import discord
import discord.ext.commands as dec

import commandhandler
import database.bot
import database.common
import helpformatter
import player
import streamserver
import usermanager

# set up a logger
logging.Formatter.converter = time.gmtime
log = logging.getLogger('ddmbot')
log.setLevel(logging.INFO)

# load opus library if needed
if not discord.opus.is_loaded():
    discord.opus.load_opus('opus')


# helper function to take care of named pipe creation
def create_pipe(pipe_path):
    try:
        os.mkfifo(pipe_path, mode=0o600)
    except OSError as e:
        if not e.errno == errno.EEXIST:
            raise


#
# Safe Voice Client to use as an placeholder before voice connection is created
#
_VOICE_CHANNELS = 2
_VOICE_BITRATE = 48000


class DummyVoiceClient:
    def __init__(self):
        self.encoder = discord.opus.Encoder(_VOICE_BITRATE, _VOICE_CHANNELS)

    @staticmethod
    def is_connected():
        return False

    def play_audio(self, data, *, encode=True):
        pass


#
# Main DdmBot class (discord.ext.commands.Bot wrapper)
#
class DdmBot:
    def __init__(self, config_file):
        # read configuration
        self._config = configparser.ConfigParser(default_section='ddmbot')
        self._config.read(config_file)

        # create named pipes (FIFOs)
        create_pipe(self._config['ddmbot']['aac_pipe'])
        create_pipe(self._config['ddmbot']['int_pipe'])
        create_pipe(self._config['ddmbot']['pcm_pipe'])

        # create event loop and a new client (bot)
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._init_lock = asyncio.Lock(loop=self._loop)
        self._voice_lock = asyncio.Lock(loop=self._loop)
        self._initialized = asyncio.Event(loop=self._loop)
        self._client = dec.Bot(loop=self._loop, command_prefix=self._config['ddmbot']['delimiter'],
                               formatter=helpformatter.DdmBotHelpFormatter(),
                               help_attrs={'hidden': True, 'aliases': ['h']}, pm_help=True)

        # register event listeners
        self._client.event(self.on_error)
        self._client.event(self.on_message)
        self._client.event(self.on_ready)
        self._client.event(self.on_voice_state_update)

        # future runtime objects -- initialized to None
        self._database = None
        self._player = None
        self._server = None
        self._stream = None
        self._users = None

        self._command_handler = None

        self._text_channel = None
        self._log_channel = None
        self._voice_channel = None
        self._direct_channel = None

        self._operator_role = None

        self._voice_client = DummyVoiceClient()

        self._bot_task = None
        self._restart = False

    #
    # Methods for setup and cleanup
    #
    def run(self):
        try:
            self._database = database.bot.BotInterface(self._loop, self._config['ddmbot'])
            self._stream = streamserver.StreamServer(self)
            self._player = player.Player(self)
            self._users = usermanager.UserManager(self)
        except:
            self._loop.run_until_complete(self._client.close())
            self._loop.close()
            raise

        try:
            self._loop.run_until_complete(self._player.init())
            self._loop.run_until_complete(self._stream.init())
            self._loop.run_until_complete(self._client.login(self._config['discord']['token']))

            self._bot_task = asyncio.gather(self._database.task_credit_renew(), self._users.task_check_timeouts(),
                                            self._player.task_player_fsm(), self._client.connect(), loop=self._loop)

            try:
                self._loop.run_until_complete(self._bot_task)
            except asyncio.CancelledError:
                pass
            finally:
                self._bot_task.cancel()
                with suppress(asyncio.CancelledError):
                    self._loop.run_until_complete(self._bot_task)

            # determine if we wanna shut down the bot instead of restarting it
            if not self._restart:
                raise KeyboardInterrupt()

        finally:
            self._loop.run_until_complete(self._client.logout())
            self._loop.run_until_complete(self._player.cleanup())
            self._loop.run_until_complete(self._stream.cleanup())

            pending = asyncio.Task.all_tasks()
            for task in pending:
                task.cancel()
                with suppress(asyncio.CancelledError, asyncio.TimeoutError, InvalidState, OSError):
                    self._loop.run_until_complete(task)

            self._loop.close()

    async def shutdown(self):
        await asyncio.sleep(3, loop=self._loop)
        self._bot_task.cancel()

    def restart(self):
        self._restart = True
        return self.shutdown()

    #
    # Event listeners
    #
    async def on_ready(self):
        async with self._init_lock:
            if self._initialized.is_set():
                log.info('DdmBot connection to discord was restored')
                self._loop.create_task(self.connect_voice())
                return

            log.info('DdmBot connected as {0} (ID: {0.id})'.format(self._client.user))
            self._setup_discord_objects()
            self._initialized.set()

        await self.connect_voice()

        # populate initial listeners
        for member in self._voice_channel.voice_members:
            if member == self._client.user:
                continue
            with suppress(database.bot.IgnoredUserError):
                if await self._database.interaction_check(int(member.id)):
                    await self._send_welcome_message(member)
                await self._users.add_listener(int(member.id), direct=False)

        # enable commands by creating a command_handler object
        self._command_handler = commandhandler.CommandHandler(self)
        # at this point, bot should be ready
        log.info('Initialization done')

    async def on_message(self, message):
        # we don't want to process bot's messages at all
        if message.author == self._client.user:
            return
        # author of the message wrote something, which is kinda a proof (s)he is alive
        await self._users.refresh_activity(int(message.author.id))
        # do ignore list pre-check ourselves if this appears to be a command
        if message.content.lstrip().startswith(self._config['ddmbot']['delimiter']):
            with suppress(database.bot.IgnoredUserError):
                if await self._database.interaction_check(int(message.author.id)):
                    await self._send_welcome_message(message.author)
                await self._client.process_commands(message)
        # else if not command, check if the channel is the text_channel, if so, refresh the counter
        elif message.channel == self._text_channel:
            self._player.bump_protection_counter()

    async def on_voice_state_update(self, before, after):
        if before == self._client.user:
            if after.voice.voice_channel != self._voice_channel:
                log.warning('Client was disconnected from the voice channel')
                await self.connect_voice()
            return
        # joining
        if before.voice.voice_channel != self._voice_channel and after.voice.voice_channel == self._voice_channel:
            with suppress(database.bot.IgnoredUserError):
                if await self._database.interaction_check(int(after.id)):
                    await self._send_welcome_message(after)
                await self._users.add_listener(int(after.id), direct=False)

        # leaving
        elif before.voice.voice_channel == self._voice_channel and after.voice.voice_channel != self._voice_channel:
            try:
                await self._users.remove_listener(int(after.id), direct=False)
            except ValueError:
                log.warning('Tried to remove {0} (ID: {0.id}) from listeners but the user was not listed'.format(after))

    @staticmethod
    async def on_error(event, *args, **kwargs):
        raise

    #
    # Interaction methods
    #
    def message(self, message):
        return self._client.send_message(self._text_channel, message)

    def whisper(self, message):
        return self._client.whisper(message)

    def whisper_id(self, user_id, message):
        user = discord.utils.get(self._client.get_all_members(), id=str(user_id))
        if user is None:
            log.error('Cannot whisper user {} -- it\'s not a recognized server member'.format(user_id))
            return
        return self._client.send_message(user, message)

    def log(self, message):
        return self._client.send_message(self._log_channel, message)

    async def connect_voice(self):
        log.info('Connecting to the voice channel')
        if self._voice_lock.locked():
            log.warning('Connecting to the voice channel still in progress')
            return
        async with self._voice_lock:
            if self._client.is_voice_connected(self._server):
                log.warning('Client is still connected to the voice channel')
                return
            tmp = await self._client.join_voice_channel(self._voice_channel)
            tmp.encoder_options(sample_rate=_VOICE_BITRATE, channels=_VOICE_CHANNELS)
            self._voice_client = tmp  # TODO: atomicity provided by GIL
            log.info('Voice channel connection succeeded')

    async def wait_for_initialization(self):
        await self._initialized.wait()

    def is_operator(self, user):
        if not isinstance(user, discord.Member):
            user = self._server.get_member(user.id)
        return self._operator_role in user.roles

    @property
    def config(self):
        return self._config

    @property
    def loop(self):
        return self._loop

    @property
    def client(self):
        return self._client

    @property
    def player(self):
        return self._player

    @property
    def server(self):
        return self._server

    @property
    def stream(self):
        return self._stream

    @property
    def users(self):
        return self._users

    @property
    def voice(self):
        return self._voice_client

    @property
    def direct(self):
        return self._direct_channel

    #
    # Internal helpers
    #

    def _setup_discord_objects(self):
        # check the server count, this bot is meant to be run on a single server
        if not self._client.servers:
            raise RuntimeError('Bot needs a server to run on but is connected to none')
        if len(self._client.servers) > 1:
            raise RuntimeError('Bot is connected to multiple servers, multi-server configuration is not supported')
        # store a server variable for later use
        self._server = next(iter(self._client.servers))

        # locate the configured channels, starting with text_channel
        self._text_channel = self._server.get_channel(self._config['discord']['text_channel'])
        if self._text_channel is None:
            raise RuntimeError('Specified text_channel cannot be found')
        if self._text_channel.type != discord.ChannelType.text:
            raise RuntimeError('Specified text_channel is a wrong type')
        text_permissions = self._text_channel.permissions_for(self._server.me)
        if not text_permissions.send_messages:
            raise RuntimeError('Bot does not have a permission to send messages in the text_channel')
        if not text_permissions.read_messages:
            raise RuntimeError('Bot does not have a permission to read messages in the text_channel')
        if not text_permissions.manage_messages:
            raise RuntimeError('Bot does not have a permission to manage messages in the text_channel')

        # log_channel
        self._log_channel = self._server.get_channel(self._config['discord']['log_channel'])
        if self._log_channel is None:
            raise RuntimeError('Specified log_channel cannot be found')
        if self._log_channel.type != discord.ChannelType.text:
            raise RuntimeError('Specified log_channel is a wrong type')
        if not self._log_channel.permissions_for(self._server.me).send_messages:
            raise RuntimeError('Bot does not have a permission to send messages in the log_channel')

        # voice_channel
        self._voice_channel = self._server.get_channel(self._config['discord']['voice_channel'])
        if self._voice_channel is None:
            raise RuntimeError('Specified voice_channel cannot be found')
        if self._voice_channel.type != discord.ChannelType.voice:
            raise RuntimeError('Specified voice_channel is a wrong type')
        voice_permissions = self._voice_channel.permissions_for(self._server.me)
        if not voice_permissions.connect:
            raise RuntimeError('Bot does not have a permission to connect to the voice channel')
        if not voice_permissions.speak:
            raise RuntimeError('Bot does not have a permission to speak in the voice channel')

        # direct_channel -- optional, for seamless stream switch feature
        if 'direct_channel' in self._config['discord'] and self._config['discord']['direct_channel']:
            self._direct_channel = self._server.get_channel(self._config['discord']['direct_channel'])
            if self._direct_channel is None:
                raise RuntimeError('Specified direct_channel cannot be found')
            if self._direct_channel.type != discord.ChannelType.voice:
                raise RuntimeError('Specified direct_channel is a wrong type')
            if not voice_permissions.move_members:
                raise RuntimeError('Bot does not have a permission to move members, either grant it this permission '
                                   'or disable seamless stream switch feature')

        # look up the operator role
        def get_server_roles():
            for role in self._server.roles:
                yield role

        self._operator_role = discord.utils.get(get_server_roles(), id=self._config['discord']['operator_role'])
        if self._operator_role is None:
            raise RuntimeError('Operator role specified cannot be found')

    def _send_welcome_message(self, user):
        return self._client.send_message(user, self._config['ddmbot']['welcome_message']
                                         .format_map(self._config['ddmbot']))


if __name__ == '__main__':
    # parse arguments
    argument_parser = argparse.ArgumentParser(description='Discord Direct Music Bot (DdmBot)')
    argument_parser.add_argument('-c', '--config-file', nargs=1, default='config.ini')
    argument_parser.add_argument('-l', '--log-file', nargs=1, default='ddmbot.log')
    arguments = argument_parser.parse_args()

    # set up logging
    stderr_logger = logging.StreamHandler()
    stderr_logger.setFormatter(logging.Formatter('{asctime} | {levelname:<8} {message}', '%Y-%m-%d %H:%M:%S',
                                                 style='{'))
    log.addHandler(stderr_logger)
    file_logger = TimedRotatingFileHandler(arguments.log_file, when='midnight', backupCount=3, utc=True)
    file_logger.setFormatter(logging.Formatter('{asctime} | {name:<20} | {levelname:<8} {message}',
                                               '%Y-%m-%d %H:%M:%S', style='{'))
    log.addHandler(file_logger)

    try:
        # now there should be an infinite loop trying to fix everything...
        while True:
            # create a ddmbot instance
            ddmbot = DdmBot(arguments.config_file)
            # without a database there is no point in proceeding
            database.common.initialize(ddmbot.config['ddmbot']['db_file'])

            try:
                ddmbot.run()
            except KeyboardInterrupt:
                raise
            except (discord.ConnectionClosed, discord.GatewayNotFound, discord.HTTPException, DisconnectedError):
                log.exception('DdmBot finished with an exception, retrying in 60 seconds')
                time.sleep(60)
            finally:
                # we should always do this to ensure database consistency
                database.common.close()

    except KeyboardInterrupt:
        log.info('DdmBot terminated')
    except Exception:
        log.critical('DdmBot crashed with an exception', exc_info=True)
        raise
