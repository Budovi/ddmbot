import asyncio
import configparser
import logging
import os
import time
from contextlib import suppress
from logging.handlers import TimedRotatingFileHandler

import discord
import discord.ext.commands as dec

import commands
import player as pl
import songmanager
import streamserver
import usermanager

# set up a logger
logging.Formatter.converter = time.gmtime
log = logging.getLogger('ddmbot')
log.setLevel(logging.INFO)
stderr_logger = logging.StreamHandler()
stderr_logger.setFormatter(logging.Formatter('{asctime} | {levelname:<8} {message}', '%Y-%m-%d %H:%M:%S', style='{'))
log.addHandler(stderr_logger)

if not discord.opus.is_loaded():
    discord.opus.load_opus('opus')

# synchronization primitives to ensure initialization is performed only once
post_init_lock = asyncio.Lock()
post_init_done = False

async def on_ready():
    log.info('Logged in as: {0} (ID: {0.id})'.format(ddmbot.user))

    global post_init_done
    async with post_init_lock:
        if not post_init_done:
            post_init_done = True

            # first off, locate the configured channels
            ddmbot.text_channel = discord.utils.get(ddmbot.get_all_channels(), id=config['general']['text_channel'],
                                                    type=discord.ChannelType.text)
            ddmbot.log_channel = discord.utils.get(ddmbot.get_all_channels(), id=config['general']['log_channel'],
                                                   type=discord.ChannelType.text)
            ddmbot.voice_channel = discord.utils.get(ddmbot.get_all_channels(), id=config['general']['voice_channel'],
                                                     type=discord.ChannelType.voice)

            # check if we got everything
            if ddmbot.text_channel is None:
                raise RuntimeError('Specified text channel could not be found')
            if ddmbot.log_channel is None:
                raise RuntimeError('Specified logging channel could not be found')
            if ddmbot.voice_channel is None:
                raise RuntimeError('Specified voice channel could not be found')

            # check for multiple servers and if we have permissions needed
            text_permissions = ddmbot.text_channel.permissions_for(ddmbot.text_channel.server.me)
            if not text_permissions.send_messages:
                raise RuntimeError('Bot does not have a permission to send messages in the text channel')
            if not text_permissions.read_messages:
                raise RuntimeError('Bot does not have a permission to read messages in the text channel')
            if not text_permissions.manage_messages:
                raise RuntimeError('Bot does not have a permission to manage messages in the text channel')
            if not ddmbot.log_channel.permissions_for(ddmbot.log_channel.server.me).send_messages:
                raise RuntimeError('Bot does not have a permission to send messages in the logging channel')
            voice_permissions = ddmbot.voice_channel.permissions_for(ddmbot.voice_channel.server.me)
            if not voice_permissions.connect:
                raise RuntimeError('Bot does not have a permission to connect to the voice channel')
            if not voice_permissions.speak:
                raise RuntimeError('Bot does not have a permission to speak in the voice channel')
            if len(ddmbot.servers) > 1:
                log.warning('Bot is connected to multiple servers. Users who are not members of a server with the '
                            'text channel used will be ignored.')

            log.info('Initializing user manager')
            users.init(player)
            # populate user manager with existing listeners
            for member in ddmbot.voice_channel.voice_members:
                if not member.voice.self_deaf:
                    await users.add_listener(int(member.id))

            log.info('Initializing command handler')
            command_handler.init()

            log.info('Connecting to the voice channel')
            # obtain VoiceClient and initialize Player
            voice_client = await ddmbot.join_voice_channel(ddmbot.voice_channel)

            log.info('Initializing player')
            player.init(voice_client, stream)

            await ddmbot.send_message(ddmbot.text_channel, 'DdmBot ready')
            log.info('Initialization done')
        else:
            log.warning('on_ready callback called again, initialization skipped')


async def on_message(message):
    # author of the message wrote something, which is kinda a proof (s)he is alive
    await users.refresh_activity(int(message.author.id))
    await ddmbot.process_commands(message)


async def on_error(event, *args, **kwargs):
    raise


async def on_voice_state_update(before, after):
    voice_client = player.voice_client
    if player.voice_client is None:
        return
    if after == ddmbot.user:
        return
    channel = voice_client.channel

    # joining
    if (before.voice.voice_channel != channel or before.voice.self_deaf) and \
            (after.voice.voice_channel == channel and not after.voice.self_deaf):
        await users.add_listener(int(after.id))
    # leaving
    elif (before.voice.voice_channel == channel and not before.voice.self_deaf) and \
            (after.voice.voice_channel != channel or after.voice.self_deaf):
        try:
            await users.remove_listener(int(after.id))
        except ValueError:
            log.warning('Tried to remove non-existing listener in on_voice_update')


if __name__ == '__main__':
    try:
        while True:
            # main loop that can be technically repeated to restart the bot
            # if an exception is raised, loop should be terminated
            # if restart flag is not set, loop should be terminated

            # parse input settings
            config = configparser.ConfigParser(default_section='general')
            config.read('config.ini')

            # add new handler to the logger
            file_logger = TimedRotatingFileHandler(config['general']['log_filename'], when='midnight', backupCount=3,
                                                   utc=True)
            file_logger.setFormatter(logging.Formatter('{asctime} | {name:<20} | {levelname:<8} {message}',
                                                       '%Y-%m-%d %H:%M:%S', style='{'))
            log.addHandler(file_logger)

            # create named pipes (FIFOs)
            with suppress(OSError):
                os.mkfifo(config['player']['aac_pipe'], mode=0o600)
            with suppress(OSError):
                os.mkfifo(config['player']['pcm_pipe'], mode=0o600)

            # create bot instance and register event hooks
            ddmbot = dec.Bot(command_prefix=config['commands']['delimiter'])
            ddmbot.event(on_ready)
            ddmbot.event(on_message)
            ddmbot.event(on_error)
            ddmbot.event(on_voice_state_update)

            # create all the other helpful classes
            stream = streamserver.StreamServer(config['stream_server'], ddmbot.loop)
            users = usermanager.UserManager(config['users'], ddmbot, stream)
            songs = songmanager.SongManager(config['songs'], ddmbot.loop)
            player = pl.Player(config['player'], ddmbot, users, songs)

            command_handler = commands.CommandHandler(config['commands'], ddmbot, users, songs, player)

            try:
                # SongManager can be initialized straight away
                # Other objects are initialized when the bot is connected
                songs.init()
                # StreamServer needs to be started
                ddmbot.loop.run_until_complete(stream.init(users))

                # ddmbot.start command is blocking
                ddmbot.loop.run_until_complete(ddmbot.start(config['general']['token']))
            except Exception as e:
                raise
            finally:
                # save the user blacklist
                command_handler.ignorelist_save()
                # cleanup
                ddmbot.loop.run_until_complete(users.cleanup())
                ddmbot.loop.run_until_complete(player.cleanup())
                ddmbot.loop.run_until_complete(stream.cleanup())
                ddmbot.loop.run_until_complete(ddmbot.logout())  # should be save to call multiple times
                ddmbot.loop.run_until_complete(songs.cleanup())
                # close the loop, this will ensure nothing is scheduled to run anymore
                ddmbot.loop.close()

            # if the bot is not scheduled for a restart, break the loop
            if not command_handler.restart_scheduled:
                break

            # if we are here, we are going to spawn everything again
            # reset initialization flag
            post_init_done = False
            # event loop was closed, if a default one was used it needs to be replaced with a new one
            if asyncio.get_event_loop().is_closed():
                asyncio.set_event_loop(asyncio.new_event_loop())

    except Exception as e:
        log.critical('DdmBot crashed with an exception', exc_info=True)
