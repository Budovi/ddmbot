import asyncio
import configparser
import fcntl
import os
from contextlib import suppress

import discord
import discord.ext.commands as dec

import commands
import player as pl
import songmanager
import streamserver
import usermanager


if not discord.opus.is_loaded():
    discord.opus.load_opus('opus')


post_init_lock = asyncio.Lock()
post_init_done = False

async def on_ready():
    print('Logged in as:\n{0} (ID: {0.id})'.format(ddmbot.user))

    global post_init_done
    async with post_init_lock:
        if not post_init_done:
            post_init_done = True
            print('Initializing the command handler')
            command_handler.init()

            print('Initializing the player')
            # get the Channel to connect to
            voice_channel = discord.utils.get(ddmbot.get_all_channels(), id=config['general']['voice_channel'],
                                              type=discord.ChannelType.voice)
            if voice_channel is None:
                raise RuntimeError('Specified voice channel not found')
            # obtain VoiceClient and initialize Player
            voice_client = await ddmbot.join_voice_channel(voice_channel)
            player.init(voice_client, stream)
            print('Initialization done')

        else:
            print('Warning: on_ready called again')

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
        try: # TODO better initial state handling
            await users.remove_listener(int(after.id))
        except ValueError:
            pass


if __name__ == '__main__':
    try:
        while True:
            # main loop that can be technically repeated to restart the bot
            # if an exception is raised, loop should be terminated
            # if restart flag is not set, loop should be terminated

            # parse input settings
            config = configparser.ConfigParser(default_section='general')
            config.read('config.ini')

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
                # SongManager and UserManager can be initialized straight away
                songs.init()
                users.init()
                # StreamServer needs to be started
                ddmbot.loop.run_until_complete(stream.init(users))
                # Player needs to be initialized after the voice connection is made

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
        pass # TODO: logger
        raise
