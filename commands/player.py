import discord.ext.commands as dec

from commands.common import *


class Player:
    """Player controls"""
    def __init__(self, bot):
        self._bot = bot

    _help_messages = {
        'group': 'Player controls (switching modes, status, title, volume)',

        'djmode': '* Switches the player to the DJ mode\n\n'
        'In the DJ mode, users can join a DJ queue and play music from their playlists. Automatic playlist is used '
        'when no DJs are present and someone is listening. Listeners can vote to skip songs played.',

        'status': 'Re-prints the status message\n\n'
        'Re-prints the status message if it has been pushed up by other messages.',

        'stop': '* Stops the player',

        'stream': '* Switches the player to the streaming mode\n\n'
        'After issuing this command, bot plays the specified stream. Such stream has no record in database, it is not '
        'included in the automatic playlist and cannot be skipped. Stream title is either extracted from the stream '
        'or can be provided as an optional argument.\nNo further checks are done for the stream length, overplaying '
        'nor the blacklist. To stop the stream an operator can use \'player stop\' command or transition into '
        '\'djmode\' directly. After the stream ends (or fails), player will transition into stopped state.',

        'title': '* Sets a stream title to the value specified\n\n'
        'This command can be used to set a different title for the currently played stream. Status message is '
        're-printed upon completion. Can be only used in the stream mode, use \'song rename\' feature to change titles '
        'of the songs in the database.',

        'volume': '* Queries or sets a volume for the discord voice channel\n\n'
        'Only discord voice channel is affected. Valid values are between 0 and 200, where 100 is the default value.'
    }

    @dec.group(invoke_without_command=True, help=_help_messages['group'])
    async def player(self, subcommand: str, *arguments: str):
        raise dec.UserInputError('Command *player* has no subcommand named {}. Please use `{}help player` to list '
                                 'all the available subcommands.'
                                 .format(subcommand, self._bot.config['ddmbot']['delimiter']))

    @privileged
    @player.command(ignore_extra=False, help=_help_messages['djmode'])
    async def djmode(self):
        await self._bot.player.set_djmode()

    @player.command(ignore_extra=False, aliases=['s'], help=_help_messages['status'])
    async def status(self):
        await self._bot.player.reprint_status()

    @privileged
    @player.command(ignore_extra=False, help=_help_messages['stop'])
    async def stop(self):
        await self._bot.player.set_stop()

    @privileged
    @player.command(ignore_extra=False, help=_help_messages['stream'])
    async def stream(self, url: str, name: str = None):
        await self._bot.player.set_stream(url, name)

    @privileged
    @player.command(ignore_extra=False, aliases=['t'], help=_help_messages['title'])
    async def title(self, name: str = None):
        await self._bot.player.set_stream_title(name)

    @privileged
    @player.command(ignore_extra=False, help=_help_messages['volume'])
    async def volume(self, volume: int = None):
        if volume is not None:
            self._bot.player.volume = volume / 100
            await self._bot.message('Player volume set to {}%'.format(int(self._bot.player.volume * 100)))
        else:
            await self._bot.message('Player volume: {}%'.format(int(self._bot.player.volume * 100)))
