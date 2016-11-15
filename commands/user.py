import discord.ext.commands as dec
import discord

import database.user
from commands.common import *


class User:
    """User blacklisting and querying information"""
    def __init__(self, bot):
        self._bot = bot
        self._db = database.user.UserInterface(bot.loop)

    _help_messages = {
        'group': 'User blacklisting and information',

        'grace': '* Removes the specified user from ignore list\n\n'
        'User may be specified by it\'s username, nick or mention.',

        'ignore': '* Puts the specified user to the ignore list\n\n'
        'User may be specified by it\'s username, nick or mention. Bot will not react in any way to the ignored users. '
        'This action won\'t remove the user from the DJ queue nor listeners.\nNote that there is currently no way of '
        'disconnecting the user from the direct stream except restarting the bot.',

        'info': 'Displays information about the user stored in the database\n\n'
        'User may be specified by it\'s username, nick or mention.',

        'kick': '* Kicks the specified user from the DJ queue\n\n'
        'User may be specified by it\'s username, nick or mention.\nNote that there is currently no way of '
        'of disconnecting the user from the direct stream except restarting the bot.'
    }

    @dec.group(invoke_without_command=True, aliases=['u'], help=_help_messages['group'])
    async def user(self, *stray: str):
        if stray:
            await self._bot.whisper('Command *user* has no subcommand named {}. Please use `{}help user` to list all '
                                    'the available subcommands.'
                                    .format(stray[0], self._bot.config['ddmbot']['delimiter']))
        else:
            await self._bot.whisper('You need to provide a subcommand to the *user* command. Please use `{}help user` '
                                    'to list all the available subcommands.'
                                    .format(self._bot.config['ddmbot']['delimiter']))

    @privileged
    @user.command(ignore_extra=False, help=_help_messages['grace'])
    async def grace(self, user: discord.Member):
        await self._db.grace(int(user.id))
        await self._bot.message('User {} successfully removed from the ignore list'.format(user))

    @privileged
    @user.command(ignore_extra=False, help=_help_messages['ignore'])
    async def ignore(self, user: discord.User):
        if self._bot.is_operator(user):
            raise dec.UserInputError('User {} is an operator and cannot be ignored'.format(user))

        await self._db.ignore(int(user.id))
        await self._bot.message('User {} has been added to the ignore list'.format(user))

    @user.command(pass_context=True, ignore_extra=False, aliases=['i'], help=_help_messages['info'])
    async def info(self, ctx, user: discord.User = None):
        info = await self._db.info(int(user.id if user is not None else ctx.message.author.id))

        reply = 'Statistics for the user {}:\n  {} song(s) in {} playlist(s)\n  Played {} time(s) from the DJ queue\n' \
                '  Listened to {} song(s)' \
                .format(user, info['song_count'], info['playlist_count'], info['play_count'], info['listen_count'])
        if info['ignored']:
            reply += "\n\nUser is ignored by the bot."

        await self._bot.whisper(reply)

    @privileged
    @user.command(ignore_extra=False, help=_help_messages['kick'])
    async def kick(self, user: discord.Member):
        await self._bot.users.leave_queue(int(user.id))
        await self._bot.message('User {} was removed from the DJ queue'.format(user))
