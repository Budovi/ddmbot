import discord.ext.commands as dec
import discord

from commands.bot import Bot
from commands.common import log
from commands.others import Others
from commands.playlist import Playlist
from commands.song import Song
from commands.user import User


#
# Main command wrapper class for DdmBot
#
class CommandHandler:
    def __init__(self, bot):
        self._bot = bot

        self._bot_cog = Bot(bot)
        self._others_cog = Others(bot)
        self._playlist_cog = Playlist(bot)
        self._song_cog = Song(bot)
        self._user_cog = User(bot)

        bot.client.add_check(self._command_check)
        bot.client.add_listener(self._on_command_error, 'on_command_error')
        bot.client.add_listener(self._on_command_completion, 'on_command_completion')

        bot.client.add_cog(self._bot_cog)
        bot.client.add_cog(self._others_cog)
        bot.client.add_cog(self._playlist_cog)
        bot.client.add_cog(self._song_cog)
        bot.client.add_cog(self._user_cog)

    #
    # Command check function
    #
    def _command_check(self, ctx):
        # if the channel is not private, delete the command immediately regardless of the response
        if not isinstance(ctx.message.channel, discord.PrivateChannel):
            self._bot.loop.create_task(self._bot.client.delete_message(ctx.message))

        # if privileged, check the member role
        if hasattr(ctx.command, 'privileged'):
            if not self._bot.is_operator(ctx.message.author):
                raise dec.CommandError('You don\'t have a permission to use the *{}* command'.format(ctx.command))

        return True

    #
    # Listeners
    #
    async def _on_command_error(self, exception, ctx):
        # non-existing commands won't trigger check thus are not deleted
        if isinstance(exception, dec.CommandNotFound) and not isinstance(ctx.message.channel, discord.PrivateChannel):
            await self._bot.client.delete_message(ctx.message)

        # get a cause if the exception was thrown inside the command routine
        if isinstance(exception, dec.CommandInvokeError):
            exception = exception.__cause__

        # now inform the author of the command on the failure using PMs
        await self._bot.client.send_message(ctx.message.author, str(exception))

        # log the error for debugging purposes
        log.debug('Command \'{}\' invoked by {} raised an exception\n{}'
                  .format(ctx.command, ctx.message.author, ctx.message.content), exc_info=exception)

    async def _on_command_completion(self, command, ctx):
        # figure out the argument start, it is <self> <context> <args>...
        arg_start = 2 if ctx.command.pass_context else 1

        # log the usage of all privileged commands
        if hasattr(ctx.command, 'privileged'):
            log.info('Operator {} has used the \'{}\' command with the following arguments: [{}]\n{}'
                     .format(ctx.message.author, ctx.command, ', '.join([str(arg) for arg in ctx.args[arg_start:]]),
                             ctx.message.content))
            await self._bot.log('Operator {} has used the *{}* command with the following arguments: [{}]\n`{}`'
                                .format(ctx.message.author, ctx.command,
                                        ', '.join([str(arg) for arg in ctx.args[arg_start:]]), ctx.message.content))
        # otherwise just when debugging
        else:
            log.debug('User {} has used the \'{}\' command with the following arguments: [{}]\n{}'
                      .format(ctx.message.author, ctx.command, ', '.join([str(arg) for arg in ctx.args[arg_start:]]),
                              ctx.message.content))
