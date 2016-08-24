import asyncio
from datetime import datetime

import discord.ext.commands as dec
import discord


#
# Decorator that adds privileged attribute to the command object
#
def privileged(priv):
    def append_attr_priv(command):
        command.privileged = priv
        return command
    return append_attr_priv


#
# Custom CommandError subclasses
#
class NotAuthorized(dec.CommandError):
    """Used when non-operator tries to invoke privileged command"""
    pass


class InvalidChannel(dec.CommandError):
    """Used when a privileged command is issued outside the command channel"""
    pass


#
# Main COG class for DdmBot
#
class CommandHandler:
    def __init__(self, config, bot, users, songs, player):
        self._config = config
        self._bot = bot
        self._users = users
        self._songs = songs
        self._player = player

        self._ignorelist = set()
        self.ignorelist_load()
        self._restart_scheduled = False

        self._cmd_channel = None
        self._log_channel = None
        self._operator_role = None

    @property
    def restart_scheduled(self):
        return self._restart_scheduled

    #
    # Initialization
    #
    def init(self):
        self._cmd_channel = discord.utils.get(self._bot.get_all_channels(), id=self._config['cmd_channel'],
                                              type=discord.ChannelType.text)
        if self._cmd_channel is None:
            raise RuntimeError('Command text channel specified was not found')
        self._log_channel = discord.utils.get(self._bot.get_all_channels(), id=self._config['log_channel'],
                                              type=discord.ChannelType.text)
        if self._log_channel is None:
            raise RuntimeError('Logging text channel specified was not found')

        def get_all_roles():
            for server in self._bot.servers:
                for role in server.roles:
                    yield role

        self._operator_role = discord.utils.get(get_all_roles(), id=self._config['operator_role'])
        if self._operator_role is None:
            raise RuntimeError('Operator role specified was not found')

        self._bot.add_listener(self._command_error, 'on_command_error')
        self._bot.add_listener(self._command_completion, 'on_command_completion')
        self._bot.add_cog(self)

    #
    # Common checks
    #
    def __check(self, ctx):
        # check ignore list and membership
        if ctx.message.author.id in self._ignorelist:
            return False
        if ctx.message.author not in self._cmd_channel.server.members:
            return False

        # if the channel is not private, delete the command immediately regardless of the response
        if not isinstance(ctx.message.channel, discord.PrivateChannel):
            self._bot.loop.create_task(self._bot.delete_message(ctx.message))

        # if privileged, check the member
        # attribute privileged is mandatory and needs to be added to every command in this class
        if not hasattr(ctx.command, 'privileged'):
            return True  # This is a command outside of our control
        if ctx.command.privileged:
            if ctx.message.channel != self._cmd_channel:
                raise InvalidChannel('Privileged commands must be used inside the {} channel'
                                     .format(self._cmd_channel.mention))
            if self._operator_role not in ctx.message.author.roles:
                raise NotAuthorized('You don\'t have a permission to use the *{}* command.'
                                    .format(ctx.command))

        return True

    #
    # Listeners
    #
    async def _command_error(self, exception, ctx):
        # we are interested in the following errors:
        # NotAuthorized, InvalidChannel, UserInputError
        if isinstance(exception, (NotAuthorized, InvalidChannel)):
            await self._bot.send_message(ctx.message.author, str(exception))
            return

        if not isinstance(exception, dec.CheckFailure):
            if hasattr(ctx.command, 'privileged') and ctx.command.privileged:
                await self._bot.send_message(self._cmd_channel, '{}, {}'.format(ctx.message.author.mention,
                                                                                str(exception)))
            else:
                await self._bot.send_message(ctx.message.author, str(exception))

    async def _command_completion(self, command, ctx):
        if hasattr(ctx.command, 'privileged') and command.privileged:
            await self._log('Operator {} has used the *{}* command with the following arguments:\n{}'
                            .format(ctx.message.author, command, ctx.kwargs))

    #
    # General bot commands
    #
    @privileged(True)
    @dec.command(ignore_extra=False)
    async def restart(self):
        self._restart_scheduled = True
        self._bot.loop.create_task(self._shutdown())

    @privileged(True)
    @dec.command(ignore_extra=False)
    async def shutdown(self):
        self._bot.loop.create_task(self._shutdown())

    @privileged(True)
    @dec.command(ignore_extra=False)
    async def ignore(self, member: discord.Member):
        if self._operator_role in member.roles:
            await self._bot.reply('User {} is an operator and cannot be ignored'.format(member.display_name))
            return

        self._ignorelist.add(member.id)
        await self._bot.reply('User {} has been added to the ignore list'.format(member.display_name))

    @privileged(True)
    @dec.command(ignore_extra=False)
    async def unignore(self, member: discord.Member):
        if member.id not in self._ignorelist:
            await self._bot.reply('User {} is not on the ignore list'.format(member.display_name))
            return

        self._ignorelist.remove(member.id)
        await self._bot.reply('User {} successfully removed from the ignore list'.format(member.display_name))

    #
    # Player controls
    #
    @privileged(True)
    @dec.command(ignore_extra=False)
    async def stop(self):
        await self._player.set_stop()

    @privileged(True)
    @dec.command(ignore_extra=False)
    async def djmode(self):
        await self._player.set_djmode()

    @privileged(True)
    @dec.command(ignore_extra=False)
    async def stream(self, url: str, name=None):
        await self._player.set_stream(url, name)

    #
    # User controls
    #
    @privileged(False)
    @dec.command(pass_context=True, ignore_extra=False)
    async def join(self, ctx):
        if self._player.streaming or self._player.stopped:
            raise dec.UserInputError('Player is not in the DJ mode')
        try:
            await self._users.join_queue(int(ctx.message.author.id))
        except ValueError:
            await self._bot.whisper('You have to be listening to join the DJ queue')

    @privileged(False)
    @dec.command(pass_context=True, ignore_extra=False)
    async def leave(self, ctx):
        try:
            await self._users.leave_queue(int(ctx.message.author.id))
        except ValueError:
            await self._bot.whisper('You are not in the DJ queue')

    @privileged(True)
    @dec.command(ignore_extra=False)
    async def kick(self, member: discord.Member):
        try:
            await self._users.leave_queue(int(member.id))
            await self._bot.reply('User {} was removed from the DJ queue'.format(member.display_name))
        except ValueError:
            raise dec.UserInputError('User {} is not in the DJ queue'.format(member.display_name))

    #
    # Song controls
    #
    @privileged(False)
    @dec.command(pass_context=True, ignore_extra=False)
    async def hype(self, ctx):
        if not self._users.is_listening(int(ctx.message.author.id)):
            raise dec.BadArgument('You must be listening to vote')
        await self._player.hype(int(ctx.message.author.id))

    @privileged(False)
    @dec.command(pass_context=True, ignore_extra=False)
    async def skip(self, ctx):
        if not self._users.is_listening(int(ctx.message.author.id)):
            raise dec.BadArgument('You must be listening to vote')
        await self._player.skip(int(ctx.message.author.id))
        await self._log('User {} has voted to skip'.format(ctx.message.author.display_name))

    @privileged(False)
    @dec.command(pass_context=True, ignore_extra=False)
    async def hqsound(self, ctx):
        url = await self._users.generate_url(int(ctx.message.author.id))
        await self._bot.whisper('Direct link: {}\nPlease note that this link will expire in a few minutes. Also, you '
                                'can only be connected from a single location, including a discord voice channel. If '
                                'you are connected already, please disconnect before following the link.'.format(url))

    @privileged(True)
    @dec.command(pass_context=True, ignore_extra=False)
    async def forceskip(self, ctx):
        if await self._player.force_skip():
            await self._bot.say('Skip forced by {}'.format(ctx.message.author.mention))

    #
    # Playlist management
    #
    @privileged(False)
    @dec.command(pass_context=True, ignore_extra=False)
    async def append(self, ctx, *uris: str):
        inserted, over, error_list = await self._songs.append_to_playlist(int(ctx.message.author.id), uris)
        reply = '{} song(s) appended\n{} song(s) over limit\n{} insertions failed'\
            .format(inserted, over, len(error_list))
        if len(error_list) > 0:
            reply = reply + '\n\nSome of the errors follow:\n > ' + '\n > '.join(error_list[:10])
        await self._bot.whisper(reply)

    @privileged(False)
    @dec.command(pass_context=True, ignore_extra=False)
    async def clear(self, ctx):
        await self._songs.clear_playlist(int(ctx.message.author.id))
        await self._bot.whisper('Your playlist was cleared')

    @privileged(False)
    @dec.command(pass_context=True, ignore_extra=False)
    async def shuffle(self, ctx):
        await self._songs.shuffle_playlist(int(ctx.message.author.id))
        await self._bot.whisper('Your playlist was shuffled')

    @privileged(False)
    @dec.command(pass_context=True, ignore_extra=False)
    async def list(self, ctx):
        items = await self._songs.list_playlist(int(ctx.message.author.id))
        if len(items) == 0:
            await self._bot.whisper('Your playlist is empty')
            return
        reply = 'First 20 songs from your playlist:\n > ' + '\n > '.join(['[{}] {}'.format(*item) for item in items])
        await self._bot.whisper(reply)

    #
    # Song management
    #
    @privileged(True)
    @dec.command(pass_context=True, ignore_extra=False)
    async def blacklist(self, ctx, song_id: int):
        try:
            await self._songs.blacklist_song(song_id)
        except ValueError as e:
            raise dec.UserInputError(str(e))

    @privileged(True)
    @dec.command(pass_context=True, ignore_extra=False)
    async def deduplicate(self, ctx):
        pass

    @privileged(True)
    @dec.command(pass_context=True, ignore_extra=False)
    async def split(self, ctx):
        pass

    @privileged(True)
    @dec.command(pass_context=True, ignore_extra=False)
    async def rename(self, ctx):
        pass

    @privileged(False)
    @dec.command(pass_context=True, ignore_extra=False)
    async def dbsearch(self, ctx):
        pass

    #
    # Ignore list load and save
    #
    def ignorelist_load(self):
        with open(self._config['ignorelist_file'], 'r') as file:
            for line in file:
                if line.lstrip().startswith(';'):
                    continue
                # leave the id in the string format for now for a quicker comparison
                self._ignorelist.add(line.split()[0])

    def ignorelist_save(self):
        with open(self._config['ignorelist_file'], 'w') as file:
            file.write(
                '; DdmBot user ignore list file\n'
                '; This file contains blacklisted users. You can modify this file offline, use commands to edit the blacklist with the bot directly.\n'
                '; One user ID per line, lines starting with a semicolon are ignored. Text following user IDs is ignored too. File is automatically\n'
                '; generated and overwritten on bot shutdown. You have been warned.\n'
            )
            for user_id in self._ignorelist:
                member = discord.utils.get(self._bot.get_all_members(), id=user_id)
                if member is None:
                    file.write('{} <unknown username>\n'.format(user_id))
                else:
                    file.write('{} {}\n'.format(user_id, member.display_name))

    #
    # Helper methods
    #
    async def _log(self, text):
        await self._bot.send_message(self._log_channel, '{} | {}'.format(str(datetime.utcnow()), text))

    async def _shutdown(self):
        await asyncio.sleep(2)
        await self._bot.logout()