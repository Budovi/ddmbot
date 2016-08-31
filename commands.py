import asyncio
import logging

import discord.ext.commands as dec
import discord

# set up the logger
log = logging.getLogger('ddmbot.commands')


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

        self._operator_role = None

    @property
    def restart_scheduled(self):
        return self._restart_scheduled

    #
    # Initialization
    #
    def init(self):
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
        if ctx.message.author not in self._bot.text_channel.server.members:
            return False

        # if the channel is not private, delete the command immediately regardless of the response
        if not isinstance(ctx.message.channel, discord.PrivateChannel):
            self._bot.loop.create_task(self._bot.delete_message(ctx.message))

        # if privileged, check the member
        # attribute privileged is mandatory and needs to be added to every command in this class
        if not hasattr(ctx.command, 'privileged'):
            return True  # This is a command outside of our control
        if ctx.command.privileged:
            if ctx.message.channel != self._bot.text_channel:
                raise InvalidChannel('Privileged commands must be used inside the {} channel'
                                     .format(self._bot.text_channel.mention))
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

        if isinstance(exception, dec.CheckFailure):
            return

        if isinstance(exception, dec.CommandNotFound) and not isinstance(ctx.message.channel, discord.PrivateChannel):
            # non-existing commands won't trigger __check thus are not deleted
            await self._bot.delete_message(ctx.message)

        if isinstance(exception, dec.CommandError):
            if isinstance(exception, dec.CommandInvokeError):
                exception = exception.__cause__
            if hasattr(ctx.command, 'privileged') and ctx.command.privileged:
                await self._message('{}, {}'.format(ctx.message.author.mention, str(exception)))
            else:
                await self._bot.send_message(ctx.message.author, str(exception))

    async def _command_completion(self, command, ctx):
        arg_start = 2 if command.pass_context else 1
        if hasattr(ctx.command, 'privileged') and command.privileged:
            log.info('Operator {} has used the {} command with the following arguments: [{}]\n{}'
                     .format(ctx.message.author, command, ', '.join([str(arg) for arg in ctx.args[arg_start:]]),
                             ctx.message.content))
            await self._log('Operator {} has used the *{}* command with the following arguments: [{}]\n`{}`'
                            .format(ctx.message.author, command, ', '.join([str(arg) for arg in ctx.args[arg_start:]]),
                                    ctx.message.content))
        else:
            log.debug('User {} has used the {} command with the following arguments: [{}]\n{}'
                      .format(ctx.message.author, command, ', '.join([str(arg) for arg in ctx.args[arg_start:]]),
                              ctx.message.content))

    #
    # General bot commands
    #
    _restart_help = '*Operators only* Restarts the bot\n\nThe configuration is reloaded from the configuration file.'

    @privileged(True)
    @dec.command(ignore_extra=False, help=_restart_help)
    async def restart(self):
        await self._message('Restarting...')
        self._restart_scheduled = True
        self._bot.loop.create_task(self._shutdown())

    _shutdown_help = '*Operators only* Shuts down the bot\n\nYou need an access to the server bot runs on to launch ' \
                     'it again.'

    @privileged(True)
    @dec.command(ignore_extra=False, help=_shutdown_help)
    async def shutdown(self):
        await self._message('Shutting down...')
        self._bot.loop.create_task(self._shutdown())

    _ignore_help = '*Operators only* Adds the user specified into the list of ignored users\n\nThe user may be ' \
                   'specified by it\'s username, nick or mention.'

    @privileged(True)
    @dec.command(ignore_extra=False, help=_ignore_help)
    async def ignore(self, member: discord.Member):
        if self._operator_role in member.roles:
            raise dec.UserInputError('User {} is an operator and cannot be ignored'.format(member))

        self._ignorelist.add(member.id)
        await self._message('User {} has been added to the ignore list'.format(member))

    _unignore_help = '*Operators only* Removes the user specified from the list of ignored users\n\nThe user may be ' \
                     'specified by it\'s username, nick or mention.'

    @privileged(True)
    @dec.command(ignore_extra=False, help=_unignore_help)
    async def unignore(self, member: discord.Member):
        if member.id not in self._ignorelist:
            raise dec.UserInputError('User {} is not on the ignore list'.format(member))

        self._ignorelist.remove(member.id)
        await self._message('User {} successfully removed from the ignore list'.format(member))

    #
    # Player controls
    #
    _stop_help = '*Operators only* Changes the player\'s mode to STOPPED'

    @privileged(True)
    @dec.command(ignore_extra=False, help=_stop_help)
    async def stop(self):
        await self._player.set_stop()

    _djmode_help = '*Operators only* Changes the player\'s mode to DJ MODE\n\nIn this mode, users can join the DJ ' \
                   'queue and take turns in playing songs from their playlists.'

    @privileged(True)
    @dec.command(ignore_extra=False, help=_djmode_help)
    async def djmode(self):
        await self._message('Player switched to DJ mode')
        await self._player.set_djmode()

    _stream_help = '*Operators only* Changes the player\'s state to STREAMING\n\nStream specified by the URL is ' \
                   'played. If the playback fails or the stream ends, bot will change it\'s state to STOPPED.\n' \
                   'No blacklisting, overplay protection or length limits are applied when using this command.'

    @privileged(True)
    @dec.command(ignore_extra=False, help=_stream_help)
    async def stream(self, url: str, name=None):
        await self._player.set_stream(url, name)

    _volume_help = '*Operators only* Queries or changes the volume of the bot in the voice channel\n\nDirect ' \
                   'stream is unaffected by this command. Valid values are between 0 and 200 [%].'

    @privileged(True)
    @dec.command(ignore_extra=False, help=_volume_help)
    async def volume(self, volume: int=None):
        if volume is not None:
            self._player.volume = volume / 100
            await self._message('Player volume set to {}%'.format(int(self._player.volume * 100)))
        else:
            await self._message('Player volume: {}%'.format(int(self._player.volume * 100)))

    #
    # User controls
    #
    _join_help = 'Adds the user to the DJ queue\n\nYou must be listening to do this. When you stop listening, you ' \
                 'will be removed from the queue automatically.'

    @privileged(False)
    @dec.command(pass_context=True, ignore_extra=False, help=_join_help)
    async def join(self, ctx):
        if self._player.streaming or self._player.stopped:
            raise dec.UserInputError('Player is not in the DJ mode')
        try:
            await self._users.join_queue(int(ctx.message.author.id))
        except ValueError:
            await self._bot.whisper('You have to be listening to join the DJ queue')

    _leave_help = 'Removes the user from the DJ queue'

    @privileged(False)
    @dec.command(pass_context=True, ignore_extra=False, help=_leave_help)
    async def leave(self, ctx):
        try:
            await self._users.leave_queue(int(ctx.message.author.id))
        except ValueError:
            await self._bot.whisper('You are not in the DJ queue')

    _kick_help = '*Operators only* Kicks the specified user from the DJ queue\n\nThe user may be specified by it\'s ' \
                 'username, nick or mention.'

    @privileged(True)
    @dec.command(ignore_extra=False, help=_kick_help)
    async def kick(self, member: discord.Member):
        try:
            await self._users.leave_queue(int(member.id))
            await self._message('User {} was removed from the DJ queue'.format(member))
        except ValueError:
            raise dec.UserInputError('User {} is not in the DJ queue'.format(member))

    #
    # Song controls
    #
    _hype_help = 'Hypes the currently playing song\n\nYou must be listening to vote. This command does not produce ' \
                 'any message, it updates the song status message instead. You can vote only once. You cannot hype ' \
                 'a song that was queued by you.'

    @privileged(False)
    @dec.command(pass_context=True, ignore_extra=False, help=_hype_help)
    async def hype(self, ctx):
        if not self._users.is_listening(int(ctx.message.author.id)):
            raise dec.UserInputError('You must be listening to vote')
        await self._player.hype(int(ctx.message.author.id))

    _skip_help = 'Votes to skip the song\n\nYou must be listening to vote. This command does not produce any ' \
                 'message, it updates the song status message instead. Although skip votes are more private, they ' \
                 'are logged and can be seen by the operators. When you vote to skip a song queued by you it is ' \
                 'skipped instantly.'

    @privileged(False)
    @dec.command(pass_context=True, ignore_extra=False, help=_skip_help)
    async def skip(self, ctx):
        if not self._users.is_listening(int(ctx.message.author.id)):
            raise dec.UserInputError('You must be listening to vote')
        await self._player.skip(int(ctx.message.author.id))
        await self._log('User {} has voted to skip'.format(ctx.message.author))

    _direct_help = 'Requests a link to the direct audio stream\n\nInstructions are sent along with the link.'

    @privileged(False)
    @dec.command(pass_context=True, ignore_extra=False, help=_direct_help)
    async def direct(self, ctx):
        url = await self._users.generate_url(int(ctx.message.author.id))
        await self._bot.whisper('Direct stream: {}\nPlease note that this link will expire in a few minutes. Also, you '
                                'can only be connected from a single location, including a discord voice channel. If '
                                'you are connected already, please disconnect before opening the playlist.'.format(url))

    _forceskip_help = '*Operators only* Skips the song currently playing or terminates the stream\n\nPlease note ' \
                      'that the song *won\'t* be blacklisted automatically.'

    @privileged(True)
    @dec.command(pass_context=True, ignore_extra=False, help=_forceskip_help)
    async def forceskip(self, ctx):
        if await self._player.force_skip():
            await self._message('Skip forced by {}'.format(ctx.message.author.mention))

    #
    # Playlist management
    #
    _append_help = 'Inserts the specified songs to your playlist\n\nYoutube, Soundcloud and Bandcamp songs and ' \
                   'playlists  are supported. You can specify multiple URLs or song IDs in the request. Songs are ' \
                   'inserted to *the end* of your playlist.'

    @privileged(False)
    @dec.command(pass_context=True, ignore_extra=False, help=_append_help)
    async def append(self, ctx, *uris: str):
        # print the disclaimer
        await self._bot.whisper('Please note that inserting new songs can take a while. Be patient and wait for the '
                                'result. You can run other commands, but avoid manipulating your playlist. Trying to '
                                'modify your playlist multiple times at once may yield unexpected results and is more '
                                'likely to fail.')
        # now do the operation
        inserted, truncated, error_list = await self._songs.append_to_playlist(int(ctx.message.author.id), uris)
        reply = '**{} song(s) appended**\n**{} insertions failed**'.format(inserted, len(error_list))
        if truncated:
            reply += '\n__**Part of the input was omitted due to playlist length restrictions.**__'
        if len(error_list) > 0:
            reply += '\n\nSome of the errors follow:\n > ' + '\n > '.join(error_list[:10])
        await self._bot.whisper(reply)

    _prepend_help = 'Inserts the specified songs into your playlist\n\nYoutube, Soundcloud and Bandcamp songs and ' \
                    'playlists are supported. You can specify multiple URLs or song IDs in the request. Songs are ' \
                    'inserted to *the beginning* of your playlist.'

    @privileged(False)
    @dec.command(pass_context=True, ignore_extra=False, help=_prepend_help)
    async def prepend(self, ctx, *uris: str):
        # print the disclaimer
        await self._bot.whisper('Please note that inserting new songs can take a while. Be patient and wait for the '
                                'result. You can run other commands, but avoid manipulating your playlist. Trying to '
                                'modify your playlist multiple times at once may yield unexpected results and is more '
                                'likely to fail.')
        # now do the operation
        inserted, truncated, error_list = await self._songs.prepend_to_playlist(int(ctx.message.author.id), uris)
        reply = '**{} song(s) prepended**\n**{} insertions failed**'.format(inserted, len(error_list))
        if truncated:
            reply += '\n__**Part of the input was omitted due to playlist length restrictions.**__'
        if len(error_list) > 0:
            reply += '\n\nSome of the errors follow:\n > ' + '\n > '.join(error_list[:10])
        await self._bot.whisper(reply)

    _pop_help = 'Removes the specified number of songs from your playlist\n\nIf no number is specified, one song is ' \
                'removed. Songs are removed from *the beginning* of your playlist.'

    @privileged(False)
    @dec.command(pass_context=True, ignore_extra=False, help=_pop_help)
    async def pop(self, ctx, count: int=1):
        real_count = await self._songs.pop_from_playlist(int(ctx.message.author.id), count)

        reply = '**{} song(s) removed from your playlist**'.format(real_count)
        if real_count < count:
            reply += '\n{} song(s) could not be removed because your playlist is empty.'.format(count - real_count)
        await self._bot.whisper(reply)

    _push_help = 'Search and prepend a single song\n\nSearches Youtube for the video with given keywords. The first ' \
                 'returned result is inserted to *the beginning* of your playlist.'

    @privileged(False)
    @dec.command(pass_context=True, ignore_extra=False, help=_push_help)
    async def push(self, ctx, *keywords: str):
        if not len(keywords):
            raise dec.UserInputError('You must specify at least one keyword to search')

        song_id, song_title = await self._songs.push_to_playlist(int(ctx.message.author.id), keywords)
        await self._bot.whisper('**Song** [{}] {} **was added to your playlist. If this is not correct, you can remove'
                                ' it by using the *pop* command.**'.format(song_id, song_title))

    _clear_help = 'Clears your playlist\n\nAll songs will be removed from your playlist.'

    @privileged(False)
    @dec.command(pass_context=True, ignore_extra=False, help=_clear_help)
    async def clear(self, ctx):
        await self._songs.clear_playlist(int(ctx.message.author.id))
        await self._bot.whisper('Your playlist was cleared')

    _shuffle_help = 'Shuffles your playlist\n\nShuffles all the songs in your playlist in a random manner.'

    @privileged(False)
    @dec.command(pass_context=True, ignore_extra=False, help=_shuffle_help)
    async def shuffle(self, ctx):
        await self._songs.shuffle_playlist(int(ctx.message.author.id))
        await self._bot.whisper('Your playlist was shuffled')

    _list_help = 'Lists the songs in your playlist\n\nDue to message length restrictions, up to 20 songs are ' \
                 'returned for a single request. By default, the songs at the beginning of your playlist are ' \
                 'returned. You can list the rest of the playlist by specifying the start with an argument (' \
                 'e.g. 17 will list songs starting from 17th to 36th).'

    @privileged(False)
    @dec.command(pass_context=True, ignore_extra=False, help=_list_help)
    async def list(self, ctx, start: int = 1):
        ordinal = lambda n: "%d%s" % (n, "tsnrhtdd"[(n // 10 % 10 != 1) * (n % 10 < 4) * n % 10::4])

        items = await self._songs.list_playlist(int(ctx.message.author.id), start - 1, 20)
        if len(items) == 0:
            if start == 1:
                await self._bot.whisper('Your playlist is empty')
            else:
                await self._bot.whisper('You don\'t have any songs in your playlist starting from the {}'
                                        .format(ordinal(start)))
            return
        reply = '**20 songs from your playlist, starting from the {}:**\n **>** '.format(ordinal(start)) + \
                '\n **>** '.join(['[{}] {}'.format(*item) for item in items])
        await self._bot.whisper(reply)

    #
    # Song management
    #
    _blacklist_help = '*Operators only* Puts a song specified by it\'s ID to the blacklist\n\nSong ID can be ' \
                      'located before the song name in the square brackets. It is included in the status message ' \
                      'and all the listings.\nThis does not prevent inserting the song to the user\'s playlist, ' \
                      'song is skipped when it is about to be played.'

    @privileged(True)
    @dec.command(ignore_extra=False, help=_blacklist_help)
    async def blacklist(self, which: int):
        try:
            await self._songs.add_to_blacklist(which)
        except ValueError as e:
            raise dec.UserInputError(str(e))
        await self._message('Song [{}] has been blacklisted'.format(which))

    _unblacklist_help = '*Operators only* Removes a song specified by it\'s ID from the blacklist\n\nSong ID can be ' \
                        'located before the song name in the square brackets. It is included in the status message ' \
                        'and all the listings.'

    @dec.command(ignore_extra=False, help=_unblacklist_help)
    async def unblacklist(self, which: int):
        try:
            await self._songs.remove_from_blacklist(which)
        except ValueError as e:
            raise dec.UserInputError(str(e))
        await self._message('Song [{}] has been removed from blacklist'.format(which))

    _deduplicate_help = '*Operators only* Marks a song to be a duplicate of another song\n\nThis is a destructive ' \
                        'operation. The duplicate is replaced by it\'s "original" just before playing. Tests for ' \
                        'the blacklist, length and overplay protection are performed on the "original" song.\n' \
                        'Song IDs can be located before the song name in the square brackets. You can use the ' \
                        ' *search* command to find out the IDs.'

    @privileged(True)
    @dec.command(ignore_extra=False, help=_deduplicate_help)
    async def deduplicate(self, which: int, target: int):
        try:
            await self._songs.merge_songs(which, target)
        except ValueError as e:
            raise dec.UserInputError(str(e))
        await self._message('Song [{}] has been marked as a duplicate of the song [{}]'.format(which, target))

    _split_help = '*Operators only* Marks a given song as an original\n\nThis command can be used to revert some ' \
                  'of the changes done by the *deduplicate* command.'

    @privileged(True)
    @dec.command(ignore_extra=False, help=_split_help)
    async def split(self, which: int):
        try:
            await self._songs.split_song(which)
        except ValueError as e:
            raise dec.UserInputError(str(e))
        await self._message('Song [{}] has been marked as unique'.format(which))

    _rename_help = '*Operators only* Changes the title of a specified song\n\nThis command does not update the ' \
                   'status message. Please note that you should put the name in the quotes if it contains spaces. ' \
                   'New name will be used next time the song is played.\nSong ID can be located before the song ' \
                   'name in the square brackets. It is included in the status message and all the listings.'

    @privileged(True)
    @dec.command(ignore_extra=False, help=_rename_help)
    async def rename(self, which: int, new_title: str):
        try:
            await self._songs.rename_song(which, new_title)
        except ValueError as e:
            raise dec.UserInputError(str(e))
        await self._message('Song [{}] has been renamed to "{}"'.format(which, new_title))

    _search_help = 'Searches the database for a song\n\nTitle and UURI are searched. All the keywords must match ' \
                   'either the title or UURI. First 20 results are returned.\nThis command can be used to look up ' \
                   'song IDs.'

    @privileged(False)
    @dec.command(ignore_extra=False, help=_search_help)
    async def search(self, *keywords: str):
        items = await self._songs.search_songs(keywords)
        if len(items) == 0:
            await self._bot.whisper('Search for songs with keywords {} has not returned any result'.format(keywords))
            return
        reply = '**First 20 songs matching the keywords {}:**\n **>** '.format(keywords) + \
                '\n **>** '.join(['[{}] {}'.format(*item) for item in items])
        await self._bot.whisper(reply)

    _info_help = 'Returns information about a song from the database\n\nMainly for debug purposes.'

    @privileged(False)
    @dec.command(ignore_extra=False, help=_info_help)
    async def info(self, which: int):
        try:
            info = await self._songs.get_song_info(which)
        except ValueError as e:
            raise dec.UserInputError(str(e))
        reply = '**Song [{id}] information:**\n' \
                '    **Unique URI:** {uuri}\n' \
                '    **Title:** {title}\n' \
                '    **Last played:** {last_played!s}\n' \
                '    **Hype count:** {total_hype_count} ({hype_count})\n' \
                '    **Skip votes:** {total_skip_votes} ({skip_votes})\n' \
                '    **Duration:** {duration}s\n' \
                '    **Credits remaining:** {credits_remaining}\n\n' \
                '    **Blacklisted:** {blacklisted}\n\n' \
                '    **Marked as a duplicate of:** {duplicates}\n' \
                '    **Is duplicated by:** {duplicated_by}'.format_map(info)
        await self._bot.whisper(reply)

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
                '; This file contains blacklisted users. You can modify this file offline, use commands to edit the '
                'blacklist with the bot directly.\n; One user ID per line, lines starting with a semicolon are '
                'ignored. Text following user IDs is ignored too. File is automatically\n; generated and overwritten '
                'on bot shutdown. You have been warned.\n'
            )
            for user_id in self._ignorelist:
                member = discord.utils.get(self._bot.get_all_members(), id=user_id)
                if member is None:
                    file.write('{} <unknown username>\n'.format(user_id))
                else:
                    file.write('{} {}\n'.format(user_id, member))

    #
    # Helper methods
    #
    async def _message(self, message):
        return await self._bot.send_message(self._bot.text_channel, message)

    async def _log(self, message):
        return await self._bot.send_message(self._bot.log_channel, message)

    async def _shutdown(self):
        await asyncio.sleep(2, loop=self._bot.loop)
        await self._bot.logout()
