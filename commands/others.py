import discord.ext.commands as dec


class Others:
    """Commands that don't fit anywhere else"""
    def __init__(self, bot):
        self._bot = bot

        # prepare direct stream info message
        ds_message = 'Playlist link: {}\nDirect link: `{}`\n\nPlease note that these links will expire in a few ' \
                     'minutes. Also, you can only be connected from a single location, including a discord voice ' \
                     'channel.'
        if self._bot.direct is not None:
            ds_message += ' If you are connected already, your previous connection will be terminated.'
        else:
            ds_message += ' If you are in the voice channel already, please disconnect before proceeding.'

        self._direct_stream_message = ds_message.format(bot.stream.playlist_url, bot.stream.stream_url)

    _help_messages = {
        'direct': 'Requests a link to the direct audio stream\n\n'
        'Instructions are sent along with the link.',

        'join': 'Adds you to the DJ queue\n\n'
        'You must be listening to do this. When you stop listening, you will be removed from the queue automatically.',

        'leave': 'Removes you from the DJ queue',

        'skip': 'Votes to skip the currently playing song\n\n'
        'You must be listening to vote. Skipping is supported in the DJ mode only. If a specified threshold (dependent '
        'on the number of listeners) is reached, song is skipped. If the current DJ uses this command, his / her song '
        'is skipped instantly.\nOperators can force the skip by supplying `force` as an argument to this command. '
        'Please note that this does not automatically blacklist the skipped song.',

        'unskip': 'Cancels a skip vote\n\n'
        'You can use this command to take back the skip vote you issued earlier.'
    }

    @dec.command(pass_context=True, ignore_extra=False, aliases=['d'], help=_help_messages['direct'])
    async def direct(self, ctx):
        token = await self._bot.users.generate_token(int(ctx.message.author.id))
        await self._bot.whisper(self._direct_stream_message.format(token, token))

    @dec.command(pass_context=True, ignore_extra=False, aliases=['j'], help=_help_messages['join'])
    async def join(self, ctx):
        if self._bot.player.streaming or self._bot.player.stopped:
            raise dec.UserInputError('Player is not in the DJ mode')
        await self._bot.users.join_queue(int(ctx.message.author.id))

    @dec.command(pass_context=True, ignore_extra=False, aliases=['l'], help=_help_messages['leave'])
    async def leave(self, ctx):
        await self._bot.users.leave_queue(int(ctx.message.author.id))

    @dec.command(pass_context=True, ignore_extra=False, help=_help_messages['skip'])
    async def skip(self, ctx, force: str = None):
        # if the skip is forced, check the privilege and do it
        if force and force.lower() in ['f', 'force']:
            if not self._bot.is_operator(ctx.message.author):
                raise dec.CommandError('You don\'t have a permission to force the skip')
            await self._bot.player.force_skip()
            await self._bot.message('Skip forced by {}'.format(ctx.message.author.mention))
            return

        # if the argument is not none, raise an error
        if force is not None:
            raise dec.UserInputError('*force* is the only argument allowed for the *skip* command')

        # now do the "normal voting"
        await self._bot.player.skip_vote(int(ctx.message.author.id))
        await self._bot.log('User {} has voted to skip'.format(ctx.message.author))

    @dec.command(pass_context=True, ignore_extra=False, help=_help_messages['unskip'])
    async def unskip(self, ctx):
        await self._bot.player.skip_unvote(int(ctx.message.author.id))
