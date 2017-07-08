import audioop
import asyncio
import enum
import errno
import fcntl
import functools
import logging
import os
import shlex
import subprocess
import threading
import time
from contextlib import suppress
from math import ceil

import discord.utils
import youtube_dl

from database.player import UnavailableSongError, PlayerInterface

# set up the logger
log = logging.getLogger('ddmbot.player')

# fcntl constants, extracted from linux API headers
FCNTL_F_LINUX_BASE = 1024
FCNTL_F_SETPIPE_SZ = FCNTL_F_LINUX_BASE + 7


class PcmProcessor(threading.Thread):
    def __init__(self, bot, next_callback):
        self._bot = bot
        config = bot.config['ddmbot']

        pipe_size = int(config['pcm_pipe_size'])
        if pipe_size > 2**31 or pipe_size <= 0:
            raise ValueError('Provided \'pcm_pipe_size\' is invalid')

        if not callable(next_callback):
            raise TypeError('Next callback must be a callable object')

        super().__init__()

        # despite the fact we expect voice_client to change, encoder parameters should be static
        self._frame_len = bot.voice.encoder.frame_size
        self._frame_period = bot.voice.encoder.frame_length / 1000.0
        self._volume = int(config['default_volume']) / 100

        self._in_pipe_fd = os.open(config['pcm_pipe'], os.O_RDONLY | os.O_NONBLOCK)
        self._out_pipe_fd = os.open(config['int_pipe'], os.O_WRONLY | os.O_NONBLOCK)

        try:
            fcntl.fcntl(self._in_pipe_fd, FCNTL_F_SETPIPE_SZ, pipe_size)
        except OSError as e:
            if e.errno == 1:
                raise RuntimeError('Required PCM pipe size is over the system limit, see \'pcm_pipe_size\' in the '
                                   'configuration file') from e
            raise e

        self._next = next_callback
        self._end = threading.Event()

    @property
    def volume(self):
        return self._volume

    @volume.setter
    def volume(self, value):
        self._volume = min(max(value, 0.0), 2.0)

    def stop(self):
        self._end.set()
        self.join()
        self.flush()
        os.close(self._in_pipe_fd)
        os.close(self._out_pipe_fd)

    def flush(self):
        try:
            os.read(self._in_pipe_fd, 1048576)
        except OSError as e:
            if e.errno != errno.EAGAIN:
                raise

    def run(self):
        loops = 0  # loop counter
        next_called = True  # variable to prevent constant calling of self._next()
        output_congestion = False  # to control log spam
        buffering_cycles = 0
        cycles_in_second = 1 // self._frame_period
        zero_data = b'\0' * self._frame_len

        # capture the starting time
        start_time = time.clock_gettime(time.CLOCK_MONOTONIC_RAW)
        while not self._end.is_set():
            # increment loop counter
            loops += 1
            # set initial value for data length
            data_len = 0

            # if it's not a buffering cycle read more data
            if buffering_cycles:
                buffering_cycles -= 1
                data = zero_data
            else:
                try:
                    data = os.read(self._in_pipe_fd, self._frame_len)
                    data_len = len(data)

                    if data_len:
                        next_called = False

                    if data_len != self._frame_len:
                        if data_len == 0:
                            # if we read nothing, that means the input to the pipe is not connected anymore
                            if not next_called:
                                next_called = True
                                self._next()
                            data = zero_data
                        else:
                            # if we read something, we are likely at the end of the input, pad with zeroes and log
                            # TODO: is there a way to distinguish buffering issues and end of the input issues?
                            log.debug('PcmProcessor: Data were padded with zeroes')
                            data.ljust(self._frame_len, b'\0')

                except OSError as e:
                    if e.errno == errno.EAGAIN:
                        data = zero_data
                        log.warning('PcmProcessor: Buffer not ready, waiting one second')
                        buffering_cycles = cycles_in_second
                    else:
                        raise

            # now we try to pass data to the output, if connected, we also send the silence (zero_data)
            if self._bot.stream.is_connected():
                try:
                    os.write(self._out_pipe_fd, data)
                    # data sent successfully, clear the congestion flag
                    output_congestion = False
                except OSError as e:
                    if e.errno == errno.EAGAIN:
                        # prevent spamming the log with megabytes of text
                        if not output_congestion:
                            log.error('PcmProcessor: Output pipe not ready, dropping frame(s)')
                            output_congestion = True
                    else:
                        raise
            else:
                # if we are not connected, there is no output congestion and the underlying buffer will be cleared
                output_congestion = False

            # and last but not least, discord output, this time, we can (should) omit partial frames or zero data
            voice_client = self._bot.voice
            if voice_client.is_connected() and data_len == self._frame_len:
                # adjust the volume
                data = audioop.mul(data, 2, self._volume)
                # call the callback
                voice_client.play_audio(data)

            # calculate next transmission time
            next_time = start_time + self._frame_period * loops
            sleep_time = max(0, self._frame_period + (next_time - time.clock_gettime(time.CLOCK_MONOTONIC_RAW)))
            time.sleep(sleep_time)


class PlayerState(enum.Enum):
    STOPPED = 0
    DJ_WAITING = 1
    DJ_COOLDOWN = 2
    DJ_PLAYING = 3
    STREAMING = 4


class Player:
    def __init__(self, bot):
        self._bot = bot
        self._config_skip_ratio = float(bot.config['ddmbot']['skip_ratio'])
        self._config_stream_end_transition = int(bot.config['ddmbot']['stream_end_transition'])

        # figure out initial state
        self._state = PlayerState.STOPPED

        self._next_state = PlayerState.STOPPED
        if bot.config['ddmbot']['initial_state'].lower() == 'djmode':
            self._next_state = PlayerState.DJ_PLAYING
        elif bot.config['ddmbot']['initial_state'].lower() != 'stopped':
            log.error('Initial state is invalid, assuming \'stopped\'')

        # state transition helpers
        self._transition_lock = asyncio.Lock(loop=bot.loop)
        self._switch_state = asyncio.Event(loop=bot.loop)
        self._auto_transition_task = None

        self._ytdl = youtube_dl.YoutubeDL({'extract_flat': 'in_playlist', 'format': 'bestaudio/best', 'quiet': True,
                                           'no_color': True})

        # state variables
        self._status_protection_count = 0
        self._apply_cooldown = True
        self._song_context = None
        self._stream_url = None
        self._stream_title = None
        self._status_message = None
        self._ffmpeg = None

        # create PCM thread
        self._pcm_thread = PcmProcessor(self._bot, self._playback_ended_callback)
        self._ffmpeg_command = 'ffmpeg -reconnect 1 -reconnect_delay_max 3 -loglevel error' \
                               ' -i {{}} -y -vn -f s16le -ar {} -ac {} {}'.format(bot.voice.encoder.sampling_rate,
            bot.voice.encoder.channels, shlex.quote(bot.config['ddmbot']['pcm_pipe']))

        # database interface
        self._database = PlayerInterface(bot.loop, bot.config['ddmbot'])

    #
    # Resource management wrappers
    #
    async def init(self):
        self._pcm_thread.start()
        await self._transition_lock.acquire()

    async def cleanup(self):
        if self._ffmpeg is not None and self._ffmpeg.poll() is None:
            self._ffmpeg.kill()
            self._ffmpeg.communicate()

        if self._pcm_thread is not None:
            self._pcm_thread.stop()

    #
    # Properties reflecting the player's state
    #
    @property
    def stopped(self):
        return self._state == PlayerState.STOPPED

    @property
    def waiting(self):
        return self._state == PlayerState.DJ_WAITING

    @property
    def cooldown(self):
        return self._state == PlayerState.DJ_COOLDOWN

    @property
    def playing(self):
        return self._state == PlayerState.DJ_PLAYING

    @property
    def streaming(self):
        return self._state == PlayerState.STREAMING

    #
    # Player controls
    #
    async def set_stop(self):
        async with self._transition_lock:
            if not self.stopped:
                self._next_state = PlayerState.STOPPED
                self._switch_state.set()
            # allow for cancelling auto transition task
            elif self._auto_transition_task is not None:
                self._auto_transition_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self._auto_transition_task
                self._auto_transition_task = None
                await self._bot.message('Auto transition was cancelled')

    async def set_djmode(self):
        async with self._transition_lock:
            if self.stopped or self.streaming:
                self._next_state = PlayerState.DJ_PLAYING
                self._switch_state.set()

    async def set_stream(self, stream_url, stream_title=None):
        self._stream_url = stream_url
        self._stream_title = stream_title
        async with self._transition_lock:
            self._next_state = PlayerState.STREAMING
            self._switch_state.set()

    async def set_stream_title(self, stream_title):
        async with self._transition_lock:
            if not self.streaming:
                raise RuntimeError('Title can be changed only in in the streaming mode')
            self._stream_title = stream_title
            self._status_message = None
            await self._update_status()

    async def skip_vote(self, user_id):
        if self._transition_lock.locked():  # TODO: change to try-lock construct, this is not atomic
            raise RuntimeError('Skip vote failed, please try again')
        async with self._transition_lock:
            if not self._bot.users.is_listening(user_id):
                raise RuntimeError('You must be listening to vote')
            if not self.playing:
                raise RuntimeError('You can vote to skip only when playing a song in the DJ mode')

            # handle skip by the DJ
            if self._song_context.dj_id == user_id:
                await self._bot.message('Song skipped by the DJ')
                self._switch_state.set()
                return

            # update song context
            self._song_context.skip_vote(user_id)
            # update the status
            await self._update_status()

            # check the skip condition
            listeners, skip_voters = self._song_context.get_current_counts()

            if listeners and skip_voters >= self._config_skip_ratio * listeners:
                await self._bot.message('Community voted to skip')
                self._switch_state.set()

    async def force_skip(self):
        if self._transition_lock.locked():
            raise RuntimeError('Skip failed, please try again (if still applicable)')
        async with self._transition_lock:
            if not self.playing:
                raise RuntimeError('Skip can be performed only when playing a song in the DJ mode')
            self._switch_state.set()

    async def skip_unvote(self, user_id):
        if self._transition_lock.locked():  # TODO: change to try-lock construct, this is not atomic
            raise RuntimeError('Removing skip vote failed, please try again')
        async with self._transition_lock:
            if not self.playing:
                raise RuntimeError('You haven\'t voted to skip')
            try:
                self._song_context.skip_unvote(user_id)
            except KeyError as e:
                raise RuntimeError('You haven\'t voted to skip') from e
            await self._update_status()

    @property
    def volume(self):
        return self._pcm_thread.volume

    @volume.setter
    def volume(self, value):
        self._pcm_thread.volume = value

    #
    # Status message reprint API
    #
    def bump_protection_counter(self):
        self._status_protection_count += 1

    async def reprint_status(self):
        async with self._transition_lock:
            if self._status_protection_count < 3:
                return
            self._status_message = None
            await self._update_status()

    #
    # UserManager interface
    #
    async def users_changed(self, listeners, djs_present):
        # we will need a transition lock in any case
        async with self._transition_lock:
            if self.stopped:
                # nobody cares about users
                return
            if listeners:
                if self.waiting:
                    self._switch_state.set()
                    return
            else:  # if not listeners_present
                if self.cooldown:
                    self._apply_cooldown = True
                    self._switch_state.set()
                    return

            if djs_present:
                self._apply_cooldown = True
                if self.cooldown:
                    self._switch_state.set()
                    return
            # if we are playing in the dj mode, we should update the song context
            if self.playing:
                self._song_context.update_listeners(listeners)
            # we also want to update the status message
            await self._update_status()

    #
    # Internally used methods and callbacks
    #
    async def _update_status(self):
        if not self._transition_lock.locked():
            raise RuntimeError('Update status may only be called with transition lock acquired')

        listener_count, direct_listeners, queue = await self._bot.users.get_display_info()
        # get all the display names mapping
        all_ids = direct_listeners | set(queue)
        # don't forget the name of the DJ
        if self.playing and self._song_context.dj_id is not None:
            all_ids.add(self._song_context.dj_id)
        names = dict()
        if all_ids:
            for member in self._bot.client.get_all_members():
                int_id = int(member.id)
                if int_id in all_ids:
                    names[int_id] = member.display_name
                    # break if we found all of them
                    if len(names) == len(all_ids):
                        break

        dls_str = ', '.join([names[ids] for ids in direct_listeners])

        new_status_message = None
        new_stream_title = None
        if self.stopped:
            new_status_message = '**Player is stopped**'
            # inform about automatic transition
            if self._auto_transition_task is not None:
                new_status_message += '\nAutomatic transition into DJ mode after {} seconds'.format(
                    self._config_stream_end_transition)

            new_stream_title = 'Awkward silence'
            await self._bot.client.change_presence()

        elif self.streaming:
            new_status_message = '**Playing stream:** {}\n**Direct listeners** ({}/{})**:** {}' \
                .format(self._stream_title, len(direct_listeners), listener_count, dls_str)
            new_stream_title = self._stream_title
            await self._bot.client.change_presence(game=discord.Game(
                name="a stream for {} listener(s)".format(listener_count)))

        elif self.waiting:
            new_status_message = '**Waiting for the first listener**'
            new_stream_title = 'Hold on a second...'
            await self._bot.client.change_presence(game=discord.Game(name="a waiting game :("))

        elif self.cooldown:
            new_status_message = '**Waiting for DJs**, automatic playlist will be initiated in a few seconds'
            new_stream_title = 'Waiting for DJs'
            await self._bot.client.change_presence(game=discord.Game(name="with a countdown clock"))

        elif self.playing:
            # assemble the rest of the information
            djs_str = ' -> '.join([names[ids] for ids in queue])
            queued_by = '' if self._song_context.dj_id is None else ', **queued by** <@{}>'.format(
                self._song_context.dj_id)
            skip_voters = self._song_context.get_current_counts()[1]
            skip_threshold = ceil(self._config_skip_ratio * listener_count)

            new_status_message = '**Playing:** [{0.song_id}] {0.song_title}, **length** {1}:{2:02d}{3}\n' \
                                 '**Skip votes:** {4}/{5} **Direct listeners** ({6}/{7})**:** {8}\n**Queue:** {9}' \
                .format(self._song_context, self._song_context.song_duration // 60,
                        self._song_context.song_duration % 60, queued_by, skip_voters, skip_threshold,
                        len(direct_listeners), listener_count, dls_str, djs_str)

            queued_by = '' if self._song_context.dj_id is None else ', queued by {}'.format(
                names[self._song_context.dj_id])
            new_stream_title = '{}{}'.format(self._song_context.song_title, queued_by)
            await self._bot.client.change_presence(game=discord.Game(
                name="songs from DJ queue for {} listener(s)".format(listener_count)))

        # Now that new_status_message and new_stream_title is put together, update them
        if self._status_message:
            self._status_message = await self._bot.client.edit_message(self._status_message, new_status_message)
            log.debug("Status message updated")
        else:
            self._status_message = await self._bot.message(new_status_message)
            await self._bot.stream.set_meta(new_stream_title)
            self._status_protection_count = 0
            log.debug("New status message created")

    async def _get_song(self, dj, retries=3):
        for _ in range(retries):
            try:
                song = await self._database.get_next_song(dj)
            except LookupError:  # no more songs in DJ's playlist
                await self._bot.users.leave_queue(dj)
                await self._bot.whisper_id(dj, 'Your playlist is empty. Please add more songs and rejoin the DJ queue.')
                return None
            except RuntimeError as e:  # there was a problem playing the song
                await self._bot.message('<@{}>, song skipped: {}'.format(dj, str(e)))
                continue
            except UnavailableSongError as e:
                await self._bot.log('Song [{}] *{}* was flagged due to a download error'
                                    .format(e.song_id, e.song_title))
                await self._bot.message('<@{}>, song skipped: {}'.format(dj, str(e)))
                continue
            return song
        await self._bot.users.leave_queue(dj)
        await self._bot.whisper_id(dj, 'Please try to fix your playlist and rejoin the queue')
        return None

    async def _get_stream_info(self):
        func = functools.partial(self._ytdl.extract_info, self._stream_url, download=False)
        try:
            info = await self._bot.loop.run_in_executor(None, func)
        except youtube_dl.DownloadError as e:
            await self._bot.message('Failed to obtain stream information: {}'.format(str(e)))
            return False
        if not self._stream_title:
            if 'twitch' in self._stream_url:  # TODO: regex should be much better
                self._stream_title = info.get('description')
            else:
                self._stream_title = info.get('title')
            if not self._stream_title:
                self._stream_title = '<untitled stream>'
        if 'url' not in info:
            await self._bot.message('Failed to extract stream URL, is the link valid?')
            return False
        self._stream_url = info['url']
        return True

    def _spawn_ffmpeg(self):
        if self.streaming:
            url = self._stream_url
        elif self.playing:
            url = self._song_context.song_url
        else:
            raise RuntimeError('Player is in an invalid state')

        args = shlex.split(self._ffmpeg_command.format(shlex.quote(url)))
        try:
            self._ffmpeg = subprocess.Popen(args)
        except FileNotFoundError as e:
            raise RuntimeError('ffmpeg executable was not found') from e
        except subprocess.SubprocessError as e:
            raise RuntimeError('Popen failed: {0.__name__} {1}'.format(type(e), str(e))) from e

    #
    # Player FSM
    #
    async def task_player_fsm(self):
        if not self._transition_lock.locked():
            raise RuntimeError('Transaction lock must be acquired before creating _player_fsm task')
        nothing_to_play = False

        await self._bot.wait_for_initialization()

        while True:
            #
            # Next state switch
            #
            log.debug('FSM: {} -> {}'.format(self._state, self._next_state))
            self._state = self._next_state

            #
            # STOPPED
            #
            if self.stopped:
                # clear the queue and dj_cooldown to behave as intended next time
                await self._bot.users.clear_queue()
                self._apply_cooldown = True
            #
            # STREAM_MODE
            #
            elif self.streaming:
                # clear the queue and dj_cooldown to behave as intended next time
                await self._bot.users.clear_queue()
                self._apply_cooldown = True
                # when the stream ends or is interrupted, next state should be 'stopped'
                self._next_state = PlayerState.STOPPED
                # get stream info
                if not await self._get_stream_info():
                    continue
                # let's play!
                self._spawn_ffmpeg()
            #
            # DJ_* MODES
            #
            elif self.waiting:
                self._apply_cooldown = True
                self._next_state = PlayerState.DJ_PLAYING
                # there is not much to do except wait

            elif self.cooldown:
                self._next_state = PlayerState.DJ_PLAYING
                # clear the flag indicating cooldown should be applied so next time it is skipped
                self._apply_cooldown = False
                # we will create a task that will trigger the transition
                cooldown_task = self._bot.loop.create_task(self._delayed_dj_task())

            elif self.playing:
                listeners = self._bot.users.get_current_listeners()
                # if there are no listeners left, we should just wait for someone to join
                if not listeners:
                    self._next_state = PlayerState.DJ_WAITING
                    continue

                # try to get a next dj and a song
                dj = await self._bot.users.get_next_dj()

                while dj is not None:
                    # we have a potential candidate for a dj, but nothing is certain at this point
                    # we will try to get a playable song, 3 times, then moving on to the next dj
                    self._song_context = await self._get_song(dj)
                    if self._song_context is not None:
                        break
                    dj = await self._bot.users.get_next_dj()

                if dj is None:
                    # time for an automatic playlist, but check if the cooldown state should be inserted before
                    if self._apply_cooldown:
                        self._next_state = PlayerState.DJ_COOLDOWN
                        continue

                    # ok, now we should just pick a song and play it
                    try:
                        self._song_context = await self._database.get_autoplaylist_song()
                    except UnavailableSongError as e:
                        # we need to log this to the logging channel
                        await self._bot.log('Song [{}] *{}* was flagged due to a download error'
                                            .format(e.song_id, e.song_title))
                        continue

                    if self._song_context is None:
                        # if we did not succeed with automatic playlist, we're... eh doomed?
                        # considering credit replenish every 24 h, we just need about 400 applicable
                        # songs slightly longer than 3.5 minutes
                        if not nothing_to_play:
                            nothing_to_play = True
                            await self._bot.message('No suitable song found for automatic playlist. Join the DJ queue '
                                                    'to play!')
                        self._apply_cooldown = True
                        self._next_state = PlayerState.DJ_COOLDOWN
                        continue

                # at this point, _song_context should contain a valid SongContext object
                # so let's clear a flag and play it!
                nothing_to_play = False
                self._song_context.update_listeners(listeners)
                self._spawn_ffmpeg()

            # update status message and ICY meta information
            if not (self.cooldown and nothing_to_play):
                await self._update_status()

            #
            # State event -- current state should be set up, we now have to wait
            #
            self._switch_state.clear()
            self._transition_lock.release()
            log.debug('FSM: waiting')
            await self._switch_state.wait()
            log.debug('FSM: trying to acquire lock')
            await self._transition_lock.acquire()

            #
            # Previous state is over at this point, we should do a proper cleanup
            #

            # reset the status message reference -- it is now invalid
            self._status_message = None

            # update song stats
            if self.playing:
                # we need to actually wait for this to ensure proper functionality of overplaying protection
                await self._database.update_stats(self._song_context)
                self._song_context = None

            # if we were in cooldown, cancel cooldown task if not finished
            elif self.cooldown:
                cooldown_task.cancel()
                with suppress(asyncio.CancelledError):
                    await cooldown_task

            # if we were in stopped state, cancel auto transition task if not finished
            elif self.stopped and self._auto_transition_task is not None:
                self._auto_transition_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self._auto_transition_task
                self._auto_transition_task = None

            # kill ffmpeg if still running
            if self._ffmpeg is not None and self._ffmpeg.poll() is None:
                self._ffmpeg.kill()
                self._ffmpeg.communicate()

            # clean the IPC pipes used
            self._pcm_thread.flush()

    #
    # Other helper methods
    #
    def _playback_ended_callback(self):
        self._bot.loop.call_soon_threadsafe(self._playback_ended)

    def _playback_ended(self):  # TODO: atomicity provided by GIL
        if self._transition_lock.locked():
            # assuming the FSM is doing a transition already
            return
        if self.playing or self.streaming:
            self._switch_state.set()
        if self.streaming and self._config_stream_end_transition:
            self._auto_transition_task = self._bot.loop.create_task(self._delayed_stream_end_transition_task())

    async def _delayed_dj_task(self):
        await asyncio.sleep(15, loop=self._bot.loop)
        async with self._transition_lock:
            if self.cooldown:
                self._switch_state.set()

    async def _delayed_stream_end_transition_task(self):
        await asyncio.sleep(self._config_stream_end_transition, loop=self._bot.loop)
        async with self._transition_lock:
            if self.stopped:
                self._next_state = PlayerState.DJ_PLAYING
                self._switch_state.set()
