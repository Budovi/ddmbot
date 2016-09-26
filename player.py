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

import discord.utils
import youtube_dl

from songmanager import UnavailableSongError

# set up the logger
log = logging.getLogger('ddmbot.player')

# fcntl constants, extracted from linux API headers
FCNTL_F_LINUX_BASE = 1024
FCNTL_F_SETPIPE_SZ = FCNTL_F_LINUX_BASE + 7


class PcmProcessor(threading.Thread):
    def __init__(self, encoder, pipe_sizes, input_pipe, output_connected, output_pipe, client_connected,
                 client_callback, next_callback):
        if not callable(client_callback):
            raise TypeError('Client callback must be a callable object')
        if not callable(next_callback):
            raise TypeError('Next callback must be a callable object')

        super().__init__()

        self._frame_len = encoder.frame_size
        self._frame_period = encoder.frame_length / 1000.0
        self._volume = 1.0

        self._in_pipe_fd = os.open(input_pipe, os.O_RDONLY | os.O_NONBLOCK)

        self._output_connected = output_connected
        self._out_pipe_fd = os.open(output_pipe, os.O_WRONLY | os.O_NONBLOCK)

        try:
            fcntl.fcntl(self._in_pipe_fd, FCNTL_F_SETPIPE_SZ, pipe_sizes)
            fcntl.fcntl(self._in_pipe_fd, FCNTL_F_SETPIPE_SZ, pipe_sizes)
        except OSError as e:
            if e.errno == 1:
                raise RuntimeError('Required PCM pipe size is over the system limit, see \'pcm_pipe_size\' in the '
                                   '[player] section of the configuration file') from e
            raise e

        self._client_connected = client_connected
        self._play = client_callback

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
            if self._output_connected.is_set():
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
            if self._client_connected.is_set() and data_len == self._frame_len:
                # adjust the volume
                data = audioop.mul(data, 2, self._volume)
                # call the callback
                self._play(data)

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
    def __init__(self, config, bot, users, songs):
        self._config_skip_ratio = float(config['skip_ratio'])
        self._config_pipe_size = int(config['pcm_pipe_size'])
        if self._config_pipe_size > 2**31 or self._config_pipe_size <= 0:
            raise ValueError('Provided \'pcm_pipe_size\' from the [player] configuration is invalid')
        self._config = config
        self._bot = bot
        self._users = users
        self._songs = songs

        # initial state is stopped
        self._state = PlayerState.STOPPED
        self._next_state = PlayerState.STOPPED
        # and state transitions are locked
        self._transition_lock = asyncio.Lock(loop=bot.loop)
        self._switch_state = asyncio.Event(loop=bot.loop)
        self._apply_cooldown = True

        self._song_context = None

        self._ytdl = youtube_dl.YoutubeDL({'extract_flat': 'in_playlist', 'format': 'bestaudio/best', 'quiet': True,
                                           'no_color': True})
        self._stream_url = None
        self._stream_name = None

        self._player_task = None
        self._pcm_thread = None
        self._ffmpeg_command = None
        self._ffmpeg = None

        self._status_message = None
        self._meta_callback = None
        self._voice_client = None

    #
    # Resource management wrappers
    #
    async def init(self, bot_voice, aac_server):
        self._voice_client = bot_voice
        self._meta_callback = aac_server.set_meta

        # TODO: replace protected member access with a method VoiceClient.is_connected()
        self._pcm_thread = PcmProcessor(bot_voice.encoder, self._config_pipe_size, self._config['pcm_pipe'],
                                        aac_server.connected, aac_server.internal_pipe_path, bot_voice._connected,
                                        bot_voice.play_audio, self._playback_ended_callback)
        self._pcm_thread.volume = int(self._config['volume']) / 100
        self._pcm_thread.start()

        self._ffmpeg_command = 'ffmpeg -loglevel error -i {{}} -y -vn' \
                               ' -f s16le -ar {} -ac {} {}' \
            .format(bot_voice.encoder.sampling_rate, bot_voice.encoder.channels, shlex.quote(self._config['pcm_pipe']))

        await self._transition_lock.acquire()
        # now that we have the lock, set the initial state, this will prevent any interference before starting the FSM
        if self._config['initial_state'] == 'playing':
            self._next_state = PlayerState.DJ_PLAYING
        elif self._config['initial_state'] != 'stopped':
            log.error('Initial state is invalid, assuming \'stopped\'')
        self._player_task = self._bot.loop.create_task(self._player_fsm())

    async def cleanup(self):
        if self._player_task is not None:
            self._player_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._player_task

        if self._ffmpeg is not None and self._ffmpeg.poll() is None:
            self._ffmpeg.kill()
            self._ffmpeg.communicate()

        if self._pcm_thread is not None:
            self._pcm_thread.stop()

    #
    # Properties reflecting the player's state
    #
    @property
    def voice_client(self):
        return self._voice_client

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

    async def set_djmode(self):
        async with self._transition_lock:
            if self.stopped or self.streaming:
                self._next_state = PlayerState.DJ_PLAYING
                self._switch_state.set()

    async def set_stream(self, stream_url, stream_name=None):
        self._stream_url = stream_url
        self._stream_name = stream_name
        async with self._transition_lock:
            self._next_state = PlayerState.STREAMING
            self._switch_state.set()

    async def hype(self, user_id):
        if self._transition_lock.locked():  # TODO: change to try-lock construct, this is not atomic
            return
        async with self._transition_lock:
            if self.playing:
                self._song_context.hype(user_id)
                await self._update_status()

    async def skip(self, user_id):
        if self._transition_lock.locked():  # TODO: change to try-lock construct, this is not atomic
            return
        async with self._transition_lock:
            try:
                if self.playing:
                    self._song_context.skip(user_id)
                    await self._update_status()
            except ValueError:
                # skipped by the user playing
                await self._message('Song skipped by the DJ')
                self._switch_state.set()

    async def force_skip(self):
        if self._transition_lock.locked():
            return False
        async with self._transition_lock:
            if self.playing or self.streaming:
                self._switch_state.set()
        return True

    @property
    def volume(self):
        return self._pcm_thread.volume

    @volume.setter
    def volume(self, value):
        self._pcm_thread.volume = value

    #
    # UserManager interface
    #
    async def users_changed(self, listeners_present, djs_present):
        # we will need a transition lock in any case
        async with self._transition_lock:
            if self.stopped:
                # nobody cares about users
                return
            if listeners_present:
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

            # we want to update status message if we are not transitioning
            await self._update_status()

    #
    # Internally used methods and callbacks
    #
    async def _update_status(self):
        if not self._transition_lock.locked():
            raise RuntimeError('Update status may only be called with transition lock acquired')

        listeners, djs = await self._users.get_state()
        hypes = self._song_context.get_hype_set() if self.playing else set()

        # get all the display names mapping
        all_ids = listeners | hypes | {self._song_context.user_id} if self.playing else listeners
        names = dict()
        if all_ids:
            for member in self._bot.get_all_members():
                int_id = int(member.id)
                if int_id in all_ids:
                    names[int_id] = member.display_name
                    # break if we found all of them
                    if len(names) == len(all_ids):
                        break

        listeners_str = ', '.join([names[ids] for ids in listeners])
        message = None
        stream_title = None

        if self.stopped:
            message = '**Player is stopped**'
            stream_title = 'Awkward silence'
            await self._bot.change_status()

        elif self.streaming:
            message = '**Playing stream:** {}\n' \
                      '**Listeners:** {}'.format(self._stream_name, listeners_str)
            stream_title = self._stream_name
            await self._bot.change_status(discord.Game(name="a stream for {} listener(s)".format(len(listeners))))

        elif self.waiting:
            message = '**Waiting for the first listener**'
            stream_title = 'Hold on a second...'
            await self._bot.change_status(discord.Game(name="a waiting game :("))

        elif self.cooldown:
            message = '**Waiting for DJs**, automatic playlist will be initiated in a few seconds'
            stream_title = 'Waiting for DJs'
            await self._bot.change_status(discord.Game(name="with a countdown clock"))

        elif self.playing:
            # assemble the rest of the information
            hypes_str = ', '.join([names[ids] for ids in hypes])
            djs_str = ' -> '.join([names[ids] for ids in djs])
            queued_by = 'auto-playlist' if self._song_context.user_id is None else \
                '<@{}>'.format(self._song_context.user_id)

            message = '**Playing:** [{0.song_id}] {0.title}, **length** {1}:{2:02d}, **queued by** {3}\n' \
                      '**Hypes:** {0.hype_count} ({4})\n**Skip votes:** {0.skip_votes}\n' \
                      '**Listeners:** {5}\n**Queue:** {6}' \
                .format(self._song_context, self._song_context.duration // 60, self._song_context.duration % 60,
                        queued_by, hypes_str, listeners_str, djs_str)

            queued_by = 'auto-playlist' if self._song_context.user_id is None else names[self._song_context.user_id]
            stream_title = '{}, queued by {}'.format(self._song_context.title, queued_by)
            await self._bot.change_status(discord.Game(name="songs from DJ queue for {} listener(s)"
                                                       .format(len(listeners))))

            # check for the automatic skip
            listener_skips = listeners & self._song_context.get_skip_set()
            if listeners and len(listener_skips) >= self._config_skip_ratio * len(listeners):
                await self._bot.send_message(self._bot.text_channel, 'Community voted to skip')
                self._switch_state.set()

        if self._status_message:
            self._status_message = await self._bot.edit_message(self._status_message, message)
            log.debug("Status message updated")
        else:
            self._status_message = await self._message(message)
            await self._meta_callback(stream_title)
            log.debug("New status message created")

    async def _get_song(self, dj, retries=3):
        for _ in range(retries):
            try:
                song = await self._songs.get_next_song(dj)
            except LookupError:  # no more songs in DJ's playlist
                await self._users.leave_queue(dj)
                await self._whisper(dj, 'Your playlist is empty. Please add more songs and rejoin the DJ queue.')
                return None
            except RuntimeError as e:  # there was a problem playing the song
                await self._message('Song skipped: {}'.format(str(e)))
                continue
            except UnavailableSongError as e:
                await self._log('Song [{}] *{}* was flagged due to a download error'
                                .format(e.song_id, e.song_title))
                await self._message('Song skipped: {}'.format(str(e)))
                continue
            return song
        await self._users.leave_queue(dj)
        await self._whisper(dj, 'Please try to fix your playlist and rejoin the queue')
        return None

    async def _get_stream_info(self):
        func = functools.partial(self._ytdl.extract_info, self._stream_url, download=False)
        try:
            info = await self._bot.loop.run_in_executor(None, func)
        except youtube_dl.DownloadError as e:
            await self._message('Failed to obtain stream information: {}'.format(str(e)))
            return False
        if not self._stream_name:
            if 'twitch' in self._stream_url:  # TODO: regex should be much better
                self._stream_name = info.get('description')
            else:
                self._stream_name = info.get('title')
            if not self._stream_name:
                self._stream_name = '<untitled stream>'
        if 'url' not in info:
            await self._message('Failed to extract stream URL, is the link valid?')
            return False
        self._stream_url = info['url']
        return True

    def _spawn_ffmpeg(self):
        if self.streaming:
            url = self._stream_url
        elif self.playing:
            url = self._song_context.url
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
    async def _player_fsm(self):
        if not self._transition_lock.locked():
            raise RuntimeError('Transaction lock must be acquired before creating _player_fsm task')
        nothing_to_play = False
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
                await self._users.clear_queue()
                self._apply_cooldown = True
            #
            # STREAM_MODE
            #
            elif self.streaming:
                # clear the queue and dj_cooldown to behave as intended next time
                await self._users.clear_queue()
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
                # if there are no listeners left, we should just wait for someone to join
                if not self._users.someone_listening():
                    self._next_state = PlayerState.DJ_WAITING
                    continue

                # try to get a next dj and a song
                dj = await self._users.get_next_dj()

                while dj is not None:
                    # we have a potential candidate for a dj, but nothing is certain at this point
                    # we will try to get a playable song, 3 times, then moving on to the next dj
                    self._song_context = await self._get_song(dj)
                    if self._song_context is not None:
                        break
                    dj = await self._users.get_next_dj()

                if dj is None:
                    # time for an automatic playlist, but check if the cooldown state should be inserted before
                    if self._apply_cooldown:
                        self._next_state = PlayerState.DJ_COOLDOWN
                        continue

                    # ok, now we should just pick a song and play it
                    try:
                        self._song_context = await self._songs.get_autoplaylist_song()
                    except UnavailableSongError as e:
                        # we need to log this to the logging channel
                        await self._log('Song [{}] *{}* was flagged due to a download error'
                                        .format(e.song_id, e.song_title))
                        continue

                    if self._song_context is None:
                        # if we did not succeed with automatic playlist, we're... eh doomed?
                        # considering credit replenish every 24 h, we just need about 400 applicable
                        # songs slightly longer than 3.5 minutes
                        if not nothing_to_play:
                            nothing_to_play = True
                            await self._message('No suitable song found for automatic playlist. Join the DJ queue to '
                                                'play!')
                        self._apply_cooldown = True
                        self._next_state = PlayerState.DJ_COOLDOWN
                        continue

                # at this point, _song_context should contain a valid SongContext object
                # so let's clear a flag and play it!
                nothing_to_play = False
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
                await self._songs.update_stats(self._song_context)
                self._song_context = None

            # if we were in cooldown, cancel cooldown task if not finished
            elif self.cooldown:
                cooldown_task.cancel()
                with suppress(asyncio.CancelledError):
                    await cooldown_task

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

    async def _delayed_dj_task(self):
        await asyncio.sleep(15, loop=self._bot.loop)
        async with self._transition_lock:
            if self.cooldown:
                self._switch_state.set()

    def _whisper(self, user_id, message):
        user = discord.utils.get(self._bot.get_all_members(), id=str(user_id))
        if user is None:
            return
        return self._bot.send_message(user, message)

    def _message(self, message):
        return self._bot.send_message(self._bot.text_channel, message)

    def _log(self, message):
        return self._bot.send_message(self._bot.log_channel, message)
