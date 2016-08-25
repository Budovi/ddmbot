import audioop
import asyncio
import enum
import errno
import os
import shlex
import subprocess
import threading
import time
from contextlib import suppress

import discord

import youtube_dl
import functools


class StreamProcessor(threading.Thread):
    def __init__(self, pipe_path, frame_len, frame_period, output_callback, **kwargs):
        if not callable(output_callback):
            raise TypeError('Output callback must be a callable object')

        super().__init__(**kwargs)
        self._pipe_fd = os.open(pipe_path, os.O_RDONLY | os.O_NONBLOCK)
        self._frame_len = frame_len
        self._frame_period = frame_period
        self._play = output_callback

        self._end = threading.Event()
        self._volume = 1.0

    def run(self):
        raise NotImplementedError()

    def stop(self):
        self._end.set()
        self.join()
        self.flush()
        os.close(self._pipe_fd)

    def flush(self):
        try:
            os.read(self._pipe_fd, 1048576)
        except OSError as e:
            if e.errno != errno.EAGAIN:
                raise


class PcmProcessor(StreamProcessor):
    def __init__(self, pipe_path, encoder, connected, output_callback, next_callback=None, **kwargs):
        if next_callback is not None and not callable(next_callback):
            raise TypeError('Next callback must be a callable object')

        super().__init__(pipe_path, encoder.frame_size, encoder.frame_length / 1000.0, output_callback, **kwargs)

        self._next = next_callback
        self._volume = 1.0
        self._connected = connected

    def run(self):
        loops = 0
        next_called = True
        start_time = time.clock_gettime(time.CLOCK_MONOTONIC_RAW)
        while not self._end.is_set():
            loops += 1
            try:
                data = os.read(self._pipe_fd, self._frame_len)
            except OSError as e:
                if e.errno == errno.EAGAIN:
                    data = ''
                else:
                    raise

            if self._volume != 1.0:
                data = audioop.mul(data, 2, self._volume)

            data_len = len(data)
            if data_len == 0:
                if not next_called and self._next is not None:
                    next_called = True
                    self._next()
            else:
                next_called = False
                if self._connected.is_set():
                    # we have at least some data, let's send them
                    if len(data) != self._frame_len:
                        data.ljust(self._frame_len, b'\0')
                    self._play(data)

            # calculate next transmission time
            next_time = start_time + self._frame_period * loops
            sleep_time = max(0, self._frame_period + (next_time - time.clock_gettime(time.CLOCK_MONOTONIC_RAW)))
            time.sleep(sleep_time)

    @property
    def volume(self):
        return self._volume

    @volume.setter
    def volume(self, value):
        self._volume = min(max(value, 0.0), 2.0)


class AacProcessor(StreamProcessor):
    def __init__(self, pipe_path, frame_len, bitrate, output_callback, **kwargs):
        super().__init__(pipe_path, frame_len, frame_len * 8 / bitrate, output_callback, **kwargs)

    def run(self):
        loops = 0
        data_requested = self._frame_len
        start_time = time.clock_gettime(time.CLOCK_MONOTONIC_RAW)
        while not self._end.is_set():
            loops += 1
            try:
                data = os.read(self._pipe_fd, data_requested)
            except OSError as e:
                if e.errno == errno.EAGAIN:
                    data = ''
                else:
                    raise

            data_len = len(data)
            if data_len != 0:
                # we cannot align the data with zeroes, so send what we've got
                self._play(data)

            # next time we will try to send the rest of the frame
            data_requested -= data_len
            if data_requested == 0:
                data_requested = self._frame_len

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
        self._dj_cooldown = asyncio.Event(loop=bot.loop)

        self._song_context = None

        self._ytdl = youtube_dl.YoutubeDL({'extract_flat': 'in_playlist', 'format': 'bestaudio/best', 'quiet': True,
                                           'no_color': True})
        self._stream_url = None
        self._stream_name = None

        users.on_first_listener(self._first_listener_callback)
        users.on_first_dj(self._dj_cooldown.set)
        users.on_last_dj(self._dj_cooldown.clear)

        self._player_task = None
        self._pcm_thread = None
        self._aac_thread = None
        self._ffmpeg_command = None
        self._ffmpeg = None

        self._status_message = None
        self._status_channel = None

        self._voice_client = None

    #
    # Resource management wrappers
    #
    def init(self, bot_voice, aac_server):
        self._voice_client = bot_voice
        self._status_channel = discord.utils.get(self._bot.get_all_channels(), id=self._config['text_channel'],
                                                 type=discord.ChannelType.text)
        if self._status_channel is None:
            raise RuntimeError('Status text channel specified was not found')

        # TODO: replace protected member access with a method VoiceClient.is_connected()
        self._pcm_thread = PcmProcessor(self._config['pcm_pipe'], bot_voice.encoder, bot_voice._connected,
                                        bot_voice.play_audio, self._playback_ended_callback)
        self._aac_thread = AacProcessor(self._config['aac_pipe'], aac_server.frame_len, aac_server.bitrate,
                                        aac_server.play_audio)

        self._pcm_thread.start()
        self._aac_thread.start()

        self._ffmpeg_command = 'ffmpeg -loglevel error -i {{}} -y -vn' \
                               ' -f s16le -ar {} -ac {} {}' \
                               ' -f adts -ac 2 -c:a libfdk_aac -b:a {}k {}' \
            .format(bot_voice.encoder.sampling_rate, bot_voice.encoder.channels, shlex.quote(self._config['pcm_pipe']),
                    aac_server.bitrate // 1000, shlex.quote(self._config['aac_pipe']))
        self._player_task = self._bot.loop.create_task(self._player_fsm())

    async def cleanup(self):
        if self._pcm_thread is not None:
            self._pcm_thread.stop()
        if self._aac_thread is not None:
            self._aac_thread.stop()

        if self._player_task is not None:
            self._player_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._player_task

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
        if self.cooldown:
            self._next_state = PlayerState.STOPPED
            self._dj_cooldown.set()
        else:
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
        if self.cooldown:
            self._next_state = PlayerState.STREAMING
            self._dj_cooldown.set()
        else:
            async with self._transition_lock:
                self._next_state = PlayerState.STREAMING
                self._switch_state.set()

    async def hype(self, user_id):
        if self._transition_lock.locked():
            return
        async with self._transition_lock:
            if self.playing:
                self._song_context.hype(user_id)
                self._bot.loop.create_task(self._update_status())

    async def skip(self, user_id):
        if self._transition_lock.locked():
            return
        async with self._transition_lock:
            try:
                if self.playing:
                    self._song_context.skip(user_id)
                    self._bot.loop.create_task(self._update_status())
            except ValueError:
                # skipped by the user playing
                await self._bot.say('Song skipped by the DJ')
                self._switch_state.set()

    async def force_skip(self):
        if self._transition_lock.locked():
            return False
        async with self._transition_lock:
            if self.playing or self.streaming:
                self._switch_state.set()
        return True

    #
    # Internally used methods and callbacks
    #
    async def _update_status(self):
        async with self._transition_lock:
            listeners, djs = await self._users.get_state()
            hypes = self._song_context.get_hype_set() if self.playing else set()

            # get all the display names mapping
            names = dict()
            if len(listeners | hypes) > 0:
                for member in self._bot.get_all_members():
                    int_id = int(member.id)
                    if int_id in listeners | hypes:
                        names[int_id] = member.display_name
                        if len(names) == len(listeners):
                            break

            listeners_str = ', '.join(names.values())
            message = None

            if self.playing:
                # assemble the rest of the information
                hypes_str = ', '.join([names[ids] for ids in hypes])
                djs_str = ' -> '.join([names[ids] for ids in djs])
                message = 'Playing: [{0.song_id}] {0.title}, queued by <@{0.user_id}>\n' \
                          'Hypes: {0.hype_count} ({1})\n' \
                          'Skip votes: {0.skip_votes}\n' \
                          'Listeners: {2}\n' \
                          'Queue: {3}'.format(self._song_context, hypes_str, listeners_str, djs_str)
                # check for the automatic skip
                listener_skips = listeners & self._song_context.get_skip_set()
                if len(listener_skips) / len(listeners) >= self._config_skip_ratio:
                    await self._bot.send_message(self._status_channel, 'Community voted to skip')
                    self._switch_state.set()

            elif self.streaming:
                message = 'Playing stream: {}\n' \
                          'Listeners: {}'.format(self._stream_name, listeners_str)

            if message:
                if self._status_message:
                    self._status_message = await self._bot.edit_message(self._status_message, message)
                else:
                    self._status_message = await self._bot.send_message(self._status_channel, message)
                    # TODO: messages needs to be unpinned to be pinned (limit of 50 pinned messages)
                    # await self._bot.pin_message(self._status_message)

    def _first_listener_callback(self):
        self._bot.loop.create_task(self._first_listener())

    async def _first_listener(self):
        async with self._transition_lock:
            if self.waiting:
                self._switch_state.set()

    def _playback_ended_callback(self):
        self._bot.loop.call_soon_threadsafe(self._playback_ended)

    def _playback_ended(self):
        if self._transition_lock.locked():
            # assuming the FSM is doing a transition already
            return
        # otherwise should be safe, stream will stop or new song played
        self._switch_state.set()

    async def _delayed_dj_task(self):
        await asyncio.sleep(15)
        self._dj_cooldown.set()

    async def _get_song(self, dj, retries=3):
        for _ in range(retries):
            try:
                song = await self._songs.get_next_song(dj)
            except LookupError as e:  # no more songs in DJ's playlist
                await self._users.leave_queue(dj)
                await self._bot.send_message(self._status_channel, '<@{}>, your playlist is empty, please add more'
                                                                   ' songs and rejoin the queue'.format(dj))
                return None
            except ValueError as e:  # there was a problem playing the song
                await self._bot.send_message(self._status_channel, 'Skipped: {}'.format(str(e)))
                continue
            return song
        await self._users.leave_queue(dj)
        await self._bot.send_message(self._status_channel, '<@{}>, please try to fix your playlist and rejoin the queue'
                                     .format(dj))
        return None

    async def _get_stream_info(self):
        func = functools.partial(self._ytdl.extract_info, self._stream_url, download=False)
        try:
            info = await self._bot.loop.run_in_executor(None, func)
        except youtube_dl.DownloadError:
            await self._bot.send_message(self._status_channel, 'Failed to download stream information')
            return False
        if self._stream_name is None:
            self._stream_name = info['title'] if len(info['title']) > 0 else '<untitled>'
        if 'url' not in info:
            await self._bot.send_message(self._status_channel, 'Failed to get stream URL')
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
        print('FSM init - acquire transition lock')
        await self._transition_lock.acquire()
        while True:
            #
            # Next state switch
            #
            print('FSM: {} -> {}'.format(self._state, self._next_state))
            self._state = self._next_state

            #
            # STOPPED
            #
            if self.stopped:
                # clear the queue and dj_cooldown to behave as intended next time
                await self._users.clear_queue()
                self._dj_cooldown.clear()
            #
            # STREAM_MODE
            #
            elif self.streaming:
                # clear the queue and dj_cooldown to behave as intended next time
                await self._users.clear_queue()
                self._dj_cooldown.clear()
                # when the stream ends or is interrupted, next state should be 'stopped'
                self._next_state = PlayerState.STOPPED
                # get stream info
                if not await self._get_stream_info():
                    continue
                # let's play!
                self._spawn_ffmpeg()
                self._bot.loop.create_task(self._update_status())

            #
            # DJ_* MODES
            #
            elif self.waiting:
                self._next_state = PlayerState.DJ_PLAYING
                # there is not much to do except wait

            elif self.cooldown:
                self._next_state = PlayerState.DJ_PLAYING
                if not self._dj_cooldown.is_set():
                    task = self._bot.loop.create_task(self._delayed_dj_task())
                    await self._dj_cooldown.wait()
                    task.cancel()
                # transition to the next state is automatic
                continue

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
                    if not self._dj_cooldown.is_set():
                        self._next_state = PlayerState.DJ_COOLDOWN
                        continue

                    # ok, now we should just pick a song and play it
                    self._song_context = await self._songs.get_autoplaylist_song()

                    if self._song_context is None:
                        # if we did not succeed with automatic playlist, we're... eh doomed?
                        # considering credit replenish every 24 h, we just need about 400 applicable
                        # songs slightly longer than 3.5 minutes
                        self._dj_cooldown.clear()
                        self._next_state = PlayerState.DJ_COOLDOWN
                        continue

                # at this point, _song_context should contain a valid SongContext object
                # so let's play it!
                self._spawn_ffmpeg()
                self._bot.loop.create_task(self._update_status())

            #
            # State event -- current state should be set up, we now have to wait
            #
            self._switch_state.clear()
            self._transition_lock.release()
            print('FSM: waiting')
            await self._switch_state.wait()
            print('FSM: trying to acquire lock')
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

            # kill ffmpeg if still running
            if self._ffmpeg is not None and self._ffmpeg.poll() is None:
                self._ffmpeg.kill()
                self._ffmpeg.communicate()

            # clean the IPC pipes used
            self._pcm_thread.flush()
            self._aac_thread.flush()
