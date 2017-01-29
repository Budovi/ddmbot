import asyncio
import errno
import logging
import os
import shlex
import subprocess
import threading
import time
from aiohttp import web, errors
from contextlib import suppress

import awaitablelock

# set up the logger
log = logging.getLogger('ddmbot.streamserver')


class AacProcessor(threading.Thread):
    def __init__(self, pipe_path, frame_len, bitrate, output_callback):
        if not callable(output_callback):
            raise TypeError('Output callback must be a callable object')

        super().__init__()

        self._pipe_fd = os.open(pipe_path, os.O_RDONLY | os.O_NONBLOCK)

        self._frame_len = frame_len
        self._frame_period = frame_len * 8 / bitrate

        self._play = output_callback

        self._end = threading.Event()

    def stop(self):
        self._end.set()
        self.join()
        self.flush()
        os.close(self._pipe_fd)

    def flush(self):
        try:
            os.read(self._pipe_fd, 1048576)  # TODO: change the magic constant
        except OSError as e:
            if e.errno != errno.EAGAIN:
                raise

    def run(self):
        loops = 0  # loop counter
        input_not_ready = False  # to control log spam
        data_requested = self._frame_len  # to keep the alignment intact

        # capture the starting time
        start_time = time.clock_gettime(time.CLOCK_MONOTONIC_RAW)
        while not self._end.is_set():
            # increment loop counter
            loops += 1

            # try to read a frame from the input -- should be there all the time
            try:
                data = os.read(self._pipe_fd, data_requested)
                # so we apparently got some data, clear the flag and calculate things
                input_not_ready = False
                data_len = len(data)
                if data_len != 0 and data_len != self._frame_len:
                    log.warning('AacProcessor: Got partial buffer of size {}'.format(data_len))

                # call the callback
                self._play(data)

                # calculate requested size for the next iteration
                data_requested -= data_len
                if data_requested == 0:
                    data_requested = self._frame_len
            except OSError as e:
                if e.errno == errno.EAGAIN:
                    # prevent spamming the log with megabytes of text
                    if not input_not_ready:
                        log.error('AacProcessor: Buffer not ready')
                        input_not_ready = True
                else:
                    raise

            # calculate next transmission time
            next_time = start_time + self._frame_period * loops
            sleep_time = max(0, self._frame_period + (next_time - time.clock_gettime(time.CLOCK_MONOTONIC_RAW)))
            time.sleep(sleep_time)


class ConnectionInfo:
    __slots__ = ['_response', '_meta', '_lock', '_init']

    def __init__(self, response: web.StreamResponse, meta: bool, loop: asyncio.AbstractEventLoop):
        self._response = response
        self._meta = meta
        self._lock = asyncio.Lock(loop=loop)
        self._init = True

    @property
    def response(self):
        return self._response

    @property
    def meta(self):
        return self._meta

    @property
    def first_send(self):
        if self._init:
            self._init = False
            return True
        return False

    async def prepare(self):
        if not self._lock.locked():
            await self._lock.acquire()

    async def wait(self):
        await self._lock.acquire()

    def terminate(self):
        self._lock.release()


class StreamServer:
    def __init__(self, bot):
        self._bot = bot
        self._config = bot.config['stream_server']
        self._config_bitrate = int(self._config['bitrate'])
        self._frame_len = int(self._config['block_size'])

        self._app = None
        self._server = None
        self._handler = None

        self._lock = awaitablelock.AwaitableLock(loop=bot.loop)
        # user -> ConnectionInfo
        self._connections = dict()

        ffmpeg_command = 'ffmpeg -loglevel error -y -f s16le -ar {} -ac {} -i {} -f adts -c:a {} -b:a {}k {}' \
            .format(bot.voice.encoder.sampling_rate, bot.voice.encoder.channels, shlex.quote(self._config['int_pipe']),
                    self._config['aac_encoder'], self._config_bitrate, shlex.quote(self._config['aac_pipe']))

        self._aac_thread = None
        self._cleanup_task = None
        self._internal_pipe = os.open(self._config['int_pipe'], os.O_RDONLY | os.O_NONBLOCK)
        self._ffmpeg = None
        self._ffmpeg_args = shlex.split(ffmpeg_command)
        self._connected = threading.Event()

        self._current_frame = b''
        self._meta_changed = False
        self._current_meta = b'\0'

        # URLs, response headers and payload assembly
        # TODO: handle URL encoding in the future (playlist_path may contain invalid characters)
        self._playlist_url = 'http://{hostname}:{port}{playlist_path}?token={{}}'.format_map(self._config)
        self._stream_url = 'http://{hostname}:{port}{stream_path}?token={{}}'.format_map(self._config)
        self._playlist_response_headers = {'Connection': 'close', 'Server': 'DdmBot streaming server', 'Content-type':
                                           'audio/mpegurl'}
        self._playlist_file = '#EXTM3U\r\n#EXTINF:-1,{name}\r\nhttp://{hostname}:{port}{stream_path}?{{}}' \
            .format_map(self._config)
        self._stream_response_headers = {'Cache-Control': 'no-cache', 'Connection': 'close', 'Pragma': 'no-cache',
                                         'Server': 'DdmBot streaming server', 'Content-Type': 'audio/aac',
                                         'Icy-BR': self._config['bitrate'], 'Icy-Pub': '0'}

        for icy_name, config_name in (('Icy-Name', 'name'), ('Icy-Description', 'description'), ('Icy-Genre', 'genre'),
                                      ('Icy-Url', 'url')):
            if config_name in self._config and self._config[config_name]:
                self._stream_response_headers[icy_name] = self._config[config_name]

    @property
    def playlist_url(self):
        return self._playlist_url

    @property
    def stream_url(self):
        return self._stream_url

    #
    # Resource management wrappers
    #
    async def init(self):
        # http server initialization
        self._app = web.Application(loop=self._bot.loop)
        self._app.router.add_route('GET', self._config['stream_path'], self._handle_new_stream)
        self._app.router.add_route('GET', self._config['playlist_path'], self._handle_new_playlist)
        self._handler = self._app.make_handler()

        self._server = await self._bot.loop.create_server(self._handler, self._config['ip_address'],
                                                          int(self._config['port']))

    async def cleanup(self):
        if self._server is not None:
            # stop listening on the socket
            self._server.close()
            await self._server.wait_closed()
        if self._app is not None:
            await self._app.shutdown()
        # close all remaining connections
        async with self._lock:
            for connection in self._connections.values():
                connection.terminate()
            self._connections.clear()
        if self._handler is not None:
            await self._handler.finish_connections(10)
        if self._app is not None:
            await self._app.cleanup()

    #
    # Player interface
    #
    def is_connected(self):
        return self._connected.is_set()

    async def set_meta(self, stream_title):
        # assemble metadata
        # TODO: magic length constant?
        metadata = 'StreamTitle=\'{}\';'.format(stream_title[:256].replace('\'', '\\\'')).encode('utf-8', 'ignore')

        # now figure out the length of the metadata
        length = len(metadata) // 16
        if len(metadata) % 16:
            length += 1

        if length > 255:  # should not happen at all, theoretical maximum is about 100
            raise RuntimeError('Metadata too long')

        # prepend the length and pad with zeroes
        metadata = bytes([length]) + metadata.ljust(length * 16, b'\0')

        # atomically update the metadata
        async with self._lock:
            log.debug('New metadata set: {}'.format(metadata))
            self._current_meta = metadata
            self._meta_changed = True

    #
    # UserManager interface
    #
    async def disconnect(self, user):
        async with self._lock:
            if user not in self._connections:
                return
            self._connections[user].terminate()
            self._connections.pop(user)

    #
    # Internal connection handling
    #
    async def _handle_new_stream(self, request):
        # check for the token validity
        token = request.query_string[6:]
        user = await self._bot.users.get_token_owner(token)
        if not request.query_string.startswith('token=') or user is None:
            response = web.Response(status=403)
            response.force_close()
            return response

        # assembly the response headers
        response_headers = self._stream_response_headers.copy()
        meta = False
        if 'ICY-METADATA' in request.headers and request.headers['ICY-METADATA'] == '1':
            response_headers['Icy-MetaInt'] = str(self._frame_len)
            meta = True

        log.debug('Valid stream request from {}, ICY-METADATA={}'.format(user, meta))

        # create response StreamResponse object
        response = web.StreamResponse(headers=response_headers)
        await response.prepare(request)
        # construct ConnectionInfo object
        connection = ConnectionInfo(response, meta, self._bot.loop)
        await connection.prepare()

        # critical section -- we are manipulating the connections
        async with self._lock:
            if not self._connections:
                # first listener needs to initialize everything
                log.debug('First listener initialization')
                # spawn cleanup task
                self._cleanup_task = self._bot.loop.create_task(self._cleanup_loop())
                # spawn ffmpeg process
                try:
                    self._ffmpeg = subprocess.Popen(self._ffmpeg_args)
                except FileNotFoundError as e:
                    raise RuntimeError('ffmpeg executable was not found') from e
                except subprocess.SubprocessError as e:
                    raise RuntimeError('Popen failed: {0.__name__} {1}'.format(type(e), str(e))) from e
                # create processing thread
                self._aac_thread = AacProcessor(self._config['aac_pipe'], self._frame_len, self._config_bitrate * 1000,
                                                self._play_audio)
                # enable input and output
                self._connected.set()
                self._aac_thread.start()

            elif user in self._connections:
                # break the existing connection
                log.debug('Previous connection for user {} found, signalling to terminate'.format(user))
                self._connections[user].terminate()

            # add the connection object to the _connections dictionary
            self._connections[user] = connection

        # notify the UserManager that a new listener was added
        # race condition is possible, but only one of the connections will be served
        await self._bot.users.add_listener(user, direct=True)

        # wait before terminating
        log.debug('Waiting for the client termination')
        with suppress(asyncio.CancelledError):
            await connection.wait()

        # now we are supposed to break the connection on request
        # self._connections.pop(user) left out INTENTIONALLY!
        # await self._users.remove_listener(user) left out INTENTIONALLY!
        # cleanup will be done by self._cleanup_task

        log.debug('Stream to {} terminated'.format(user))
        return response

    async def _handle_new_playlist(self, request):
        # TODO: handle URL encoding
        body = self._playlist_file.format(request.query_string)
        response = web.Response(text=body, headers=self._playlist_response_headers)
        return response

    def _play_audio(self, data):
        self._current_frame = data if len(self._current_frame) == self._frame_len else self._current_frame + data

        with self._lock:
            for user, connection in self._connections.items():
                init = connection.first_send
                # send data, if the connection is a new one whole frame (part) must be sent
                if init:
                    log.debug('Sending initial frame to {}'.format(user))
                    connection.response.write(self._current_frame)
                else:
                    connection.response.write(data)

                # now, if the frame is complete, append the metadata
                if connection.meta and len(self._current_frame) == self._frame_len:
                    if init or self._meta_changed:
                        log.debug('Sending metadata to {}'.format(user))
                        connection.response.write(self._current_meta)
                    else:
                        connection.response.write(b'\0')

            if len(self._current_frame) == self._frame_len:
                # metadata were sent
                self._meta_changed = False

    def _last_listener_cleanup(self):
        log.debug('Last listener deinitialization')

        # stop the input
        self._connected.clear()
        # kill ffmpeg process
        self._ffmpeg.kill()
        self._ffmpeg.communicate()
        # kill processing thread
        self._aac_thread.stop()
        # flush internal pipe
        try:
            os.read(self._internal_pipe, 1048576)
        except OSError as e:
            if e.errno != errno.EAGAIN:
                raise
        # reinitialize some internal variables
        self._current_frame = b''

    async def _cleanup_loop(self):
        while True:
            # sleep for small amount of time
            await asyncio.sleep(1, loop=self._bot.loop)

            # keep the list of disconnected listeners
            disconnected = list()
            # as this manipulates with connections, it is a critical section
            async with self._lock:
                # iterate over all connections
                for user, connection in self._connections.items():
                    try:
                        await asyncio.wait_for(connection.response.drain(), 0.001, loop=self._bot.loop)
                    except (errors.DisconnectedError, asyncio.CancelledError, ConnectionResetError):
                        log.debug('Connection broke with {}'.format(user))
                        disconnected.append(user)
                    except asyncio.TimeoutError:
                        log.debug('Connection stalled with {}'.format(user))
                        disconnected.append(user)

                # now we can pop disconnected listeners and notify the UserManager
                for user in disconnected:
                    self._connections.pop(user)
                    try:
                        await self._bot.users.remove_listener(user, direct=True)
                    except ValueError:
                        log.warning('Connection broke with {}, but the user was not listening'.format(user))

                # cleanup must be done here because the original handler won't be resumed
                if not self._connections:
                    self._last_listener_cleanup()
                    return
