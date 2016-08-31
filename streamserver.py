import asyncio
import errno
import logging
import os
import shlex
import subprocess
import threading
import time
from aiohttp import web, errors

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
            os.read(self._pipe_fd, 1048576)
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
                if data_len != self._frame_len:
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
    __slots__ = ['_response', '_meta', '_event', '_init']

    def __init__(self, response: web.StreamResponse, meta: bool, loop: asyncio.AbstractEventLoop):
        self._response = response
        self._meta = meta
        self._event = asyncio.Event(loop=loop)
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

    async def wait(self):
        await self._event.wait()

    def terminate(self):
        self._event.set()


class StreamServer:
    def __init__(self, config, loop):
        self._config = config
        self._config_bitrate = int(config['bitrate'])
        self._frame_len = int(config['block_size'])
        self._loop = loop

        self._users = None
        self._app = None
        self._server = None
        self._handler = None

        self._lock = awaitablelock.AwaitableLock(loop=loop)
        # user -> ConnectionInfo
        self._connections = dict()

        self._aac_thread = None
        self._cleanup_task = None
        self._internal_pipe = os.open(config['int_pipe'], os.O_RDONLY | os.O_NONBLOCK)
        self._ffmpeg = None
        self._ffmpeg_args = None
        self._connected = threading.Event()

        self._current_frame = b''
        self._meta_changed = False
        self._current_meta = b'\0'

        # response headers and payload assembly
        self._playlist_response_headers = {'Connection': 'close', 'Server': 'DdmBot v:0.1 alpha', 'Content-type':
                                           'audio/mpegurl'}
        self._playlist_file = '#EXTM3U\r\n#EXTINF:-1,{}\r\nhttp://{}:{}{}?{{}}'\
            .format(config['name'], config['hostname'], config['port'], config['stream_path'])
        self._stream_response_headers = {'Cache-Control': 'no-cache', 'Connection': 'close', 'Pragma': 'no-cache',
                                         'Server': 'DdmBot v:0.1 alpha', 'Content-Type': 'audio/aac',
                                         'Icy-BR': config['bitrate'], 'Icy-Pub': '0'}

        for icy_name, config_name in (('Icy-Name', 'name'), ('Icy-Description', 'description'), ('Icy-Genre', 'genre'),
                                      ('Icy-Url', 'url')):
            if config_name in config and config[config_name]:
                self._stream_response_headers[icy_name] = config[config_name]

    @property
    def url_format(self):
        # TODO: handle URL encoding in the future (playlist_path may contain invalid characters)
        return 'http://{}:{}{}?token={{}}'.format(self._config['hostname'], self._config['port'],
                                                  self._config['playlist_path'])

    #
    # Resource management wrappers
    #
    async def init(self, users, bot_voice):
        self._users = users
        ffmpeg_command = 'ffmpeg -loglevel error -y -f s16le -ar {} -ac {} -i {}' \
                         ' -f adts -c:a libfdk_aac -b:a {}k {}' \
            .format(bot_voice.encoder.sampling_rate, bot_voice.encoder.channels, shlex.quote(self._config['int_pipe']),
                    self._config_bitrate, shlex.quote(self._config['aac_pipe']))
        self._ffmpeg_args = shlex.split(ffmpeg_command)

        # http server initialization
        self._app = web.Application(loop=self._loop)
        self._app.router.add_route('GET', self._config['stream_path'], self._handle_new_stream)
        self._app.router.add_route('GET', self._config['playlist_path'], self._handle_new_playlist)
        self._handler = self._app.make_handler()

        self._server = await self._loop.create_server(self._handler, self._config['ip_address'],
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
    @property
    def internal_pipe_path(self):
        return self._config['int_pipe']

    @property
    def connected(self):
        return self._connected

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
        user = await self._users.get_token_owner(token)
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

        log.debug('New stream request from {}, ICY-METADATA={}'.format(user, meta))

        # create response StreamResponse object
        response = web.StreamResponse(headers=response_headers)
        await response.prepare(request)
        # construct ConnectionInfo object
        connection = ConnectionInfo(response, meta, self._loop)

        # critical section -- we are manipulating the connections
        async with self._lock:
            if len(self._connections) == 0:
                # first listener needs to initialize everything
                log.debug('First listener initialization')
                # spawn cleanup task
                self._cleanup_task = self._loop.create_task(self._cleanup_loop())
                # spawn ffmpeg process
                #args = shlex.split(self._ffmpeg_command.format(shlex.quote(url)))
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
                self._connections[user].terminate()

            # add the connection object to the _connections dictionary
            self._connections[user] = connection

        # notify the UserManager that a new listener was added
        # race condition is possible, but only one of the connections will be served
        await self._users.add_listener(user, token)

        # wait before terminating
        log.debug('Waiting for the client termination')
        await connection.wait()

        # now we are supposed to break the connection on request
        # self._connections.pop(user) left out INTENTIONALLY!
        # await self._users.remove_listener(user) left out INTENTIONALLY!
        # cleanup will be done by self._cleanup_task

        log.debug('Stream to {} terminated'.format(user))
        return response

    async def _handle_new_playlist(self, request):
        # TODO: handle URL encoding
        log.debug('New playlist request, query string: {}'.format(request.query_string))
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
            await asyncio.sleep(1, loop=self._loop)

            # keep the list of disconnected listeners
            disconnected = list()
            # as this manipulates with connections, it is a critical section
            async with self._lock:
                # iterate over all connections
                for user, connection in self._connections.items():
                    try:
                        await asyncio.wait_for(connection.response.drain(), 0.001, loop=self._loop)
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
                        await self._users.remove_listener(user)
                    except ValueError:
                        log.warning('Connection broke with {}, but the user was not listening'.format(user))

                # cleanup must be done here because the original handler won't be resumed
                if len(self._connections) == 0:
                    self._last_listener_cleanup()
                    return
