import asyncio
from aiohttp import web, errors
from contextlib import suppress


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
    def __init__(self, config, loop):
        self._config = config
        self._config_bitrate = int(config['bitrate'])
        self._frame_len = int(config['block_size'])
        self._loop = loop

        self._users = None
        self._app = None
        self._server = None
        self._handler = None
        self._sending_task = None

        self._data_event = asyncio.Event(loop=loop)
        self._current_frame = b''
        self._current_data = b''
        self._meta_changed = False
        self._current_meta = b'\0'

        self._lock = asyncio.Lock(loop=loop)
        # user -> (response, meta, init)
        self._connections = dict()

        self._generic_response_headers = {'Cache-Control': 'no-cache', 'Connection': 'close', 'Pragma': 'no-cache',
                                          'Server': 'DdmBot v:0.1 alpha', 'Content-Type': 'audio/aac',
                                          'Icy-BR': config['bitrate'], 'Icy-Pub': '0'}

        for icy_name, config_name in (('Icy-Name', 'name'), ('Icy-Description', 'description'), ('Icy-Genre', 'genre'),
                                      ('Icy-Url', 'url')):
            if config_name in config and config[config_name]:
                self._generic_response_headers[icy_name] = config[config_name]

    @property
    def bitrate(self):
        return self._config_bitrate * 1000

    @property
    def frame_len(self):
        return self._frame_len

    @property
    def url_format(self):
        return 'http://{}:{}{}?token={{}}'.format(self._config['hostname'], self._config['port'],
                                                  self._config['path'])

    #
    # Resource management wrappers
    #
    async def init(self, users):
        self._users = users
        self._app = web.Application(loop=self._loop)
        self._app.router.add_route('GET', self._config['path'], self._handle_new)
        self._handler = self._app.make_handler()
        self._sending_task = self._loop.create_task(self._sending_loop())

        self._server = await self._loop.create_server(self._handler, self._config['ip_address'],
                                                      int(self._config['port']))

    async def cleanup(self):
        if self._sending_task is not None:
            self._sending_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._sending_task
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
    async def set_meta(self, stream_title):
        # assemble metadata
        # TODO: magic length constant?
        metadata = 'StreamTitle:\'{}\';StreamURL=\'\';'.format(stream_title[:256].replace('\'', '\\\''))\
            .encode('utf-8', 'ignore')

        # now figure out the length of the metadata
        length = len(metadata) // 16
        if len(metadata) % 16:
            length += 1

        if length > 255:  # should not happen at all, theoretical maximum is about 100
            raise RuntimeError('Metadata too long')

        # prepend the length and pad with zeroes
        metadata = bytes([length]) + metadata.rjust(length * 16, b'\0')

        # atomically update the metadata
        async with self._lock:
            self._current_meta = metadata
            self._meta_changed = True

    def play_audio(self, data):
        # TODO: eh... thread safety?
        if self._data_event.is_set():
            # this is not good... previous data has not processed yet
            raise RuntimeError('StreamServer is stalled')

        self._current_data = data

        # we rely on the fact that this method is called on the frame boundaries (at least)
        self._current_frame = data if len(self._current_frame) == self._frame_len else self._current_frame + data

        # last thing to do is to notify the sending loop, safely
        self._loop.call_soon_threadsafe(self._data_event.set)

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
    async def _handle_new(self, request):
        # check for the token validity
        token = request.query_string[6:]
        user = await self._users.get_token_owner(token)
        if not request.query_string.startswith('token=') or user is None:
            response = web.Response(status=403)
            response.force_close()
            return response

        # assembly the response headers
        response_headers = self._generic_response_headers.copy()
        meta = False
        if 'icy-metadata' in request.headers and request.headers['icy-metadata'] == '1':
            response_headers['Icy-MetaInt'] = str(self._frame_len)
            meta = True

        # create response StreamResponse object
        response = web.StreamResponse(headers=response_headers)
        await response.prepare(request)
        # construct ConnectionInfo object
        connection = ConnectionInfo(response, meta, self._loop)
        await connection.prepare()
        # critical section - we are manipulating connections
        async with self._lock:
            if user in self._connections:
                # break the existing connection
                self._connections[user].terminate()
            self._connections[user] = connection
        # notify the UserManager that a new listener was added
        # race condition is possible, but only one of the connections will be served
        await self._users.add_listener(user, token)

        # wait before terminating
        await connection.wait()

        # now we are supposed to break the connection, do it explicitly before returning
        with suppress(errors.DisconnectedError, asyncio.CancelledError, ConnectionResetError):
            await response.write_eof()

        return response

    async def _sending_loop(self):
        while True:
            # wait for the next data batch
            await self._data_event.wait()
            # keep the list of disconnected listeners
            disconnected = list()
            # as this manipulates with buffers and connections, it is a critical section
            async with self._lock:
                # iterate over all connections
                for user, connection in self._connections.items():
                    try:
                        init = connection.first_send
                        # send data, if the connection is a new one whole frame (part) must be sent
                        if init:
                            connection.response.write(self._current_frame)
                        else:
                            connection.response.write(self._current_data)

                        # now, if the frame is complete, append the metadata
                        if connection.meta and len(self._current_frame) == self._frame_len:
                            if init or self._meta_changed:
                                connection.response.write(self._current_meta)
                            else:
                                connection.response.write(b'\0')
                        # call that actually rises any exceptions on a broken connection
                        # TODO: get rid of this coroutine somehow?
                        await connection.response.drain()
                    except (errors.DisconnectedError, asyncio.CancelledError, ConnectionResetError):
                        disconnected.append(user)
                        connection.terminate()
                # clear meta flag
                self._meta_changed = False
                # remove all the disconnected clients
                for user in disconnected:
                    self._connections.pop(user)

            # clear the flag for the next iteration
            self._data_event.clear()

            # and finally, notify UserManager if some users have left
            for user in disconnected:
                await self._users.remove_listener(user)
