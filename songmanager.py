import asyncio
import functools
import random
import re
from contextlib import suppress
from datetime import datetime, timedelta

import peewee
import youtube_dl

# database object
# TODO: can we get rid of this 'ugly global'?
# actually, deferred database object is available, not recommended to use though
database = peewee.SqliteDatabase(None, journal_mode='WAL')


class DBSchema(peewee.Model):
    class Meta:
        database = database


class DBCreditTimestamp(DBSchema):
    last = peewee.DateTimeField()


class DBSong(DBSchema):
    id = peewee.PrimaryKeyField()

    # song unique URI for consistent lookup and search
    uuri = peewee.CharField(index=True)
    # title can be changed eventually
    title = peewee.CharField()
    last_played = peewee.DateTimeField()

    hype_count = peewee.IntegerField(default=0)
    skip_votes = peewee.IntegerField(default=0)
    play_count = peewee.IntegerField(default=0)

    duration = peewee.IntegerField()
    credit_count = peewee.IntegerField()
    is_blacklisted = peewee.BooleanField(default=False)

    duplicate = peewee.ForeignKeyField('self', null=True)  # TODO handle on_delete?


# we will need this to resolve a dependency loop
DeferredDBUser = peewee.DeferredRelation()


class DBSongLink(DBSchema):
    id = peewee.PrimaryKeyField()

    user = peewee.ForeignKeyField(DeferredDBUser)
    song = peewee.ForeignKeyField(DBSong)
    next = peewee.ForeignKeyField('self', null=True)


class DBUser(DBSchema):
    discord_id = peewee.BigIntegerField(primary_key=True)

    hype_count_got = peewee.IntegerField(default=0)
    hype_count_given = peewee.IntegerField(default=0)
    skip_votes_got = peewee.IntegerField(default=0)
    skip_votes_given = peewee.IntegerField(default=0)
    play_count = peewee.IntegerField(default=0)

    playlist_head = peewee.ForeignKeyField(DBSongLink, null=True, default=None)


DeferredDBUser.set_model(DBUser)


class SongContext:
    __slots__ = ['_user', '_song', '_title', '_url', '_hypes', '_skips']

    def __init__(self, user_id, song_id, title, url):
        self._user = user_id
        self._song = song_id
        self._title = title
        self._url = url

        self._hypes = set()
        self._skips = set()

    @property
    def song_id(self):
        return self._song

    @property
    def user_id(self):
        return self._user

    @property
    def title(self):
        return self._title

    @property
    def url(self):
        return self._url

    def hype(self, user_id):
        if user_id != self._user:
            self._hypes.add(user_id)
            self._skips.discard(user_id)

    def skip(self, user_id):
        if user_id == self._user:
            raise ValueError('Self skip should be handled by the player')
        self._hypes.discard(user_id)
        self._skips.add(user_id)

    @property
    def hype_count(self):
        return len(self._hypes)

    @property
    def skip_votes(self):
        return len(self._skips)

    def get_hype_set(self):
        return self._hypes

    def get_skip_set(self):
        return self._skips


class SongManager:
    def __init__(self, config, loop):
        self._config_ap_threshold = int(config['ap_hype_threshold'])
        self._config_ap_ratio = int(config['ap_hype_skip_ratio'])
        self._config_max_duration = int(config['length_limit'])
        self._config_max_songs = int(config['chain_limit'])
        self._config_op_interval = int(config['op_interval'])
        self._config_op_credit_cap = int(config['op_credit_cap'])
        self._config_op_credit_renew = timedelta(hours=int(config['op_credit_renew']))
        self._loop = loop

        self._ytdl = youtube_dl.YoutubeDL({'extract_flat': 'in_playlist', 'format': 'bestaudio/best', 'quiet': True,
                                           'no_color': True})
        self._database = database
        self._database.init(config['db_file'])

        self._credit_task = None

    #
    # Resource management wrappers
    #
    def init(self):
        self._database.connect()
        self._database.create_tables([DBCreditTimestamp, DBUser, DBSong, DBSongLink], safe=True)

        self._credit_task = self._loop.create_task(self._credit_renew())

    async def cleanup(self):
        self._database.close()

        if self._credit_task is not None:
            self._credit_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._credit_task

    #
    # Interface to be used by a player
    #
    async def get_next_song(self, user_id):
        func = functools.partial(self._get_next_song, user_id)
        return await self._loop.run_in_executor(None, func)

    async def update_stats(self, song_ctx):
        func = functools.partial(self._update_stats, song_ctx)
        return await self._loop.run_in_executor(None, func)

    async def get_autoplaylist_song(self):
        func = functools.partial(self._get_autoplaylist_song)
        return await self._loop.run_in_executor(None, func)

    #
    # Playlist operations
    #
    async def list_playlist(self, user_id, offset, limit):
        func = functools.partial(self._list_playlist, user_id, offset, limit)
        return await self._loop.run_in_executor(None, func)

    async def append_to_playlist(self, user_id, uris):
        func = functools.partial(self._append_to_playlist, user_id, uris)
        return await self._loop.run_in_executor(None, func)

    async def shuffle_playlist(self, user_id):
        func = functools.partial(self._shuffle_playlist, user_id)
        return await self._loop.run_in_executor(None, func)

    async def clear_playlist(self, user_id):
        func = functools.partial(self._clear_playlist, user_id)
        return await self._loop.run_in_executor(None, func)

    #
    # Song management
    #
    async def add_to_blacklist(self, song_id):
        func = functools.partial(self._add_to_blacklist, song_id)
        return await self._loop.run_in_executor(None, func)

    async def remove_from_blacklist(self, song_id):
        func = functools.partial(self._remove_from_blacklist, song_id)
        return await self._loop.run_in_executor(None, func)

    async def search_songs(self, keywords):
        func = functools.partial(self._search_songs, keywords)
        return await self._loop.run_in_executor(None, func)

    async def get_song_info(self, song_id):
        func = functools.partial(self._get_song_info, song_id)
        return await self._loop.run_in_executor(None, func)

    async def merge_songs(self, source_id, target_id):
        func = functools.partial(self._merge_songs, source_id, target_id)
        return await self._loop.run_in_executor(None, func)

    async def split_song(self, song_id):
        func = functools.partial(self._merge_songs, song_id, song_id)
        return await self._loop.run_in_executor(None, func)

    async def rename_song(self, song_id, new_title):
        func = functools.partial(self._rename_song, song_id, new_title)
        return await self._loop.run_in_executor(None, func)

    #
    # Internally used methods and attributes
    #

    # some class (static) constant variables
    _yt_regex = re.compile(r'^(https?://)?(www\.)?youtu(\.be/|be.com/.+?[?&]v=)(?P<id>[a-zA-Z0-9_-]+)')
    _sc_regex = re.compile(r'^(https?://)?soundcloud.com/(?P<artist>[^/]+)/(?P<track>[^/?]+)')
    _bc_regex = re.compile(r'^(https?://)?(?P<artist>[^.]+).bandcamp.com/track/(?P<track>[^/?]+)')
    _list_regex = re.compile(
        r'^(https?://)?(www\.youtube\.com/.*[?&]list=.+|soundcloud\.com/[^/]+/sets/.+|[^.:/]+\.bandcamp.com/album/.+)$')
    _url_base = {'yt': 'https://www.youtube.com/watch?v={}',
                 'sc': 'https://soundcloud.com/{}/{}',
                 'bc': 'https://{}.bandcamp.com/track/{}'}

    @staticmethod
    def _make_url(song_uuri):
        uuri_parts = song_uuri.split(':')
        return SongManager._url_base[uuri_parts[0]].format(*uuri_parts[1:])

    @staticmethod
    def _is_list(input_url):
        return SongManager._list_regex.match(input_url) is not None

    @staticmethod
    def _make_uuri(song_url):
        # makes unique URI from URLs suitable for database storage
        # method will return URI in one of the following formats:
        #   yt:<youtube_id> for youtube video
        #   sc:<artist>:<track> for soundcloud
        #   bc:<artist>:<track> for bandcamp
        match = SongManager._yt_regex.match(song_url)
        if match:
            return 'yt:{}'.format(match.group('id'))
        match = SongManager._sc_regex.match(song_url)
        if match:
            return 'sc:{}:{}'.format(match.group('artist'), match.group('track'))
        match = SongManager._bc_regex.match(song_url)
        if match:
            return 'bc:{}:{}'.format(match.group('artist'), match.group('track'))
        return None

    def _get_user(self, user_id):  # intentionally kept as an instance method
        # this can be potentially the first query on the user
        user, created = DBUser.get_or_create(discord_id=user_id)
        return user

    def _get_song(self, song_url):
        song_uuri = self._make_uuri(song_url)
        if not song_uuri:
            # TODO: try second time with youtube_dl URL resolution (not sure how effective that would be)
            raise ValueError('Malformed URL or unsupported service: {}'.format(song_url))
        # potentially the first query of the song
        try:
            song = DBSong.get(DBSong.uuri == song_uuri)
        except DBSong.DoesNotExist:
            # we need to create a new record, youtube_dl is necessary to obtain a title and a song length
            result = self._ytdl.extract_info(self._make_url(song_uuri), download=False, process=False)
            try:
                title = result['title']
            except KeyError:
                raise RuntimeError('Failed to extract song title')
            try:
                duration = int(result['duration'])
            except (KeyError, ValueError):
                raise RuntimeError('Failed to extract song duration')
            song, created = DBSong.create_or_get(uuri=song_uuri, title=title, last_played=datetime.utcfromtimestamp(0),
                                                 duration=duration, credit_count=self._config_op_credit_cap)
        return song

    def _get_next_song(self, user_id):
        song = None
        with self._database.atomic():
            # check if there is an associated playlist
            user = self._get_user(user_id)
            if user.playlist_head_id is None:
                raise LookupError('User\'s playlist is empty')

            # join song link and song tables to obtain a result
            link = DBSongLink.select(DBSongLink, DBSong).join(DBSong).where(
                DBSongLink.id == user.playlist_head_id).get()
            song = link.song
            # update next song "pointer"
            user.playlist_head_id = link.next_id
            user.save()
            link.delete_instance()
            # check duplicate song flag
            if song.duplicate_id is not None:
                song = song.duplicate

        # check the constrains
        # -- blacklist
        if song.is_blacklisted:
            raise RuntimeError('Song [{}] is blacklisted'.format(song.id))
        # -- last played
        time_diff = datetime.now() - song.last_played
        if time_diff.total_seconds() < self._config_op_interval:
            raise RuntimeError('Song [{}] has been played recently'.format(song.id))
        # -- credits remaining
        if song.credit_count == 0:
            raise RuntimeError('Song [{}] is overplayed'.format(song.id))
        # -- check the song length
        if song.duration > self._config_max_duration:
            raise RuntimeError('Song [{}]\'s length exceeds the limit'.format(song.id))

        # fetch the URL using youtube_dl
        result = self._ytdl.extract_info(self._make_url(song.uuri), download=False)
        return SongContext(user_id, song.id, song.title, result['url'])

    def _update_stats(self, song_ctx: SongContext):
        current_time = datetime.now()
        # update a song in the database -- hype, skip count, credit count, last played
        song_query = DBSong.update(hype_count=DBSong.hype_count + song_ctx.hype_count,
                                   skip_votes=DBSong.skip_votes + song_ctx.skip_votes,
                                   play_count=DBSong.play_count + 1,
                                   last_played=current_time, credit_count=DBSong.credit_count - 1) \
            .where(DBSong.id == song_ctx.song_id)
        # update a user in the database -- hype, skip count song received
        user_query = DBUser.update(hype_count_got=DBUser.hype_count_got + song_ctx.hype_count,
                                   skip_votes_got=DBUser.skip_votes_got + song_ctx.skip_votes,
                                   play_count=DBUser.play_count + 1) \
            .where(DBUser.discord_id == song_ctx.user_id)

        # prepare all the other users which may not exist in the database
        user_dict_list = [{'discord_id': user_id} for user_id in (song_ctx.get_hype_set() | song_ctx.get_skip_set())]
        if len(user_dict_list):
            DBUser.insert_many(user_dict_list).on_conflict('ignore').execute()
        # now do the votes query which will update _given stats
        hypes_query = DBUser.update(hype_count_given=DBUser.hype_count_given + 1)\
            .where(DBUser.discord_id << song_ctx.get_hype_set())
        skips_query = DBUser.update(skip_votes_given=DBUser.skip_votes_given + 1)\
            .where(DBUser.discord_id << song_ctx.get_skip_set())

        with self._database.atomic():
            song_query.execute()
            user_query.execute()
            hypes_query.execute()
            skips_query.execute()

    def _get_autoplaylist_song(self):
        reference_time = datetime.now() - \
                         timedelta(seconds=self._config_op_interval)
        query = DBSong.select(DBSong).where(
            DBSong.last_played < reference_time,  # overplay protection interval
            DBSong.hype_count >= self._config_ap_threshold,  # hype threshold
            DBSong.skip_votes * self._config_ap_ratio <= DBSong.hype_count,  # hype to skip ratio
            DBSong.duration <= self._config_max_duration,  # song duration
            DBSong.credit_count > 0,  # overplay protection
            ~DBSong.is_blacklisted,  # cannot be blacklisted
            DBSong.duplicate >> None  # not fair + outdated information
        ).order_by(peewee.fn.Random())

        try:
            song = query.get()
        except DBSong.DoesNotExist:
            # there is no song conforming to the autoplaylist conditions
            return None

        result = self._ytdl.extract_info(self._make_url(song.uuri), download=False)
        return SongContext(None, song.id, song.title, result['url'])

    def _list_playlist(self, user_id, offset, limit):
        result = list()
        with self._database.atomic():
            user = self._get_user(user_id)
            current_link_id = user.playlist_head_id
            for index in range(limit + offset):
                if current_link_id is None:
                    break
                link = DBSongLink.select(DBSongLink, DBSong).join(DBSong).where(DBSongLink.id == current_link_id).get()
                if index >= offset:
                    result.append((link.song.id, link.song.title))
                current_link_id = link.next_id
        return result

    def _process_uris(self, uris, limit):
        song_list = list()
        error_list = list()
        for uri in uris:
            if len(song_list) >= limit:
                return song_list, error_list, True
            if uri.isdigit():  # test if it's a plain integer -- we will assume it's an unique URI
                try:
                    song_list.append(DBSong.get(id=int(uri)))
                except DBSong.DoesNotExist:
                    error_list.append('Song [{}] cannot be found in the database'.format(uri))
            elif self._is_list(uri):
                try:  # because of youtube_dl
                    result = self._ytdl.extract_info(uri, download=False)
                    if 'entries' not in result:
                        error_list.append('Malformed URL or unsupported service: {}'.format(uri))
                        continue

                    for entry in result['entries']:
                        if len(song_list) >= limit:
                            return song_list, error_list, True
                        try:  # youtube_dl or regex matching can fail
                            if entry['ie_key'] == 'Youtube':
                                # for some reason youtube URLs are not URLs but video IDs
                                entry['url'] = self._url_base['yt'].format(entry['id'])
                            song_list.append(self._get_song(entry['url']))
                        except ValueError as e:
                            error_list.append(str(e))
                        except youtube_dl.utils.DownloadError as e:
                            error_list.append('{}, while processing {}'.format(str(e), entry['url']))
                except youtube_dl.utils.DownloadError as e:
                    error_list.append('{}, while processing list {}'.format(str(e), uri))
            else:  # should be a single song
                try:  # youtube_dl or regex matching can fail
                    song_list.append(self._get_song(uri))
                except ValueError as e:
                    error_list.append(str(e))
                except youtube_dl.utils.DownloadError as e:
                    error_list.append('{}, while processing {}'.format(str(e), uri))

        return song_list, error_list, False

    def _append_to_playlist(self, user_id, uris):
        count = DBSongLink.select().where(DBSongLink.user == user_id).count()
        if count >= self._config_max_songs:
            raise RuntimeError('Your playlist is full')
        # assembly the list of songs for insertion
        song_list, error_list, truncated = self._process_uris(uris, self._config_max_songs - count)
        # now create the links in the database
        with self._database.atomic():
            user = self._get_user(user_id)

            # atomically re-check the condition
            to_insert = self._config_max_songs - DBSongLink.select().where(DBSongLink.user == user_id).count()
            if to_insert <= 0:
                raise RuntimeError('Your playlist is full')

            connection_point = None
            if user.playlist_head_id is not None:
                connection_point = DBSongLink.get(DBSongLink.user == user_id, DBSongLink.next >> None)

            previous_link = None
            for song in song_list[to_insert - 1::-1]:
                previous_link = DBSongLink.create(user=user_id, song=song.id, next=previous_link).id
            # connect the chain created
            if connection_point is not None:
                connection_point.next_id = previous_link
                connection_point.save()
            else:
                user.playlist_head_id = previous_link
                user.save()

        truncated |= len(song_list) > to_insert
        inserted = min(len(song_list), to_insert)

        return inserted, truncated, error_list

    def _shuffle_playlist(self, user_id):
        query = DBSongLink.select().where(DBSongLink.user == user_id)
        song_list = list()

        # TODO: find a better way
        # this approach is awfully inefficient, in most databases you can random shuffle column using a single query
        # idea: join the table with randomly ordered selection on equal row numbers; update with the joined value
        # problem: approach is totally non-portable
        with self._database.atomic():
            for item in query:
                song_list.append(item.song_id)
            random.shuffle(song_list)
            for item, new_id in zip(query, song_list):
                item.song_id = new_id
                item.save()

    def _clear_playlist(self, user_id):
        update_query = DBUser.update(playlist_head=None).where(DBUser.discord_id == user_id)
        delete_query = DBSongLink.delete().where(DBSongLink.user == user_id)
        with self._database.atomic():
            update_query.execute()
            delete_query.execute()

    def _add_to_blacklist(self, song_id):  # intentionally kept as an instance method
        if DBSong.update(is_blacklisted=True).where(DBSong.id == song_id).execute() != 1:
            raise ValueError('Song [{}] cannot be found in the database'.format(song_id))

    def _remove_from_blacklist(self, song_id):  # intentionally kept as an instance method
        if DBSong.update(is_blacklisted=False).where(DBSong.id == song_id).execute() != 1:
            raise ValueError('Song [{}] cannot be found in the database'.format(song_id))

    def _search_songs(self, keywords):  # intentionally kept as an instance method
        query = DBSong.select(DBSong.id, DBSong.title)
        for keyword in keywords:
            keyword = '%{}%'.format(keyword)
            query = query.where((DBSong.title ** keyword) | (DBSong.uuri ** keyword))
        query = query.limit(20)

        result = list()
        for row in query:
            result.append((row.id, row.title))
        return result

    def _get_song_info(self, song_id):  # intentionally kept as an instance method
        try:
            song = DBSong.get(id=song_id)
            result = {'id': song.id, 'title': song.title, 'last_played': song.last_played, 'uuri': song.uuri,
                      'hype_count': song.hype_count, 'total_hype_count': song.hype_count,
                      'skip_votes': song.skip_votes, 'total_skip_votes': song.skip_votes,
                      'play_count': song.play_count, 'total_play_count': song.play_count,
                      'duration': song.duration, 'credits_remaining': song.credit_count,
                      'blacklisted': song.is_blacklisted, 'duplicates': None, 'duplicated_by': list()}
            if song.duplicate_id is not None:
                song2 = song.duplicate
                result['duplicates'] = (song2.id, song2.title)
                result['total_hype_count'] += song2.hype_count
                result['total_skip_votes'] += song2.skip_votes
                result['total_play_count'] += song2.play_count
            duplicate_query = DBSong.select().where(DBSong.duplicate == song.id)
            for song2 in duplicate_query:
                result['duplicated_by'].append((song2.id, song2.title))
                result['total_hype_count'] += song2.hype_count
                result['total_skip_votes'] += song2.skip_votes
                result['total_play_count'] += song2.play_count

            return result
        except DBSong.DoesNotExist:
            raise ValueError('Song [{}] cannot be found in the database'.format(song_id))

    def _merge_songs(self, source_id, target_id):
        if source_id == target_id:
            # this is effectively a "split" call
            if DBSong.update(duplicate=None).where(DBSong.id == source_id).execute() != 1:
                raise ValueError('Song [{}] cannot be found in the database'.format(source_id))
        else:
            with self._database.atomic():
                try:
                    target_song = DBSong.get(DBSong.id == target_id)
                except DBSong.DoesNotExist:
                    raise ValueError('Song [{}] cannot be found in the database'.format(target_id))

                if target_song.duplicate_id == source_id:
                    # we're "reassigning" the duplicate flags
                    target_song.duplicate_id = None
                    target_song.save()
                elif target_song.duplicate_id is not None:
                    # if a target is duplicate, we will update to duplicate_id instead
                    target_id = target_song.duplicate_id
                if DBSong.update(duplicate=target_id).where(
                                (DBSong.id == source_id) | (DBSong.duplicate == source_id)).execute() == 0:
                    raise ValueError('Song [{}] cannot be found in the database'.format(source_id))

    def _rename_song(self, song_id, new_title):  # intentionally kept as an instance method
        if DBSong.update(title=new_title).where(DBSong.id == song_id).execute() != 1:
            raise ValueError('Song [{}] cannot be found in the database'.format(song_id))

    async def _credit_renew(self):
        # check if the last timestamp is present in the database
        if DBCreditTimestamp.select().count() == 0:
            DBCreditTimestamp.create(last=datetime.now())

        # now the endless task loop
        while True:
            current_time = datetime.now()
            last_time = DBCreditTimestamp.get().last
            credits_to_add = (current_time - last_time) // self._config_op_credit_renew
            if credits_to_add > 0:
                # written timestamp correction
                written_timestamp = last_time + (credits_to_add * self._config_op_credit_renew)
                # construct the queries and execute them in a transaction
                ts_query = DBCreditTimestamp.update(last=written_timestamp)
                credit_query = DBSong.update(
                    credit_count=peewee.fn.MIN(DBSong.credit_count + credits_to_add, self._config_op_credit_cap))
                with self._database.atomic():
                    ts_query.execute()
                    credit_query.execute()
            # next check in an hour
            await asyncio.sleep(3600, loop=self._loop)
