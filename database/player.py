from datetime import datetime, timedelta

from database.common import *


class UnavailableSongError(Exception):
    def __init__(self, *args, song_id=None, song_title=None):
        super().__init__(*args)
        self._song_id = song_id
        self._song_title = song_title

    @property
    def song_id(self):
        return self._song_id

    @property
    def song_title(self):
        return self._song_title


class SongContext:
    __slots__ = ['_dj', '_song', '_title', '_duration', '_url', '_skip_voters', '_all_listeners', '_current_listeners']

    def __init__(self, user_id, song_id, title, duration, url):
        self._dj = user_id
        self._song = song_id
        self._title = title
        self._duration = duration
        self._url = url

        self._skip_voters = set()
        self._all_listeners = set()
        self._current_listeners = set()

    @property
    def song_id(self):
        return self._song

    @property
    def dj_id(self):
        return self._dj

    @property
    def song_title(self):
        return self._title

    @property
    def song_duration(self):
        return self._duration

    @property
    def song_url(self):
        return self._url

    @property
    def listeners(self):
        return self._all_listeners

    def get_current_counts(self):
        return len(self._skip_voters), len(self._skip_voters & self._current_listeners)

    def get_final_sets(self):
        return self._all_listeners, self._skip_voters

    def update_listeners(self, listeners):
        self._all_listeners = self._all_listeners | listeners
        self._current_listeners = listeners

    def skip_unvote(self, user_id):
        self._skip_voters.remove(user_id)

    def skip_vote(self, user_id):
        self._skip_voters.add(user_id)


class PlayerInterface(DBInterface, DBSongUtil):
    def __init__(self, loop, config):
        self._config_ap_threshold = int(config['ap_threshold'])
        self._config_ap_ratio = float(config['ap_skip_ratio'])
        self._config_max_duration = int(config['song_length_limit'])
        self._config_op_interval = int(config['op_interval'])
        DBInterface.__init__(self, loop)

    @in_executor
    def get_next_song(self, user_id):
        song = None
        with self._database.atomic():
            # check if there is an associated playlist
            try:
                playlist = Playlist.select(Playlist.id, Playlist.head, Playlist.repeat) \
                    .join(User, on=(User.active_playlist == Playlist.id)).where(User.id == user_id).get()
            except Playlist.DoesNotExist as e:
                raise LookupError('You don\'t have an active playlist') from e

            if playlist.head is None:
                raise LookupError('Your playlist is empty')

            # join song link and song tables to obtain a result
            link = Link.select(Link, Song).join(Song).where(Link.id == playlist.head).get()
            song = link.song

            # now check if the link should be re-appended or deleted, update the pointers
            if not playlist.repeat:
                # update next song "pointer", should work in any situation
                Playlist.update(head=link.next_id).where(Playlist.id == playlist.id).execute()
                # delete the link
                link.delete_instance()
            elif link.next_id is not None:  # we should repeat and playlist does consist of multiple songs
                # update next song "pointer"
                Playlist.update(head=link.next_id).where(Playlist.id == playlist.id).execute()
                # append the link at the end
                Link.update(next=link.id).where(Link.next >> None, Link.playlist == playlist.id).execute()
                Link.update(next=None).where(Link.id == link.id, Link.playlist == playlist.id).execute()

            # check duplicate song flag and do the replacement if necessary
            if song.duplicate_id is not None:
                song = song.duplicate

        # check the constrains
        # -- blacklist
        if song.is_blacklisted:
            raise RuntimeError('Song [{}] was blacklisted by an operator'.format(song.id))
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
        try:
            result = self._ytdl.extract_info(self._make_url(song.uuri), download=False)
        except youtube_dl.DownloadError as e:  # blacklist the song and raise an exception
            if not song.has_failed:
                log.warning('Download of the song [{}] failed'.format(song.id), exc_info=True)
                Song.update(has_failed=True).where(Song.id == song.id).execute()
            raise UnavailableSongError('Download of the song [{}] failed'.format(song.id), song_id=song.id,
                                       song_title=song.title) from e

        # there is a chance song was marked as failed before but it no longer applies, fix the flag
        if song.has_failed:
            log.info('Failed flag was removed from the song [{}] after a successful download'.format(song.id))
            Song.update(has_failed=False).where(Song.id == song.id).execute()

        return SongContext(user_id, song.id, song.title, song.duration, result['url'])

    @in_executor
    def get_autoplaylist_song(self):
        reference_time = datetime.now() - timedelta(seconds=self._config_op_interval)
        query = Song.select(Song).where(
            Song.last_played < reference_time,  # overplay protection interval
            Song.listener_count >= self._config_ap_threshold,  # listener threshold
            Song.skip_vote_count < peewee.Passthrough(self._config_ap_ratio) * Song.listener_count,  # skip ratio
            Song.duration <= self._config_max_duration,  # song duration
            Song.credit_count > 0,  # overplay protection
            ~Song.is_blacklisted,  # cannot be blacklisted
            ~Song.has_failed,  # probably unavailable
            Song.duplicate >> None  # not fair + outdated information
        ).order_by(peewee.fn.Random())

        try:
            song = query.get()
        except Song.DoesNotExist:
            # there is no song conforming to the automatic playlist conditions
            return None

        try:
            result = self._ytdl.extract_info(self._make_url(song.uuri), download=False)
        except youtube_dl.DownloadError as e:  # blacklist the song and raise an exception
            log.warning('Download of the song [{}] failed'.format(song.id), exc_info=True)
            Song.update(has_failed=True).where(Song.id == song.id).execute()
            raise UnavailableSongError('Download of the song [{}] failed'.format(song.id), song_id=song.id,
                                       song_title=song.title) from e
        return SongContext(None, song.id, song.title, song.duration, result['url'])

    @in_executor
    def update_stats(self, song_ctx: SongContext):
        current_time = datetime.now()
        listeners, skip_voters = song_ctx.get_final_sets()
        # update a song in the database -- listener and skip count, last played, credit count
        song_query = Song.update(listener_count=Song.listener_count + len(listeners),
                                 skip_vote_count=Song.skip_vote_count + len(skip_voters),
                                 last_played=current_time, credit_count=Song.credit_count - 1) \
            .where(Song.id == song_ctx.song_id)
        # update the dj in the database -- play count
        dj_query = User.update(play_count=User.play_count + 1).where(User.id == song_ctx.dj_id)
        # update the listeners in the database -- listen count
        condition = User.id == listeners.pop()
        while listeners:
            condition |= User.id == listeners.pop()
        listener_query = User.update(listen_count=User.listen_count + 1).where(condition)

        with self._database.atomic():
            song_query.execute()
            dj_query.execute()
            listener_query.execute()
