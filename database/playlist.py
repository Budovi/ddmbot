import random
from collections import deque
from datetime import datetime

from database.common import *


class SongUriProcessor(DBSongUtil):
    def __init__(self, database, credit_cap, uris, *, reverse=False):
        self._database = database
        self._credit_cap = credit_cap
        self._uris = deque(uris)
        self._reverse = reverse

    def __iter__(self):
        return self

    def _get_song(self, song_url):
        song_uuri = self._make_uuri(song_url)
        if not song_uuri:
            raise ValueError('Malformed URL or unsupported service')
        # potentially the first query of the song
        try:
            song = Song.get(Song.uuri == song_uuri)
        except Song.DoesNotExist:
            # we need to create a new record, youtube_dl is necessary to obtain a title and a song length
            result = self._ytdl.extract_info(self._make_url(song_uuri), download=False, process=False)
            try:
                title = result['title']
            except KeyError as e:
                raise RuntimeError('Failed to extract song title') from e
            try:
                duration = int(result['duration'])
            except (KeyError, ValueError) as e:
                raise RuntimeError('Failed to extract song duration') from e
            # since the song may be about to be added multiple times, check again and insert atomically
            with self._database.atomic():
                try:
                    song = Song.create(uuri=song_uuri, title=title, last_played=datetime.utcfromtimestamp(0),
                                       duration=duration, credit_count=self._credit_cap)
                except peewee.IntegrityError:
                    song = Song.get(Song.uuri == song_uuri)
        return song

    def __next__(self):
        # get a new item
        try:
            uri = self._uris.pop() if self._reverse else self._uris.popleft()
        except IndexError as e:
            raise StopIteration from e
        # check if song id
        if uri.isdigit():
            try:
                return Song.get(Song.id == int(uri))
            except Song.DoesNotExist as e:
                raise RuntimeError('Song [{}] cannot be found in the database'.format(uri)) from e
        # it can be a list otherwise
        if self._is_list(uri):
            try:
                result = self._ytdl.extract_info(uri, download=False)
                if 'entries' not in result:
                    raise RuntimeError('Malformed URL or unsupported service')
                # create a new uri list from the results
                if result['extractor'] == 'youtube:playlist':
                    list_uris = [self._url_base['yt'].format(entry['id']) for entry in result['entries']]
                else:
                    list_uris = [entry['url'] for entry in result['entries']]
                # put back most of them and keep the first one
                if self._reverse:
                    self._uris.extend(list_uris[:-1])
                    uri = list_uris[-1]
                else:
                    self._uris.extendleft(list_uris[:0:-1])
                    uri = list_uris[0]
            except Exception as e:
                raise RuntimeError('Processing `{}` failed: {}'.format(uri, str(e))) from e
        # now we have a single song, hopefully at least
        try:
            return self._get_song(uri)
        except Exception as e:
            raise RuntimeError('Processing `{}` failed: {}'.format(uri, str(e))) from e


class PlaylistInterface(DBInterface, DBPlaylistUtil):
    def __init__(self, loop, config):
        self._config_max_playlists = int(config['playlist_count_limit'])
        self._config_max_songs = int(config['song_count_limit'])
        self._config_op_credit_cap = int(config['op_credit_cap'])
        DBInterface.__init__(self, loop)

    @in_executor
    def exists(self, user_id, playlist_name):
        try:
            self._get_playlist(user_id, playlist_name)
        except KeyError:
            return False
        return True

    @in_executor
    def get_active(self, user_id):
        try:
            playlist = Playlist.select(Playlist).join(User, on=(User.active_playlist == Playlist.id)) \
                .where(User.id == user_id).get()
        except Playlist.DoesNotExist as e:
            raise LookupError('You don\'t have an active playlist') from e
        return playlist.name

    @in_executor
    def set_active(self, user_id, playlist_name):
        with self._database.atomic():
            playlist = self._get_playlist(user_id, playlist_name)
            User.update(active_playlist=playlist.id).where(User.id == user_id).execute()

    @in_executor
    def create(self, user_id, playlist_name):
        # do some preliminary name checks
        if len(playlist_name) > 32:
            raise ValueError('Playlist name must be 32 characters long or shorter')
        if not self._playlist_regex.match(playlist_name):
            raise ValueError('Playlist name may only contain alphanumeric characters, dashes and underscores')

        with self._database.atomic():
            # check for the limit
            if Playlist.select().where(Playlist.user == user_id).count() >= self._config_max_playlists:
                raise RuntimeError('You\'ve reached the maximum number of playlists allowed')
            # if it's ok, create new playlist, integrity error means a playlist with the same name already exists
            try:
                Playlist.create(user=user_id, name=playlist_name)
            except peewee.IntegrityError as e:
                raise ValueError('You already have a playlist with the chosen name'.format(playlist_name)) from e

    @in_executor
    def clear(self, user_id, playlist_name):
        with self._database.atomic():
            playlist, created = self._get_playlist_ex(user_id, playlist_name=playlist_name)

            Playlist.update(head=None).where(Playlist.id == playlist.id).execute()
            Link.delete().where(Link.playlist == playlist.id).execute()

        return playlist.name

    @in_executor
    def list(self, user_id):
        query = Playlist.select(Playlist.name, peewee.fn.COUNT(Link.id).alias('song_count'), Playlist.repeat) \
            .join(Link, join_type=peewee.JOIN_LEFT_OUTER, on=(Link.playlist == Playlist.id)) \
            .where(Playlist.user == user_id).group_by(Playlist.name)

        return list(query.dicts())

    @in_executor
    def show(self, user_id, offset, limit, playlist_name):
        with self._database.atomic():
            playlist, created = self._get_playlist_ex(user_id, playlist_name=playlist_name)

            total = Link.select().where(Link.playlist == playlist.id).count()
            query = Song.raw('WITH RECURSIVE cte (id, title, next) AS ('
                             'SELECT song.id, song.title, link.next_id FROM song JOIN link ON song.id == link.song_id '
                             '  WHERE link.id == ? '
                             'UNION ALL '
                             'SELECT song.id, song.title, link.next_id FROM cte, song JOIN link '
                             '  ON song.id == link.song_id WHERE link.id == cte.next) '
                             'SELECT id, title FROM cte LIMIT ? OFFSET ?;', playlist.head_id, limit, offset)
            songs = list(query.tuples())

        return songs, playlist.name, total

    @in_executor
    def shuffle(self, user_id, playlist_name):
        with self._database.atomic():
            playlist, created = self._get_playlist_ex(user_id, playlist_name=playlist_name)
            query = Link.select().where(Link.playlist == playlist.id)
            song_list = list()

            for item in query:
                song_list.append(item.song_id)
            random.shuffle(song_list)
            for item, new_id in zip(query, song_list):
                item.song_id = new_id
                item.save()

        return playlist.name

    @in_executor
    def delete(self, user_id, playlist_name):
        with self._database.atomic():
            playlist, created = self._get_playlist_ex(user_id, playlist_name=playlist_name)
            User.update(active_playlist=None).where(User.id == user_id, User.active_playlist == playlist.id).execute()
            Link.delete().where(Link.playlist == playlist.id).execute()
            playlist.delete_instance()

        return playlist.name

    @in_executor
    def repeat(self, user_id, repeat, playlist_name):
        with self._database.atomic():
            playlist, created = self._get_playlist_ex(user_id, playlist_name=playlist_name)
            Playlist.update(repeat=repeat).where(Playlist.id == playlist.id).execute()

        return playlist.name

    @in_executor
    def insert(self, user_id, playlist_name, prepend, uris):
        # we will return a log of messages
        messages = list()

        # get a playlist
        playlist, created = self._get_playlist_ex(user_id, playlist_name=playlist_name, create_default=True)
        if created:
            messages.append('Since you haven\'t had any playlist, a *default* one was created for you. Note that songs '
                            'will be removed from it after playing.')
        # construct some iterator object from uris
        song_list = SongUriProcessor(self._database, self._config_op_credit_cap, uris, reverse=prepend)

        # compose "already present message"
        present_message = 'The song [{}] {} was already present in your playlist.'
        if prepend:
            present_message += ' It was moved to the front.'

        # counters
        inserted = 0
        failed = 0
        while True:
            # get a next song to append
            try:
                song = next(song_list)
            except StopIteration:
                return playlist.name, inserted, failed, False, messages
            except Exception as e:
                # append an error to the list
                messages.append(str(e))
                failed += 1
                continue

            # now insert it
            try:
                result = self._prepend_song(user_id, song.id, playlist.name) if prepend \
                    else self._append_song(user_id, song.id, playlist.name)

                if result:
                    inserted += 1
                else:  # song was in the playlist already
                    messages.append(present_message.format(song.id, song.title))
            except Exception as e:
                # either reached the limit, or the playlist does not exist anymore -- either case we end return
                messages.append(str(e))
                failed += 1
                return playlist.name, inserted, failed, True, messages

    @in_executor
    def pop(self, user_id, count, playlist_name):
        if count <= 0:
            return 0

        with self._database.atomic():
            # get the target playlist
            playlist, created = self._get_playlist_ex(user_id, playlist_name=playlist_name)

            deleted = 0
            # remove *count* links
            current_link = playlist.head_id
            while deleted < count and current_link is not None:
                next_link = Link.select(Link.next).where(Link.id == current_link).get().next_id
                Link.delete().where(Link.id == current_link).execute()
                current_link = next_link
                deleted += 1
            # update the playlist head
            Playlist.update(head=current_link).where(Playlist.id == playlist.id).execute()

        return playlist.name, deleted

    @in_executor
    def pop_id(self, user_id, song_id, playlist_name):
        with self._database.atomic():
            # get the target playlist
            playlist, created = self._get_playlist_ex(user_id, playlist_name=playlist_name)

            # find the target link
            try:
                target_link = Link.get(Link.playlist == playlist.id, Link.song == song_id)
            except Link.DoesNotExist as e:
                raise LookupError('Specified song was not found in your playlist') from e

            # find the previous link to break up the chain
            try:
                previous_link = Link.get(Link.next == target_link.id)
                Link.update(next=target_link.next_id).where(Link.id == previous_link.id).execute()
            except Link.DoesNotExist:
                # the song is in the front of the queue
                Playlist.update(head=target_link.next_id).where(Playlist.id == playlist.id).execute()

            # finally, delete the target link
            target_link.delete_instance()

        return playlist.name

    #
    # Internally used methods
    #
    def _append_song(self, user_id, song_id, playlist_name):
        with self._database.atomic():
            # get a playlist
            playlist = self._get_playlist(user_id, playlist_name)

            # check for duplicates, if present just return
            if Link.select().where(Link.playlist == playlist.id, Link.song == song_id).count():
                return False
            # check for the song count limit
            count = Link.select().join(Playlist, on=(Link.playlist == Playlist.id)).where(Playlist.user == user_id) \
                .count()
            if count >= self._config_max_songs:
                raise RuntimeError('You\'ve reached the song count limit for your playlists')
            # insert a new link
            link = Link.create(playlist=playlist.id, song=song_id, next=None)
            # modify the previous link to point to the new one
            if playlist.head_id is None:
                Playlist.update(head=link.id).where(Playlist.id == playlist.id).execute()
            else:
                Link.update(next=link.id).where(Link.playlist == playlist.id,
                                                Link.next >> None, Link.id != link.id).execute()
            return True

    def _prepend_song(self, user_id, song_id, playlist_name):
        with self._database.atomic():
            # get a playlist
            playlist = self._get_playlist(user_id, playlist_name)

            try:
                # if there is a duplicate, we won't insert a new link
                duplicate = Link.get(Link.playlist == playlist.id, Link.song == song_id)
                # we will reuse the link and push it to the front
                if Link.update(next=duplicate.next_id).where(Link.next == duplicate.id).execute():
                    # we are not the first link if the statement above modified something
                    Link.update(next=playlist.head_id).where(Link.id == duplicate.id).execute()
                    Playlist.update(head=duplicate.id).where(Playlist.id == playlist.id).execute()
                return False
            except Link.DoesNotExist:  # can be only raised by the previous Link.get()
                # do the "normal insert" -- we need to check for length in this case
                count = Link.select().join(Playlist, on=(Link.playlist == Playlist.id)) \
                    .where(Playlist.user == user_id).count()
                if count >= self._config_max_songs:
                    raise RuntimeError('You\'ve reached the song count limit for your playlists')

                link = Link.create(playlist=playlist.id, song=song_id, next=playlist.head_id)
                Playlist.update(head=link.id).where(Playlist.id == playlist.id).execute()
                return True
