import random
from datetime import datetime


from database.common import *


class PlaylistInterface(DBInterface, DBPlaylistUtil, DBSongUtil):
    def __init__(self, loop, config):
        self._config_max_playlists = int(config['playlist_count_limit'])
        self._config_max_songs = int(config['song_count_limit'])
        self._config_op_credit_cap = int(config['op_credit_cap'])
        DBInterface.__init__(self, loop)

    @in_executor
    def get_active(self, user_id):
        playlist, created = self._get_active_playlist(user_id)
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
                playlist = Playlist.create(user=user_id, name=playlist_name)
            except Playlist.IntegrityError:
                raise ValueError('You already have a playlist with the chosen name'.format(playlist_name))
            User.update(active_playlist=playlist.id).where(User.id == user_id).execute()

    @in_executor
    def clear(self, user_id, playlist_name=None):
        if playlist_name is None:
            playlist, created = self._get_active_playlist(user_id)
        else:
            playlist = self._get_playlist(user_id, playlist_name)

        update_query = Playlist.update(head=None).where(Playlist.id == playlist.id)
        delete_query = Link.delete().where(Link.playlist == playlist.id)
        with self._database.atomic():
            update_query.execute()
            delete_query.execute()

        return playlist.name

    @in_executor
    def list(self, user_id):
        query = Playlist.select(Playlist.name, peewee.fn.COUNT(Link.id).alias('song_count'), Playlist.repeat) \
            .join(Link, join_type=peewee.JOIN_LEFT_OUTER, on=(Link.playlist == Playlist.id)) \
            .where(Playlist.user == user_id).group_by(Playlist.name).dicts()

        return list(query)

    @in_executor
    def show(self, user_id, offset, limit, playlist_name=None):
        if playlist_name is None:
            playlist, created = self._get_active_playlist(user_id)
        else:
            playlist = self._get_playlist(user_id, playlist_name)

        with self._database.atomic():
            total = Link.select().where(Link.playlist == playlist.id).count()
            query = Song.raw('WITH RECURSIVE cte (id, title, next) AS ('
                             'SELECT song.id, song.title, link.next_id FROM song JOIN link ON song.id == link.song_id '
                             '  WHERE link.id == ? '
                             'UNION ALL '
                             'SELECT song.id, song.title, link.next_id FROM cte, song JOIN link '
                             '  ON song.id == link.song_id WHERE link.id == cte.next) '
                             'SELECT id, title FROM cte LIMIT ? OFFSET ?;', playlist.head_id, limit, offset)

        return list(query.tuples()), playlist.name, total

    @in_executor
    def shuffle(self, user_id, playlist_name=None):
        if playlist_name is None:
            playlist, created = self._get_active_playlist(user_id)
        else:
            playlist = self._get_playlist(user_id, playlist_name)

        query = Link.select().where(Link.playlist == playlist.id)
        song_list = list()

        with self._database.atomic():
            for item in query:
                song_list.append(item.song_id)
            random.shuffle(song_list)
            for item, new_id in zip(query, song_list):
                item.song_id = new_id
                item.save()

        return playlist.name

    @in_executor
    def switch(self, user_id, playlist_name):
        with self._database.atomic():
            playlist = self._get_playlist(user_id, playlist_name)
            User.update(active_playlist=playlist.id).where(User.id == user_id).execute()

    @in_executor
    def delete(self, user_id, playlist_name=None):
        if playlist_name is None:
            playlist, created = self._get_active_playlist(user_id)
        else:
            playlist = self._get_playlist(user_id, playlist_name)

        update_query = User.update(active_playlist=None).where(User.id == user_id, User.active_playlist == playlist.id)
        link_delete_query = Link.delete().where(Link.playlist == playlist.id)
        with self._database.atomic():
            update_query.execute()
            link_delete_query.execute()
            playlist.delete_instance()

        return playlist.name

    @in_executor
    def repeat(self, user_id, repeat, playlist_name):
        playlist = self._get_playlist(user_id, playlist_name)
        Playlist.update(repeat=repeat).where(Playlist.id == playlist.id).execute()

        return playlist_name

    @in_executor
    def append(self, user_id, uris, playlist_name=None):
        # we will return a dictionary
        result = dict()
        result['created_playlist'] = False

        # check the song limit
        count = Link.select().join(Playlist, on=(Link.playlist == Playlist.id)).where(Playlist.user == user_id).count()
        if count >= self._config_max_songs:
            raise RuntimeError('You\'ve reached the song count limit for your playlists')

        # assembly the list of songs for insertion
        song_list, result['error_list'], result['truncated'] = self._process_uris(uris, self._config_max_songs - count)

        # now create the links in the database
        with self._database.atomic():
            # atomically re-check the condition for song count
            to_insert = self._config_max_songs - Link.select().join(Playlist, on=(Link.playlist == Playlist.id)) \
                .where(Playlist.user == user_id).count()
            if to_insert <= 0:
                raise RuntimeError('You\'ve reached the song count limit for your playlists')

            # get the target playlist
            if playlist_name is None:
                playlist, result['created_playlist'] = self._get_active_playlist(user_id, create_default=True)
            else:
                playlist = self._get_playlist(user_id, playlist_name)

            # store a connection point
            connection_point = None
            if playlist.head_id is not None:
                connection_point = Link.get(Link.playlist == playlist.id, Link.next >> None)
            # create a chain of songs to append
            previous_link = None
            for song in song_list[to_insert - 1::-1]:
                previous_link = Link.create(playlist=playlist.id, song=song.id, next=previous_link).id
            # connect the chain created
            if connection_point is not None:
                connection_point.next_id = previous_link
                connection_point.save()
            else:
                Playlist.update(head=previous_link).where(Playlist.id == playlist.id).execute()

        result['truncated'] |= len(song_list) > to_insert
        result['inserted'] = min(len(song_list), to_insert)

        return result

    @in_executor
    def prepend(self, user_id, uris, playlist_name=None):
        # we will return a dictionary
        result = dict()
        result['created_playlist'] = False

        # check the song limit
        count = Link.select().join(Playlist, on=(Link.playlist == Playlist.id)).where(Playlist.user == user_id).count()
        if count >= self._config_max_songs:
            raise RuntimeError('You\'ve reached the song count limit for your playlists')

        # assembly the list of songs for insertion
        song_list, result['error_list'], result['truncated'] = self._process_uris(uris, self._config_max_songs - count)

        # now create the links in the database
        with self._database.atomic():
            # atomically re-check the condition for song count
            to_insert = self._config_max_songs - Link.select().join(Playlist, on=(Link.playlist == Playlist.id)) \
                .where(Playlist.user == user_id).count()
            if to_insert <= 0:
                raise RuntimeError('You\'ve reached the song count limit for your playlists')

            # get the target playlist
            if playlist_name is None:
                playlist, result['created_playlist'] = self._get_active_playlist(user_id, create_default=True)
            else:
                playlist = self._get_playlist(user_id, playlist_name)

            # create a song chain to prepend
            previous_link = playlist.head_id
            for song in song_list[to_insert - 1::-1]:
                previous_link = Link.create(playlist=playlist.id, song=song.id, next=previous_link).id
            # connect the chain created
            Playlist.update(head=previous_link).where(Playlist.id == playlist.id).execute()

        result['truncated'] |= len(song_list) > to_insert
        result['inserted'] = min(len(song_list), to_insert)

        return result

    @in_executor
    def pop(self, user_id, count, playlist_name=None):
        if count <= 0:
            return 0

        with self._database.atomic():
            # get the target playlist
            if playlist_name is None:
                playlist, created = self._get_active_playlist(user_id)
            else:
                playlist = self._get_playlist(user_id, playlist_name)

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

        return deleted

    @in_executor
    def pop_id(self, user_id, song_id, playlist_name=None):
        deleted = 0
        with self._database.atomic():
            # get the target playlist
            if playlist_name is None:
                playlist, created = self._get_active_playlist(user_id)
            else:
                playlist = self._get_playlist(user_id, playlist_name)

            # first cycle needs to handle the 'head pointer'
            if playlist.head_id is None:
                return deleted
            current_link = Link.get(Link.id == playlist.head_id)
            while current_link is not None and current_link.song_id == song_id:
                next_link = current_link.next
                current_link.delete_instance()
                current_link = next_link
                deleted += 1
            if current_link is None:
                # playlist remained empty
                Playlist.update(head=None).where(Playlist.id == playlist.id).execute()
                return deleted
            # playlist won't be empty
            Playlist.update(head=current_link.id).where(Playlist.id == playlist.id).execute()

            # do a second loop which is "link-only" thus better
            previous_link = current_link
            current_link = current_link.next
            while current_link is not None:
                next_link = current_link.next
                if current_link.song_id == song_id:
                    # we will need to delete the current link
                    Link.update(next=current_link.next_id).where(Link.id == previous_link.id).execute()
                    current_link.delete_instance()
                    deleted += 1
                else:
                    previous_link = current_link
                current_link = next_link

        return deleted

    #
    # Internally used methods
    #
    def _get_song(self, song_url):
        song_uuri = self._make_uuri(song_url)
        if not song_uuri:
            raise ValueError('Malformed URL or unsupported service: {}'.format(song_url))
        # potentially the first query of the song
        try:
            song = Song.get(Song.uuri == song_uuri)
        except Song.DoesNotExist:
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
            song, created = Song.create_or_get(uuri=song_uuri, title=title,
                                               last_played=datetime.utcfromtimestamp(0),
                                               duration=duration, credit_count=self._config_op_credit_cap)
        return song

    def _process_uris(self, uris, limit):
        song_list = list()
        error_list = list()
        for uri in uris:
            if len(song_list) >= limit:
                return song_list, error_list, True
            if uri.isdigit():  # test if it's a plain integer -- we will assume it's an unique URI
                try:
                    song_list.append(Song.get(id=int(uri)))
                except Song.DoesNotExist:
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
                        except youtube_dl.DownloadError as e:
                            error_list.append(
                                'Inserting `{}` from playlist failed: {}'.format(entry['url'], str(e)))
                except youtube_dl.DownloadError as e:
                    error_list.append('Processing list `{}` failed: {}'.format(uri, str(e)))
            else:  # should be a single song
                try:  # youtube_dl or regex matching can fail
                    song_list.append(self._get_song(uri))
                except ValueError as e:
                    error_list.append(str(e))
                except youtube_dl.DownloadError as e:
                    error_list.append('Inserting `{}` failed: {}'.format(uri, str(e)))

        return song_list, error_list, False
