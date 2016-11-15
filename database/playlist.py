import random

from database.common import *


class PlaylistInterface(DBInterface, DBPlaylistUtil):
    def __init__(self, loop, config):
        self._config_max_playlists = int(config['playlist_count_limit'])
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

        result = list()
        with self._database.atomic():
            total = Link.select().where(Link.playlist == playlist.id).count()
            current_link_id = playlist.head_id
            for index in range(limit + offset):
                if current_link_id is None:
                    break
                link = Link.select(Link.next, Song.id, Song.title).join(Song).where(Link.id == current_link_id) \
                    .tuples().get()
                if index >= offset:
                    result.append(link[1:])
                current_link_id = link[0]

        return result, playlist.name, total

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
    def remove(self, user_id, playlist_name=None):
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
