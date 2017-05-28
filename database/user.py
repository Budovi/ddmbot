from database.common import *


class UserInterface(DBInterface):
    @in_executor
    def info(self, user_id):
        # interesting info: play count, number of playlists, number of songs and if user is blacklisted
        try:
            user = User.get(User.id == user_id)
        except User.DoesNotExist as e:
            raise ValueError('User is not in the database') from e

        with self._database.atomic():
            playlist_count = Playlist.select().where(Playlist.user == user_id).count()
            song_count = Link.select().join(Playlist, on=(Link.playlist == Playlist.id)) \
                .where(Playlist.user == user.id).count()

        return {'play_count': user.play_count, 'listen_count': user.listen_count, 'playlist_count': playlist_count,
                'song_count': song_count, 'ignored': user.is_ignored}

    @in_executor
    def ignore(self, user_id):
        # we can technically ignore user that is not in the database yet
        user, created = User.get_or_create(id=user_id, defaults={'is_ignored': True})
        if created:
            log.warning('Ignoring user {} that is not in the database'.format(user_id))
        else:
            if user.is_ignored:
                raise ValueError('User is on the ignore list already')
            User.update(is_ignored=True).where(User.id == user_id).execute()

    @in_executor
    def grace(self, user_id):
        if User.update(is_ignored=False).where(User.id == user_id, User.is_ignored).execute() != 1:
            raise ValueError('User is not on the ignore list')
