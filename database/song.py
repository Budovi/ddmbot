from datetime import datetime

from database.common import *


class SongInterface(DBInterface, DBSongUtil, DBPlaylistUtil):
    def __init__(self, loop, config):
        self._config_max_songs = int(config['song_count_limit'])
        self._config_op_credit_cap = int(config['op_credit_cap'])
        DBInterface.__init__(self, loop)

    #
    # Interface methods
    #
    @in_executor
    def blacklist(self, song_id):
        if Song.update(is_blacklisted=True).where(Song.id == song_id, ~Song.is_blacklisted).execute() != 1:
            raise ValueError('Song [{}] does not exist or is blacklisted already'.format(song_id))

    @in_executor
    def permit(self, song_id):  # intentionally kept as an instance method
        if Song.update(is_blacklisted=False).where(Song.id == song_id, Song.is_blacklisted).execute() != 1:
            raise ValueError('Song [{}] does not exist or is not blacklisted'.format(song_id))

    @in_executor
    def search(self, keywords, limit):
        query = Song.select(Song.id, Song.title)
        for keyword in keywords:
            keyword = '%{}%'.format(keyword)
            query = query.where((Song.title ** keyword) | (Song.uuri ** keyword))
        total = query.count()
        result = list()
        for row in query.limit(limit):
            result.append((row.id, row.title))
        return result, total

    @in_executor
    def get_info(self, song_id):
        try:
            result = Song.select().where(Song.id == song_id).dicts().get()
        except Song.DoesNotExist:
            raise ValueError('Song [{}] cannot be found in the database'.format(song_id))
        # put url instead of unique uri into the result dictionary
        result['url'] = self._make_url(result.pop('uuri'))
        # remove duplicated_id
        duplicate_id = result.pop('duplicate')
        # add total counts
        result['total_listener_count'] = result['listener_count']
        result['total_skip_vote_count'] = result['skip_vote_count']
        # handle duplicates
        result['duplicates'] = None
        result['duplicated_by'] = list()

        if duplicate_id is not None:
            song = Song.select(Song.id, Song.title, Song.listener_count, Song.skip_vote_count) \
                .where(Song.id == duplicate_id).tuples().get()
            result['duplicates'] = song[0:2]
            result['total_listener_count'] += song[2]
            result['total_skip_vote_count'] += song[3]

        duplicate_query = Song.select(Song.id, Song.title, Song.listener_count, Song.skip_vote_count) \
            .where(Song.duplicate == song_id).tuples()
        for song in duplicate_query:
            result['duplicated_by'].append(song[0:2])
            result['total_listener_count'] += song[2]
            result['total_skip_vote_count'] += song[3]

        return result

    @in_executor
    def merge(self, source_id, target_id):
        if source_id == target_id:
            # this is effectively a "split" call
            if Song.update(duplicate=None).where(Song.id == source_id).execute() != 1:
                raise ValueError('Song [{}] cannot be found in the database'.format(source_id))
        else:
            with self._database.atomic():
                try:
                    target_song = Song.get(Song.id == target_id)
                except Song.DoesNotExist:
                    raise ValueError('Song [{}] cannot be found in the database'.format(target_id))

                if target_song.duplicate_id == source_id:
                    # we're "reassigning" the duplicate flags
                    target_song.duplicate_id = None
                    target_song.save()
                elif target_song.duplicate_id is not None:
                    # if a target is duplicate, we will update to duplicate_id instead
                    target_id = target_song.duplicate_id
                if Song.update(duplicate=target_id).where(
                                (Song.id == source_id) | (Song.duplicate == source_id)).execute() == 0:
                    raise ValueError('Song [{}] cannot be found in the database'.format(source_id))

    @in_executor
    def rename(self, song_id, new_title):
        if Song.update(title=new_title).where(Song.id == song_id).execute() != 1:
            raise ValueError('Song [{}] cannot be found in the database'.format(song_id))

    @in_executor
    def list_failed(self, limit):
        query = Song.select(Song.id, Song.title).where(Song.has_failed, Song.duplicate >> None)
        total = query.count()
        result = list()
        for song in query.limit(limit):
            result.append((song.id, song.title))
        return result, total

    @in_executor
    def clear_failed(self, song_id):
        query = Song.update(has_failed=False)
        if song_id is not None:
            # apply only to a song specified
            if query.where(Song.id == song_id).execute() != 1:
                raise ValueError('Song [{}] cannot be found in the database'.format(song_id))
        else:
            # clear the flag for all the songs
            query.where(Song.duplicate >> None).execute()

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
    def delete(self, user_id, song_id, playlist_name=None):
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
