from database.common import *


class SongInterface(DBInterface, DBSongUtil):
    def __init__(self, loop):
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
        except Song.DoesNotExist as e:
            raise ValueError('Song [{}] cannot be found in the database'.format(song_id)) from e
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
                except Song.DoesNotExist as e:
                    raise ValueError('Song [{}] cannot be found in the database'.format(target_id)) from e

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
