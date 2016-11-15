import asyncio
from datetime import datetime, timedelta

from database.common import *


class IgnoredUserError(Exception):
    pass


class BotInterface(DBInterface):
    def __init__(self, loop, config):
        self._config_op_credit_cap = int(config['op_credit_cap'])
        self._config_op_credit_renew = timedelta(hours=int(config['op_credit_renew']))
        DBInterface.__init__(self, loop)

    @in_executor
    def interaction_check(self, user_id):
        user, created = User.get_or_create(id=user_id)
        if user.is_ignored:
            raise IgnoredUserError
        return created

    @in_executor
    def _credit_bump(self, timestamp, count):
        # construct the queries and execute them in a transaction
        ts_query = CreditTimestamp.update(last=timestamp)
        credit_query = Song.update(
            credit_count=peewee.fn.MIN(Song.credit_count + count, self._config_op_credit_cap))
        with self._database.atomic():
            ts_query.execute()
            credit_query.execute()

    async def task_credit_renew(self):
        # check if the last timestamp is present in the database
        if CreditTimestamp.select().count() == 0:
            CreditTimestamp.create(last=datetime.now())

        # now the endless task loop
        while True:
            current_time = datetime.now()
            last_time = CreditTimestamp.get().last
            credits_to_add = (current_time - last_time) // self._config_op_credit_renew
            if credits_to_add > 0:
                # written timestamp correction
                written_timestamp = last_time + (credits_to_add * self._config_op_credit_renew)
                self._credit_bump(written_timestamp, credits_to_add)
            # next check in an hour
            await asyncio.sleep(3600, loop=self._loop)
