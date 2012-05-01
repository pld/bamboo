from os import getenv

from pymongo import Connection

from config import settings


class Database(object):
    'Container for mongo db connection'

    # mongodb connection default host and port
    _connection = Connection()
    _db = None

    @classmethod
    def create_db(cls, db_name):
        cls._db = cls._connection[db_name]

    @classmethod
    def connection(cls):
        return cls._connection

    @classmethod
    def db(cls, name=None):
        if not cls._db or name:
            cls.create_db(name or settings.database_name)
        return cls._db
