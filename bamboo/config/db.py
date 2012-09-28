from os import getenv

from pymongo import Connection

from config import settings


class Database(object):
    """
    Container for the mongo db connection.
    """

    # mongodb connection default host and port
    _connection = Connection()
    _db = None

    @classmethod
    def create_db(cls, db_name):
        """
        Create a database *db_name* under the *_connection*.
        """
        cls._db = cls._connection[db_name]

    @classmethod
    def connection(cls):
        """
        Return the database *_connection*.
        """
        return cls._connection

    @classmethod
    def db(cls, name=None):
        """
        Create the database if it has not been created.  Then return the
        database.
        """
        if not cls._db or name:
            cls.create_db(name or settings.DATABASE_NAME)
        return cls._db
