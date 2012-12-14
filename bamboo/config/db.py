from os import getenv

from pymongo import Connection

from bamboo.config import settings


class Database(object):
    """Container for the mongo db connection."""

    # mongodb connection default host and port
    _connection = Connection()
    _db = None

    @classmethod
    def create_db(cls, db_name):
        """Create a database `db_name` under the `_connection`.

        :param cls: The class to set a connection on.
        :param db_name: The name of the collection to create.
        """
        cls._db = cls._connection[db_name]

    @classmethod
    def connection(cls):
        """Return the database `_connection`."""
        return cls._connection

    @classmethod
    def db(cls, name=None):
        """Create the database if it has not been created.

        :param cls: The class to set the database for.
        :param name: The name of the database to create.
        :returns: The created database.
        """
        # TODO why is this or?
        if not cls._db or name:
            cls.create_db(name or settings.DATABASE_NAME)
        return cls._db
