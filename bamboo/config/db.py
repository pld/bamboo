from pymongo import MongoClient

from bamboo.config import settings


class Database(object):
    """Container for the MongoDB client."""

    # MongoDB client default host and port
    __client__ = MongoClient('localhost', 27017, w=1, j=False)
    __db__ = None

    @classmethod
    def create_db(cls, db_name):
        """Create a database `db_name`.

        :param cls: The class to set a client on.
        :param db_name: The name of the collection to create.
        """
        cls.__db__ = cls.__client__[db_name]

    @classmethod
    def client(cls):
        """Return the database client."""
        return cls.__client__

    @classmethod
    def db(cls, name=None):
        """Create the database if it has not been created.

        If name is provided, create a new database.

        :param cls: The class to set the database for.
        :param name: The name of the database to create, default None.
        :returns: The created database.
        """
        if not cls.__db__ or name:
            cls.create_db(name or settings.DATABASE_NAME)
        return cls.__db__
