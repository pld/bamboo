from bamboo.config.db import Database
from bamboo.core.frame import BAMBOO_RESERVED_KEYS
from bamboo.lib.decorators import classproperty
from bamboo.lib.mongo import dict_for_mongo, remove_mongo_reserved_keys


class AbstractModel(object):
    """An abstact class for all MongoDB models.

    Attributes:

    - __collection__: The MongoDB collection to communicate with.
    - STATE: A key with which to store state.
    - STATE_PENDING: A value for the pending state.
    - STATE_READY: A value for the ready state.

    """

    __collection__ = None
    __collectionname__ = None

    # delimiter when passing multiple groups as a string
    GROUP_DELIMITER = ','
    ERROR_MESSAGE = 'error_message'

    DB_READ_BATCH_SIZE = 1000
    DB_SAVE_BATCH_SIZE = 2000
    MIN_BATCH_SIZE = 50

    STATE = 'state'
    STATE_FAILED = 'failed'
    STATE_PENDING = 'pending'
    STATE_READY = 'ready'

    @property
    def state(self):
        return self.record[self.STATE]

    @property
    def error_message(self):
        return self.record.get(self.ERROR_MESSAGE)

    @property
    def is_pending(self):
        return self.state == self.STATE_PENDING

    @property
    def is_ready(self):
        return self.state == self.STATE_READY

    @property
    def record_ready(self):
        return self.record is not None and self.state == self.STATE_READY

    @property
    def clean_record(self):
        """Remove reserved keys from records."""
        _dict = {
            key: value for (key, value) in self.record.items() if not key in
            BAMBOO_RESERVED_KEYS
        }
        return remove_mongo_reserved_keys(_dict)

    @classmethod
    def set_collection(cls, collection_name):
        """Return a MongoDB collection for the passed name.

        :param collection_name: The name of collection to return.

        :returns: A MongoDB collection from the current database.
        """
        return Database.db()[collection_name]

    @classproperty
    @classmethod
    def collection(cls):
        """Set the internal collection to the class' collection name."""
        if not cls.__collection__:
            cls.__collection__ = AbstractModel.set_collection(
                cls.__collectionname__)

        return cls.__collection__

    @classmethod
    def create(cls, *args):
        model = cls()
        return model.save(*args)

    @classmethod
    def find(cls, query_args, as_dict=False, as_cursor=False):
        """An interface to MongoDB's find functionality.

        :param query_args: An optional QueryArgs to hold the query arguments.
        :param as_cursor: If true, return the cursor.
        :param as_dict: If true, return dicts and not model instances.

        :returns: A list of dicts or model instances for each row returned.
        """
        cursor = cls.collection.find(query_args.query,
                                     query_args.select,
                                     sort=query_args.order_by,
                                     limit=query_args.limit)

        if as_cursor:
            return cursor
        else:
            return [record for record in cursor] if as_dict else [
                cls(record) for record in cursor
            ]

    @classmethod
    def find_one(cls, query, select=None, as_dict=False):
        """Return the first row matching `query` and `select` from MongoDB.

        :param query: A query to pass to MongoDB.
        :param select: An optional select to pass to MongoDB.
        :param as_dict: If true, return dicts and not model instances.

        :returns: A model instance of the row returned for this query and
            select.
        """
        record = cls.collection.find_one(query, select)
        return record if as_dict else cls(record)

    def __init__(self, record=None):
        """Instantiate with data in `record`."""
        self.record = record

    def __nonzero__(self):
        return self.record is not None

    def failed(self, message=None):
        """Perist the state of the current instance to `STATE_FAILED`.

        :params message: A string store as the error message, default None.
        """
        doc = {self.STATE: self.STATE_FAILED}

        if message:
            doc.update({self.ERROR_MESSAGE: message})

        self.update(doc)

    def pending(self):
        """Perist the state of the current instance to `STATE_PENDING`"""
        self.update({self.STATE: self.STATE_PENDING})

    def ready(self):
        """Perist the state of the current instance to `STATE_READY`"""
        self.update({self.STATE: self.STATE_READY})

    def delete(self, query):
        """Delete rows matching query.

        :param query: The query for rows to delete.
        """
        self.collection.remove(query)

    def save(self, record):
        """Save `record` in this model's collection.

        Save the record in the model instance's collection and set the internal
        record of this instance to the passed in record.

        :param record: The dict to save in the model's collection.

        :returns: The record passed in.
        """
        self.collection.insert(record)
        self.record = record

        return self

    def update(self, record):
        """Update the current instance with `record`.

        Update the current model instance based on its `_id`, set it to the
        passed in `record`.

        :param record: The record to replace the instance's data with.
        """
        record = dict_for_mongo(record)
        id_dict = {'_id': self.record['_id']}
        self.collection.update(id_dict, {'$set': record})

        # Set record to the latest record from the database
        self.record = self.__class__.collection.find_one(id_dict)

    def split_groups(self, groups):
        """Split a string based on the group delimiter"""
        return groups.split(self.GROUP_DELIMITER) if groups else []

    def join_groups(self, groups):
        return self.GROUP_DELIMITER.join(groups)
