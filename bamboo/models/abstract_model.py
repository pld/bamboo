from math import ceil

from bamboo.config.db import Database
from bamboo.config.settings import DB_SAVE_BATCH_SIZE
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

    STATE = 'state'
    STATE_FAILED = 'failed'
    STATE_PENDING = 'pending'
    STATE_READY = 'ready'

    @property
    def state(self):
        return self.record[self.STATE]

    @property
    def is_ready(self):
        return self.state == self.STATE_READY

    @classmethod
    def set_collection(cls, collection_name):
        """Return a MongoDB collection for the passed name.

        :param collection_name: The name of collection to return.

        :returns: A MongoDB collection from the current database.
        """
        return Database.db()[collection_name]

    def failed(self):
        """Perist the state of the current instance to `STATE_FAILED`"""
        self.update({self.STATE: self.STATE_FAILED})

    def pending(self):
        """Perist the state of the current instance to `STATE_PENDING`"""
        self.update({self.STATE: self.STATE_PENDING})

    def ready(self):
        """Perist the state of the current instance to `STATE_READY`"""
        self.update({self.STATE: self.STATE_READY})

    @classproperty
    @classmethod
    def collection(cls):
        """Set the internal collection to the class' collection name."""
        if not cls.__collection__:
            cls.__collection__ = AbstractModel.set_collection(
                cls.__collectionname__)

        return cls.__collection__

    @classmethod
    def find(cls, query, select=None, distinct=None, as_dict=False,
             limit=0, order_by=None, as_cursor=False):
        """An interface to MongoDB's find functionality.

        :param query: A query to pass to MongoDB.
        :param select: An optional select statement to pass to MongoDB.
        :param as_dict: If true, return dicts and not model instances.
        :param limit: Limit on the number of rows returned.
        :param order_by: Sort resulting rows according to a column value and
            sign indicating ascending or descending.

        Examples of `order_by`:

          - ``order_by='mycolumn'``
          - ``order_by='-mycolumn'``

        :returns: A list of dicts or model instances for each row returned.
        """
        if order_by:
            if order_by[0] in ('-', '+'):
                sort_dir, field = -1 if order_by[0] == '-' else 1, order_by[1:]
            else:
                sort_dir, field = 1, order_by
            order_by = [(field, sort_dir)]

        cursor = cls.collection.find(
            query, select, sort=order_by, limit=limit)

        if as_cursor:
            return cursor
        else:
            return [record for record in cursor] if as_dict else [
                cls(record) for record in cursor
            ]

    @classmethod
    def find_one(cls, query, select=None):
        """Return the first row matching `query` and `select` from MongoDB.

        :param query: A query to pass to MongoDB.
        :param select: An optional select to pass to MongoDB.

        :returns: A model instance of the row returned for this query and
            select.
        """
        return cls(cls.collection.find_one(query, select))

    def __init__(self, record=None):
        """Instantiate with data in `record`."""
        self.record = record

    def __nonzero__(self):
        return self.record is not None

    @property
    def clean_record(self):
        """Remove reserved keys from records."""
        _dict = {
            key: value for (key, value) in self.record.items() if not key in
            BAMBOO_RESERVED_KEYS
        }
        return remove_mongo_reserved_keys(_dict)

    def create(self):
        model = self.__class__()
        model.save()
        return model

    def delete(self, query):
        """Delete rows matching query.

        :param query: The query for rows to delete.
        """
        self.collection.remove(query, safe=True)

    def save(self, record):
        """Save `record` in this model's collection.

        Save the record in the model instance's collection and set the internal
        record of this instance to the passed in record.

        :param record: The dict to save in the model's collection.

        :returns: The record passed in.
        """
        self.collection.insert(record, safe=True)
        self.record = record
        return record

    def update(self, record):
        """Update the current instance with `record`.

        Update the current model instance based on its `_id`, set it to the
        passed in `record`.

        :param record: The record to replace the instance's data with.
        """
        record = dict_for_mongo(record)
        id_dict = {'_id': self.record['_id']}
        self.collection.update(id_dict, {'$set': record}, safe=True)

        # Set record to the latest record from the database
        self.record = self.__class__.collection.find_one(id_dict)

    def batch_save(self, dframe):
        """Save records in batches to avoid document size maximum setting.

        :param dframe: A DataFrame to save in the current model.
        """
        def command(records):
            self.collection.insert(records, safe=True)

        self._batch_command(command, dframe)

    def _batch_command(self, command, dframe):
        batches = int(ceil(float(len(dframe)) / DB_SAVE_BATCH_SIZE))

        for batch in xrange(0, batches):
            start = batch * DB_SAVE_BATCH_SIZE
            end = (batch + 1) * DB_SAVE_BATCH_SIZE
            records = [
                row.to_dict() for (_, row) in dframe[start:end].iterrows()]
            command(records)
