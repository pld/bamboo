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

    STATE = 'status'
    STATE_PENDING = 'pending'
    STATE_READY = 'ready'

    @property
    def status(self):
        return self.record[self.STATE]

    @property
    def is_ready(self):
        return self.status == self.STATE_READY

    @classmethod
    def set_collection(cls, collection_name):
        """Return a MongoDB collection for the passed name.

        Args:

        - collection_name: The name of collection to return.

        Returns:
            A MongoDB collection from the current database.
        """
        return Database.db()[collection_name]

    def ready(self):
        """Perist the state of the current instance to STATE_READY"""
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
    def find(cls, query, select=None, as_dict=False,
             limit=0, order_by=None, as_cursor=False):
        """An interface to MongoDB's find functionality.

        Args:

        - query: A query to pass to MongoDB.
        - select: An optional select statement to pass to MongoDB.
        - as_dict: If true, return dicts and not model instances.
        - limit: Limit on the number of rows returned.
        - order_by: Sort resulting rows according to a column value and sign
          indicating ascending or descending. For example:

          - ``order_by='mycolumn'``
          - ``order_by='-mycolumn'``

        Returns:
            A list of dicts or model instances for each row returned.
        """
        if order_by:
            if order_by[0] in ('-', '+'):
                sort_dir, field = -1 if order_by[0] == '-' else 1, order_by[1:]
            else:
                sort_dir, field = 1, order_by
            order_by = [(field, sort_dir)]

        records = cls.collection.find(
            query, select, sort=order_by, limit=limit)

        if as_cursor:
            return records
        else:
            return [record for record in records] if as_dict else [
                cls(record) for record in records
            ]

    @classmethod
    def find_one(cls, query, select=None):
        """Return the first row matching *query* and *select* from MongoDB.

        Args:

        - query: A query to pass to MongoDB.
        - select: An optional select to pass to MongoDB.

        Returns:
            A model instance of the row returned for this query and select.
        """
        return cls(cls.collection.find_one(query, select))

    def __init__(self, record=None):
        """Instantiate with data in *record*."""
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

    def delete(self, query):
        """Delete rows matching query.

        Args:

        - query: The query for rows to delete.

        """
        self.collection.remove(query, safe=True)

    def save(self, record):
        """Save *record* in this model's collection.

        Save the record in the model instance's collection and set the internal
        record of this instance to the passed in record.

        Args:

        - record: The dict to save in the model's collection.

        Returns:
            The record passed in.
        """
        self.collection.insert(record, safe=True)
        self.record = record
        return record

    def update(self, record):
        """Update the current instance with *record*.

        Update the current model instance based on its *_id*, set it to the
        passed in *record*.

        Args:
        - record: The record to replace the instance's data with.

        """
        record = dict_for_mongo(record)
        id_dict = {'_id': self.record['_id']}
        self.collection.update(id_dict, {'$set': record}, safe=True)
        self.record = super(
            self.__class__, self.__class__).find_one(id_dict).record

    def batch_save(self, dframe):
        """Save records in batches to avoid document size maximum setting.

        Args:

        - dframe: A DataFrame to save in the current model.

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
