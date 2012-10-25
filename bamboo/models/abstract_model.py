from math import ceil

from bamboo.config.db import Database
from bamboo.config.settings import DB_BATCH_SIZE
from bamboo.core.frame import BAMBOO_RESERVED_KEYS, DATASET_ID
from bamboo.lib.decorators import classproperty
from bamboo.lib.mongo import dict_for_mongo, remove_mongo_reserved_keys


class AbstractModel(object):

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
        return Database.db()[collection_name]

    def ready(self):
        self.update({self.STATE: self.STATE_READY})

    @classproperty
    @classmethod
    def collection(cls):
        if not cls.__collection__:
            cls.__collection__ = AbstractModel.set_collection(
                cls.__collectionname__)
        return cls.__collection__

    @classmethod
    def find(cls, query, select=None, as_dict=False,
             limit=0, order_by=None):
        """
        Interface to mongo's find()

        order_by: sort resulting rows according to a column value (ASC or DESC)
            Examples:   order_by='mycolumn'
                        order_by='-mycolumn'

        limit: apply a limit on the number of rows returned.
            limit is applied AFTER ordering.
        """
        if order_by:
            if order_by[0] in ('-', '+'):
                sort_dir, field = -1 if order_by[0] == '-' else 1, order_by[1:]
            else:
                sort_dir, field = 1, order_by
            order_by = [(field, sort_dir)]

        records = cls.collection.find(
            query, select, sort=order_by, limit=limit)

        return [record for record in records] if as_dict else [
            cls(record) for record in records
        ]

    @classmethod
    def find_one(cls, query, select=None):
        return cls(cls.collection.find_one(query, select))

    def __init__(self, record=None):
        """
        Instantiate with data in *record*.
        """
        self.record = record

    def __nonzero__(self):
        return self.record is not None

    @property
    def clean_record(self):
        """
        Remove reserved keys from records.
        """
        _dict = dict([
            (key, value) for (key, value) in self.record.items() if not key in
            BAMBOO_RESERVED_KEYS
        ])
        return remove_mongo_reserved_keys(_dict)

    def delete(self, query):
        """
        Delete the record.
        """
        self.collection.remove(query, safe=True)

    def save(self, record):
        self.collection.insert(record, safe=True)
        self.record = record
        return record

    def update(self, record):
        """
        Update the model instance based on its *_id*, setting it to the passed
        in *record*.
        """
        record = dict_for_mongo(record)
        self.collection.update(
            {'_id': self.record['_id']}, {'$set': record}, safe=True)

    def batch_save(self, records):
        """
        Save records in batches to avoid document size maximum setting.
        """
        batches = int(ceil(float(len(records)) / DB_BATCH_SIZE))

        for batch in range(0, batches):
            start = batch * DB_BATCH_SIZE
            end = (batch + 1) * DB_BATCH_SIZE
            self.collection.insert(records[start:end], safe=True)
