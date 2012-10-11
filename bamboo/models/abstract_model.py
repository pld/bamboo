from bamboo.config.db import Database
from bamboo.core.frame import BAMBOO_RESERVED_KEYS, DATASET_ID
from bamboo.lib.decorators import classproperty
from bamboo.lib.mongo import remove_mongo_reserved_keys


class AbstractModel(object):

    __collection__ = None

    @classmethod
    def set_collection(cls, collection_name):
        return Database.db()[collection_name]

    @classproperty
    @classmethod
    def collection(cls):
        if not cls.__collection__:
            cls.__collection__ = AbstractModel.set_collection(
                cls.__collectionname__)
        return cls.__collection__

    @classmethod
    def find(cls, query, select=None, as_dict=False,
             limit=None, order_by=None):
        """ Interface to mongo's find()

        order_by: sort resulting rows according to a column value (ASC or DESC)
            Examples:   order_by='mycolumn'
                        order_by='-mycolumn'

        limit: apply a limit on the number of rows returned.
            limit is applied AFTER ordering.
        """
        records = cls.collection.find(query, select)

        # apply ORDER BY
        # Usage:
        # sort='column' or sort='-column'
        if order_by:
            if order_by[0] in ('-', '+'):
                sort_dir, field = -1 if order_by[0] == '-' else 1, order_by[1:]
            else:
                sort_dir, field = 1, order_by
            records = records.sort(field, sort_dir)

        # apply LIMIT
        # limit is applied AFTER order_by as we assume people would want
        # to limit the overall data.
        # It's less efficient that sorting a limited subset of data obvsl.
        if limit:
            records = records.limit(limit)

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

    def delete(self, query):
        """
        Delete the record.
        """
        self.collection.remove(query, safe=True)

    def save(self, record):
        self.collection.insert(record, safe=True)
        self.record = record
        return record

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
