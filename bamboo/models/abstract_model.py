from config.db import Database
from lib.constants import DATASET_ID
from lib.decorators import classproperty
from lib.mongo import remove_mongo_reserved_keys


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
    def find(cls, query, select=None, as_dict=False):
        records = cls.collection.find(query, select)
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

    @property
    def clean_record(self):
        """
        Remove reserved keys from records.
        """
        _dict = remove_mongo_reserved_keys(self.record)
        _dict.pop(DATASET_ID, None)
        return _dict
