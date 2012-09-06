from config.db import Database
from lib.constants import DATASET_ID
from lib.decorators import classproperty
from lib.mongo import mongo_remove_reserved_keys


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
        """
        Return the calculations for given *dataset*.
        """
        records = cls.collection.find(query, select)
        return [record for record in records] if as_dict else [
            cls(record) for record in records
        ]

    @classmethod
    def find_one(cls, dataset_id):
        return cls(cls.collection.find_one({DATASET_ID: dataset_id}))

    def __init__(self, record=None):
        """
        Instantiate with data in *record*.
        """
        self.record = record

    def delete(self, query):
        """
        Delete dataset with *dataset_id*.
        """
        self.collection.remove(query, safe=True)

    @property
    def clean_record(self):
        """
        Clean records after taking them out of.
        """
        return mongo_remove_reserved_keys(self.record)
