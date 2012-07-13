from config.db import Database
from lib.decorators import classproperty


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
