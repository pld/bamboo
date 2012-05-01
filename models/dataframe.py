import uuid

from lib.constants import BAMBOO_HASH, DATAFRAME_ID
from models.abstract_model import AbstractModel
from lib.utils import classproperty


class Dataframe(AbstractModel):

    _table_name = 'dataframe'
    _table = None

    @classproperty
    @classmethod
    def table(cls):
       if not cls._table:
            cls._table = AbstractModel.set_table(cls._table_name)
       return cls._table

    @classmethod
    def find(cls, _hash):
        return cls.table.find({BAMBOO_HASH: _hash})

    @classmethod
    def find_one(cls, _hash):
        return cls.table.find_one({BAMBOO_HASH: _hash})

    @classmethod
    def save(cls, _hash):
        record = {BAMBOO_HASH: _hash, DATAFRAME_ID: uuid.uuid4().hex}
        cls.table.insert(record)
        return record
