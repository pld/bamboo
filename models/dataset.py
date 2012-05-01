import uuid

from lib.constants import BAMBOO_HASH, DATAFRAME_ID
from lib.utils import df_to_hexdigest
from models.abstract_model import AbstractModel
from models.observation import Observation


class Dataset(AbstractModel):

    __collectionname__ = 'dataset'

    @classmethod
    def find(cls, _hash):
        return cls.collection.find({BAMBOO_HASH: _hash})

    @classmethod
    def find_one(cls, _hash):
        return cls.collection.find_one({BAMBOO_HASH: _hash})

    @classmethod
    def find_or_create(cls, dframe, **kwargs):
        digest = df_to_hexdigest(dframe)
        if not cls.find(digest).count():
            dataset = cls.save(digest)
            Observation.save(dframe, dataset, **kwargs)

    @classmethod
    def save(cls, _hash):
        record = {BAMBOO_HASH: _hash, DATAFRAME_ID: uuid.uuid4().hex}
        cls.collection.insert(record)
        return record
