import uuid

from lib.constants import DATASET_ID, DATASET_OBSERVATION_ID
from models.abstract_model import AbstractModel
from models.observation import Observation


class Dataset(AbstractModel):

    __collectionname__ = 'datasets'

    @classmethod
    def find(cls, dataset_id):
        return cls.collection.find({DATASET_ID: dataset_id})

    @classmethod
    def find_one(cls, dataset_id):
        return cls.collection.find_one({DATASET_ID: dataset_id})

    @classmethod
    def create(cls, dframe, **kwargs):
        dataset_id = uuid.uuid4().hex
        dataset = cls.save(dataset_id)
        Observation.save(dframe, dataset, **kwargs)
        return dataset_id

    @classmethod
    def save(cls, dataset_id, column_name=False, dataset=None):
        """
        Store data set for a bamboo hash and create a unique internal ID for
        that data set.
        """
        record = {DATASET_ID: dataset_id, DATASET_OBSERVATION_ID: uuid.uuid4().hex}
        cls.collection.insert(record)
        return record

    @classmethod
    def delete(cls, dataset_id):
        cls.collection.remove({DATASET_ID: dataset_id})
