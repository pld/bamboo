import uuid

from lib.constants import DATASET_ID, DATASET_OBSERVATION_ID
from models.abstract_model import AbstractModel


class Dataset(AbstractModel):

    __collectionname__ = 'datasets'

    @classmethod
    def find(cls, dataset_id):
        return cls.collection.find({DATASET_ID: dataset_id})

    @classmethod
    def find_one(cls, dataset_id):
        return cls.collection.find_one({DATASET_ID: dataset_id})

    @classmethod
    def create(cls, dataset_id):
        return cls.save(dataset_id)

    @classmethod
    def save(cls, dataset_id):
        """
        Store dataset with *dataset_id* as the unique internal ID.
        """
        record = {
            DATASET_ID: dataset_id,
            DATASET_OBSERVATION_ID: uuid.uuid4().hex
        }
        cls.collection.insert(record)
        return record

    @classmethod
    def delete(cls, dataset_id):
        """
        Delete dataset with *dataset_id*
        """
        cls.collection.remove({DATASET_ID: dataset_id})
