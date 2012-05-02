from lib.constants import DATASET_ID, FORMULA, NAME
from models.abstract_model import AbstractModel


class Calculation(AbstractModel):

    __collectionname__ = 'calculations'

    @classmethod
    def save(cls, dframe, formula, name, **kwargs):
        record = {
            DATASET_ID: dframe[DATASET_ID],
            FORMULA: formula,
            NAME: name,
        }
        cls.collection.insert(record)
        return record

    @classmethod
    def find(cls, dataset):
        return cls.collection.find({
            DATASET_ID: dataset[DATASET_ID],
        })
