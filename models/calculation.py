from lib.constants import DATASET_ID, FORMULA, NAME
from models.abstract_model import AbstractModel


class Calculation(AbstractModel):

    __collectionname__ = 'calculations'

    @classmethod
    def save(cls, dataset, formula, name, **kwargs):
        record = {
            DATASET_ID: dataset[DATASET_ID],
            FORMULA: formula,
            NAME: name,
        }
        cls.collection.insert(record)
        cls._calculate(record, dataset[DATASET_ID])
        return record

    @classmethod
    def find(cls, dataset):
        return cls.collection.find({
            DATASET_ID: dataset[DATASET_ID],
        })

    @classmethod
    def _calculate(cls, record, dataset_id):
        'Calculate new calulculation in the background'
        # TODO: write me
        pass
