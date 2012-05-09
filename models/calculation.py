from lib.calculator import Calculator
from lib.constants import DATASET_ID
from models.abstract_model import AbstractModel


class Calculation(AbstractModel):

    __collectionname__ = 'calculations'

    FORMULA = 'formula'
    NAME = 'name'

    @classmethod
    def save(cls, dataset, formula, name, **kwargs):
        record = {
            DATASET_ID: dataset[DATASET_ID],
            cls.FORMULA: formula,
            cls.NAME: name,
        }
        cls.collection.insert(record)
        cls._calculate(record, dataset)
        return record

    @classmethod
    def find(cls, dataset):
        return cls.collection.find({
            DATASET_ID: dataset[DATASET_ID],
        })

    @classmethod
    def _calculate(cls, record, dataset):
        calculator = Calculator(record, dataset, cls.FORMULA, cls.NAME)
        calculator.start()
