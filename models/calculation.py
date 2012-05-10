from lib.calculator_client import CalculatorClient
from lib.constants import DATASET_ID
from models.abstract_model import AbstractModel


class Calculation(AbstractModel):

    __collectionname__ = 'calculations'
    calculator_client = CalculatorClient()

    FORMULA = 'formula'
    NAME = 'name'


    @classmethod
    def save(cls, dataset, formula, name, **kwargs):
        record = {
            DATASET_ID: dataset[DATASET_ID],
            cls.FORMULA: formula,
            cls.NAME: name,
        }
        calculation_id = str(cls.collection.insert(record))
        # call remote calculate and pass calculation id
        cls.calculator_client.send(calculation_id)
        return record

    @classmethod
    def find(cls, dataset):
        return cls.collection.find({
            DATASET_ID: dataset[DATASET_ID],
        })
