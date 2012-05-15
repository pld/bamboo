from lib.calculator import Calculator
from lib.constants import DATASET_ID
from models.abstract_model import AbstractModel
from models.observation import Observation


class Calculation(AbstractModel):

    __collectionname__ = 'calculations'
    calculator = Calculator()

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
        # call remote calculate and pass calculation id
        dframe = Observation.find(dataset, as_df=True)
        cls.calculator.run.delay(dataset, dframe, formula, name)
        return record

    @classmethod
    def find(cls, dataset):
        return cls.collection.find({
            DATASET_ID: dataset[DATASET_ID],
        })
