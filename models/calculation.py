from lib.constants import DATAFRAME_ID, FORMULA, NAME
from models.abstract_model import AbstractModel


class Calculation(AbstractModel):

    __collectionname__ = 'calculations'

    @classmethod
    def save(cls, dframe, formula, name, **kwargs):
        record = {
            DATAFRAME_ID: dframe[DATAFRAME_ID],
            FORMULA: formula,
            NAME: name,
        }
        cls.collection.insert(record)

    @classmethod
    def find(dframe):
        return [x for x in cls.collection.find({
            DATAFRAME_ID: dframe[DATAFRAME_ID],
        })]
