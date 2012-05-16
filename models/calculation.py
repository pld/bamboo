from lib.constants import DATASET_ID
from lib.exceptions import ParseError
from lib.parser import Parser
from lib.tasks.calculator import calculate_column
from models.abstract_model import AbstractModel
from models.observation import Observation


class Calculation(AbstractModel):

    __collectionname__ = 'calculations'
    parser = Parser()

    FORMULA = 'formula'
    NAME = 'name'


    @classmethod
    def save(cls, dataset, formula, name, **kwargs):
        """
        Attempt to parse formula, then save formula, and add a task to calculate
        formula.
        """
        try:
            # ensure that the formula is parsable
            cls.parser.parse_formula(formula)
        except ParseError, err:
            # do not save record, return error
            return err

        record = {
            DATASET_ID: dataset[DATASET_ID],
            cls.FORMULA: formula,
            cls.NAME: name,
        }
        cls.collection.insert(record)

        # call remote calculate and pass calculation id
        dframe = Observation.find(dataset, as_df=True)
        calculate_column.delay(dataset, dframe, formula, name)
        return record

    @classmethod
    def find(cls, dataset):
        return cls.collection.find({
            DATASET_ID: dataset[DATASET_ID],
        })
