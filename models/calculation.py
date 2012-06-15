from lib.constants import ALL, DATASET_ID, ERROR, STATS
from lib.exceptions import ParseError
from lib.mongo import mongo_remove_reserved_keys
from lib.parser import Parser
from lib.tasks.calculator import calculate_column
from models.abstract_model import AbstractModel
from models.dataset import Dataset
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

        dframe = Observation.find(dataset, as_df=True)

        # attempt to get a row from the dataframe
        try:
            row = dframe.irow(0)
        except IndexError, err:
            row = {}

        # ensure that the formula is parsable
        try:
            cls.parser.validate_formula(formula, row)
        except ParseError, err:
            # do not save record, return error
            return {ERROR: err}

        record = {
            DATASET_ID: dataset[DATASET_ID],
            cls.FORMULA: formula,
            cls.NAME: name,
        }
        cls.collection.insert(record)

        # invalidate summary ALL since we have a new column
        stats = dataset.get(STATS)
        if stats:
            del stats[ALL]
            del dataset[STATS]
            Dataset.update(dataset, {STATS: stats})

        # call remote calculate and pass calculation id
        calculate_column.delay(dataset, dframe, formula, name)
        return mongo_remove_reserved_keys(record)

    @classmethod
    def find(cls, dataset):
        """
        Return the calculations for given *dataset*.
        """
        return [mongo_remove_reserved_keys(record) for record in cls.collection.find({
            DATASET_ID: dataset[DATASET_ID],
        })]
