from pymongo.objectid import ObjectId

from lib.constants import DATASET_ID
from models.calculation import Calculation
from models.dataset import Dataset
from models.observation import Observation


class Calculator(object):
    """
    For calculating new columns.
    """

    def run(self, calculation_id):
        """
        Get necessary data given a calculation ID, execute calculation formula,
        store results in dataset the calculation refers to.
        """
        calculation = Calculation.collection.find_one({
            '_id': ObjectId(calculation_id)
        })
        dataset = Dataset.find_one(calculation[DATASET_ID])
        dframe = Observation.find(dataset, as_df=True)

        func = lambda x: calculation[Calculation.FORMULA]

        new_column = dframe.apply(func, axis=1)
        new_column.name = calculation[Calculation.NAME]
        Observation.update(dframe.join(new_column), dataset)
