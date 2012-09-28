import json

from controllers.abstract_controller import AbstractController
from lib.constants import ID
from lib.mongo import dump_mongo_json
from lib.utils import call_async, dump_or_error
from models.calculation import Calculation
from models.dataset import Dataset


class Calculations(AbstractController):

    def DELETE(self, dataset_id, name):
        """
        Delete the calculation for the dataset specified by the hash
        *dataset_id* from mongo and the column *name*.

        This will also remove the column *name* from the data frame for
        dataset.
        """
        result = None

        calculation = Calculation.find_one(dataset_id, name)
        if calculation:
            dataset = Dataset.find_one(dataset_id)
            task = call_async(calculation.delete, calculation, dataset)
            result = {self.SUCCESS: 'deleted calculation: %s for dataset: %s' %
                      (name, dataset_id)}
        return dump_or_error(result,
                             'name and dataset_id combination not found')

    def POST(self, dataset_id, formula, name, group=None):
        """
        Create a new calculation for *dataset_id* named *name* that calulates
        the *formula*.  Variables in formula can only refer to columns in the
        dataset.
        """
        dataset = Dataset.find_one(dataset_id)
        if dataset:
            calculation = Calculation()
            calculation.save(dataset, formula, name, group)
            return dump_mongo_json(calculation.clean_record)

    def GET(self, dataset_id):
        """
        Retrieve the calculations for *dataset_id*.
        """
        dataset = Dataset.find_one(dataset_id)
        if dataset:
            # get the calculations
            calculations = Calculation.find(dataset)
            return dump_mongo_json(
                [x.clean_record for x in calculations])
