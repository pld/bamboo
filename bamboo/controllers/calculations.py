import json

from bamboo.controllers.abstract_controller import AbstractController
from bamboo.core.parser import ParseError
from bamboo.lib.constants import ID
from bamboo.lib.mongo import dump_mongo_json
from bamboo.lib.utils import call_async
from bamboo.models.calculation import Calculation
from bamboo.models.dataset import Dataset


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
            task = call_async(
                calculation.delete, dataset, calculation, dataset)
            result = {self.SUCCESS: 'deleted calculation: %s for dataset: %s' %
                      (name, dataset_id)}
        return self.dump_or_error(result,
                             'name and dataset_id combination not found')

    def POST(self, dataset_id, formula, name, group=None):
        """
        Create a new calculation for *dataset_id* named *name* that calulates
        the *formula*.  Variables in formula can only refer to columns in the
        dataset.
        """
        error = 'name and dataset_id combination not found'
        result = None
        dataset = Dataset.find_one(dataset_id)
        if dataset:
            try:
                Calculation.create(dataset, formula, name, group)
                result = {
                    self.SUCCESS: 'created calulcation: %s for dataset: %s'
                        % (name, dataset_id)}
            except ParseError as err:
                error = e.__str__()
        return self.dump_or_error(result, error)

    def GET(self, dataset_id):
        """
        Retrieve the calculations for *dataset_id*.
        """
        dataset = Dataset.find_one(dataset_id)
        if dataset:
            # get the calculations
            calculations = dataset.calculations()
            return dump_mongo_json(
                [x.clean_record for x in calculations])
