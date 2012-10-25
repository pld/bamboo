import json

from bamboo.controllers.abstract_controller import AbstractController
from bamboo.core.parser import ParseError
from bamboo.lib.mongo import dump_mongo_json
from bamboo.models.calculation import Calculation
from bamboo.models.dataset import Dataset


class Calculations(AbstractController):
    """
    The Calculations Controller provides access to calculations.  Calculations
    are formulas and names
    that (for now) must be linked to a specific dataset via that dataset's ID.

    All actions in the Calculations Controller can optionally take a *callback*
    parameter.  If passed the returned result will be wrapped this the
    parameter value.  E.g., is ``callback=parseResults`` the returned value
    will be ``parseResults([some-JSON])``, where ``some-JSON`` is the function
    return value.
    """

    def delete(self, dataset_id, name, callback=False):
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
            calculation.delete(dataset)
            result = {self.SUCCESS: 'deleted calculation: %s for dataset: %s' %
                      (name, dataset_id)}
        return self.dump_or_error(result,
                                  'name and dataset_id combination not found',
                                  callback)

    def create(self, dataset_id, formula, name, group=None, callback=False):
        """
        Create a new calculation for *dataset_id* named *name* that calulates
        the *formula*.  Variables in formula can only refer to columns in the
        dataset.
        """
        error = 'dataset_id not found'
        result = None
        dataset = Dataset.find_one(dataset_id)
        if dataset:
            try:
                Calculation.create(dataset, formula, name, group)
                result = {
                    self.SUCCESS: 'created calulcation: %s for dataset: %s'
                    % (name, dataset_id)}
            except ParseError as e:
                error = e.__str__()
        return self.dump_or_error(result, error, callback)

    def show(self, dataset_id, callback=False):
        """
        Retrieve the calculations for *dataset_id*.
        """
        dataset = Dataset.find_one(dataset_id)
        result = None

        if dataset:
            # get the calculations
            result = dataset.calculations()
            result = [x.clean_record for x in result]
        return self.dump_or_error(result, 'dataset_id not found', callback)
