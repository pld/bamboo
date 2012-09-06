import json

from lib.constants import ID
from lib.mongo import dump_mongo_json
from models.calculation import Calculation
from models.dataset import Dataset


class Calculations(object):

    exposed = True

    def POST(self, dataset_id, formula, name, group=None, query=None):
        """
        Create a new calculation for *dataset_id* named *name* that calulates
        the *formula*.  Variables in formula can only refer to columns in the
        dataset.
        """
        dataset = Dataset.find_one(dataset_id)
        if dataset:
            calculation = Calculation()
            calculation.save(dataset, formula, name, group, query)
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
