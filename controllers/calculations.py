import json

from models.calculation import Calculation
from models.dataset import Dataset
from lib.mongo import dump_mongo_json


class Calculations(object):

    def __init__(self):
        pass

    exposed = True

    def POST(self, dataset_id, formula, name, query=None, constraints=None):
        dataset = Dataset.find_one(dataset_id)
        if dataset:
            Calculation.save(dataset, formula, name)
            return json.dumps({'id': dataset_id})

    def GET(self, dataset_id):
        dataset = Dataset.find_one(dataset_id)
        if dataset:
            # get the calculations
            calculations = Calculation.find(dataset)
            return dump_mongo_json([x for x in calculations])
