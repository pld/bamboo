from models.calculation import Calculation
from models.dataset import Dataset
from lib.utils import mongo_to_df, mongo_to_json


class Calculations(object):

    def __init__(self):
        pass

    exposed = True

    def POST(self, id, formula, name, query=None, constraints=None):
        dataset = Dataset.find_one(id)
        if dataset:
            Calculation.save(dataset, formula, name)

    def GET(self, id):
        dataset = Dataset.find_one(id)
        if dataset:
            dframe = mongo_to_df(cursor)
            # get the calculations
            calculations = Calculation.find(dataset)
            columns_to_calculate = []
            # see if all the calculations have been run
            for column in calculations:
                if column not in dframe.columns:
                    columns_to_calculate.append(column)
            if columns_to_calculate:
                # run calculations
                pass
            return mongo_to_json(cursor)
