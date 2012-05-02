from models import calculation
from lib.utils import mongo_to_df, mongo_to_json


class Calculations(object):

    def __init__(self):
        pass

    exposed = True

    def POST(self, id, formula, name, query=None, constraints=None):
        df_link = dataframe.find_one(id)
        if df_link:
            calculation.save(df, formula, name)

    def GET(self, id):
        df_link = dataframe.find_one(id)
        if df_link:
            # get the current dataframe for this id
            cursor = observation.find(df_link, as_df=True)
            dframe = mongo_to_df(cursor)
            # get the calculations
            calculations = calculation.find(df_link)
            columns_to_calculate = []
            # see if all the calculations have been run
            for column in calculations:
                if column not in dframe.columns:
                    columns_to_calculate.append(column)
            if columns_to_calculate:
                # run calculations
                pass
            return mongo_to_json(cursor)
