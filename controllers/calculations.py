from models import calculation


class Calculations(object):

    def __init__(self):
        pass

    exposed = True

    def POST(self, id, formula, name=None, query=None, constraints=None):
        df_link = dataframe.find_one(id)
        if df_link:
            calculation.save(df, formula)
