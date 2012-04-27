import json

from lib.stats import summarize_with_groups
from lib.utils import mongo_to_df
from models import dataframe, observation

class Calculate(object):
    'Calculate controller'

    def __init__(self):
        pass

    exposed = True

    def GET(self, _hash=None, group=None, query=None):
        """
        Retrieve a calculation for dataframe with hash '_hash'.
        Retrieve data frame using query 'query' and group by 'group'.
        """
        df_link = dataframe.find_one(_hash)
        if df_link:
            rows = [x for x in observation.find(df_link, query)]
            stats = summarize_with_groups(mongo_to_df(rows))
            return json.dumps(stats)
