import json

from lib.stats import summarize_with_groups
from lib.utils import mongo_to_df
from models.dataset import Dataset
from models.observation import Observation

class Calculate(object):
    'Calculate controller'

    def __init__(self):
        pass

    exposed = True

    def GET(self, id, group=None, query=None):
        """
        Retrieve a calculation for dataframe with hash 'id'.
        Retrieve data frame using query 'query' and group by 'group'.
        """
        dataset = Dataset.find_one(id)
        if dataset:
            observations = Observation.find(dataset, query, as_df=True)
            stats = summarize_with_groups(observations, group)
            return json.dumps(stats)
        return 'dataset not found'
