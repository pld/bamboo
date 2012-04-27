import json

import cherrypy

from config.db import db
from lib.stats import summarize_with_groups
from lib.utils import mongo_to_df
from models import dataframe, observation

class Calculate(object):

    def __init__(self):
        pass

    exposed = True

    def GET(self, id=None, group=None, query=None):
        df_link = dataframe.find_one(id)
        if df_link:
            rows = [x for x in observation.find(df_link, query)]
            stats = summarize_with_groups(mongo_to_df(rows))
            return json.dumps(stats)
