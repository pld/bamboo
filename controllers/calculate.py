import json

import cherrypy

from lib.constants import BAMBOO_ID
from config.db import db
from stats import summarize_df
from lib.utils import mongo_to_df, series_to_jsondict

class Calculate(object):

    def __init__(self):
        pass

    exposed = True

    def GET(self, id=None, group=None, query=None):
        if id:
            # query for data related to id
            r = [x for x in db().collections.find({BAMBOO_ID: id})]
            df = mongo_to_df(r)
            # calculate summary statistics
            stats = {"(ALL)" : summarize_df(df)}
            if group:
                grouped_stats = series_to_jsondict(df.groupby(group).apply(summarize_df))
                stats.update(grouped_stats)
            return json.dumps(stats)
