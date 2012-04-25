import json

import cherrypy

from config.db import db
from lib.constants import BAMBOO_ID
from lib.stats import summarize_df
from lib.utils import mongo_to_df, series_to_jsondict
from models import collection

class Calculate(object):

    def __init__(self):
        pass

    exposed = True

    def GET(self, id=None, group=None, query=None):
        if id:
            rows = collection.get(id, query)
            df = mongo_to_df(rows)
            # calculate summary statistics
            stats = {"(ALL)" : summarize_df(df)}
            if group:
                grouped_stats = series_to_jsondict(df.groupby(group).apply(summarize_df))
                stats.update(grouped_stats)
            return json.dumps(stats)
