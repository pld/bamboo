import json

import cherrypy

from constants import SOURCE
from db import db
from stats import summarize_df 
from utils import mongo_to_df, series_to_json

class Calculate(object):

    def __init__(self):
        pass

    exposed = True

    def GET(self, id=None, group=None):
        if id:
            # query for data related to id
            r = [x for x in db().collections.find({SOURCE: id})]
            df = mongo_to_df(r)
            if group:
                stats = series_to_json(df.groupby(group).apply(summarize_df))
            # calculate summary statistics
            else: 
                stats = summarize_df(df)
            return json.dumps(stats)
