import json

import cherrypy

from db import db
from stats import summary_stats
from utils import mongo_to_df, series_to_json, SOURCE

class Calculate(object):

    def __init__(self):
        pass

    exposed = True

    def GET(self, id=None):
        if id:
            # query for data related to id
            r = [x for x in db().collections.find({SOURCE: id})]
            df = mongo_to_df(r)
            # calculate summary statistics
            dtypes = df.dtypes
            stats = filter(lambda d: d.values()[0] is not None, [
                {c: series_to_json(summary_stats(dtypes[c], i))}
                        for c, i in df.iteritems()
            ])
            return json.dumps(stats)
