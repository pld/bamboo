import json

import cherrypy

from config.db import db
from lib.constants import BAMBOO_ID
from lib.stats import summarize_with_groups
from lib.utils import mongo_to_df
from models import collection

class Calculate(object):

    def __init__(self):
        pass

    exposed = True

    def GET(self, id=None, group=None, query=None):
        if id:
            rows = [x for x in collection.get(id, query)]
            stats = summarize_with_groups(mongo_to_df(rows))
            return json.dumps(stats)
