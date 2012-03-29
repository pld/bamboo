import json
import urllib2

import cherrypy
from pandas import read_csv

from db import db
from utils import df_for_mongo

SOURCE = '_source'

class Collection(object):

    collections = db().collections

    def __init__(self):
        pass

    exposed = True

    def GET(self, d=None):
        if d:
            r = [x for x in db().collections.find({SOURCE: d})]
            return json.dumps(r)
        return 'Ohai World!'

    def POST(self, d=None):
        if d:
            if 'http://' in d or 'https://' in d:
                f = urllib2.urlopen(d)
                df = read_csv(f, na_values=['n/a'])
                j = df_for_mongo(df)
                # add metadata to file
                for e in j:
                    e[SOURCE] = d
                # insert data into collections
                object_ids = db().collections.insert(j)
                return json.dumps({
                    'object_ids': [str(_id) for _id in object_ids]
                })
