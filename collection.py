import json
import urllib2

import cherrypy
from pandas import read_csv

from constants import SOURCE
from db import db
from utils import df_to_mongo, mongo_to_jsondict

class Collection(object):

    def __init__(self):
        pass

    exposed = True

    def DELETE(self, id=None):
        '''
        Delete id 'id' from mongo
        '''
        if id:
            db().collections.remove({SOURCE: id})
            return 'deleted id: %s' % id

    def GET(self, id=None, format='json', query=None):
        '''
        Return data set for id 'id' in format 'format'.
        Execute query 'query' in mongo if passed.
        '''
        if id:
            json_str = mongo_to_jsondict([
                r for r in db().collections.find({SOURCE: id})
            ])
            return json.dumps(json_str)

    def POST(self, url=None, data=None):
        '''
        Read data from URL 'url'.
        If URL is not provided and data is provided, read posted data 'data'.
        '''
        if url and 'http://' in url or 'https://' in url:
            f = urllib2.urlopen(url)
            df = read_csv(f, na_values=['n/a'])
            j = df_to_mongo(df)
            # add metadata to file
            for e in j:
                e[SOURCE] = url
            # insert data into collections
            db().collections.insert(j)
            return json.dumps({'id': url})
