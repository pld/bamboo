import json
from urllib2 import HTTPError

from bson import json_util
import cherrypy
from pandas import read_csv

from config.db import db
from lib.constants import BAMBOO_ID, SOURCE
from lib.utils import df_to_mongo, mongo_decode_keys, df_to_hexdigest, open_data_file
import models.collection

class Collections(object):

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
        if query:
            try:
                query = json.loads(query, object_hook=json_util.object_hook)
            except ValueError, e:
                return e.message
        else:
            query = {}
        if id:
            query[BAMBOO_ID] = id
            json_str = mongo_decode_keys([
                r for r in db().collections.find(query)
            ])
            return json.dumps(json_str, default=json_util.default)

    def POST(self, url=None, data=None):
        '''
        Read data from URL 'url'.
        If URL is not provided and data is provided, read posted data 'data'.
        '''
        f = open_data_file(url)
        if not f: return # could not get a file handle
        try:
            df = read_csv(f, na_values=['n/a'])
        except (IOError, HTTPError):
            return # error reading file/url
        digest = df_to_hexdigest(df)
        num_rows_with_digest = db().collections.find(
                {BAMBOO_ID: digest}).count()
        if not num_rows_with_digest:
            df = df_to_mongo(df)
            # add metadata to file
            for e in df:
                e[BAMBOO_ID] = digest
                e[SOURCE] = url
            # insert data into collections
            db().collections.insert(df)
        return json.dumps({'id': digest})
