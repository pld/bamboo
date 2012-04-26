import json
from urllib2 import HTTPError

from bson import json_util
import cherrypy
from pandas import read_csv

from config.db import db
from lib.constants import BAMBOO_ID, SOURCE
from lib.utils import mongo_decode_keys, df_to_hexdigest, open_data_file
from  models import collection

class Collections(object):

    def __init__(self):
        pass

    exposed = True

    def DELETE(self, id):
        """
        Delete id 'id' from mongo
        """
        collection.delete(id)
        return 'deleted id: %s' % id

    def GET(self, id, format='json', query=None):
        """
        Return data set for id 'id' in format 'format'.
        Execute query 'query' in mongo if passed.
        """
        rows =  mongo_decode_keys([x for x in collection.get(id, query)])
        return json.dumps(rows, default=json_util.default)

    def POST(self, url=None, data=None):
        """
        Read data from URL 'url'.
        If URL is not provided and data is provided, read posted data 'data'.
        """
        f = open_data_file(url)
        if not f: return # could not get a file handle
        try:
            df = read_csv(f, na_values=['n/a'])
        except (IOError, HTTPError):
            return # error reading file/url
        digest = df_to_hexdigest(df)
        num_rows_with_digest = collection.get(digest).count()
        if not num_rows_with_digest:
            collection.save(df, digest=digest, url=url)
        return json.dumps({'id': digest})
