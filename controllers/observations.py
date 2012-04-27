import json
from urllib2 import HTTPError

from bson import json_util
import cherrypy
from pandas import read_csv

from lib.constants import SOURCE
from lib.utils import mongo_decode_keys, df_to_hexdigest, open_data_file
from models import dataframe, observation


class Observations(object):

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
        df_link = dataframe.find_one(id)
        if df_link:
            rows =  mongo_decode_keys([x for x in observation.find(df_link, query)])
            return json.dumps(rows, default=json_util.default)
        return 'id not found'

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
        if not dataframe.find(digest).count():
            df_link = dataframe.save(digest)
            observation.save(df, dataframe=df_link, url=url)
        return json.dumps({'id': digest})
