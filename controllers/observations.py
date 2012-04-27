import json
from urllib2 import HTTPError

from bson import json_util
from pandas import read_csv

from lib.utils import mongo_decode_keys, df_to_hexdigest, open_data_file
from models import dataframe, observation


class Observations(object):
    'Observation controller'

    def __init__(self):
        pass

    exposed = True

    def DELETE(self, _hash):
        """
        Delete dataframe with hash '_hash' from mongo
        """
        observation.delete(_hash)
        return 'deleted hash: %s' % _hash

    def GET(self, _hash, query=None):
        """
        Return data set for hash '_hash' in format 'format'.
        Execute query 'query' in mongo if passed.
        """
        df_link = dataframe.find_one(_hash)
        if df_link:
            rows =  mongo_decode_keys(
                    [x for x in observation.find(df_link, query)])
            return json.dumps(rows, default=json_util.default)
        return 'id not found'

    def POST(self, url=None):
        """
        Read data from URL 'url'.
        If URL is not provided and data is provided, read posted data 'data'.
        """
        _file = open_data_file(url)
        if not _file:
            # could not get a file handle
            return
        try:
            dframe = read_csv(_file, na_values=['n/a'])
        except (IOError, HTTPError):
            return # error reading file/url
        digest = df_to_hexdigest(dframe)
        if not dataframe.find(digest).count():
            df_link = dataframe.save(digest)
            observation.save(dframe, dataframe=df_link, url=url)
        return json.dumps({'id': digest})
