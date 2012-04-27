import json

from bson import json_util

from config.db import db
from lib.constants import DATAFRAME_ID, SOURCE
from lib.utils import df_to_mongo


TABLE = db().observations


def delete(id):
    TABLE.remove({DATAFRAME_ID: id})


def find(df, query=None):
    """
    Try to parse query if exists, then get all rows for ID matching query,
    or if no query all.  Decode rows from mongo and return.
    """
    if query:
        try:
            query = json.loads(query, object_hook=json_util.object_hook)
        except ValueError, e:
            return e.message
    else:
        query = {}
    query[DATAFRAME_ID] = df[DATAFRAME_ID]
    return TABLE.find(query)


def save(df, **kwargs):
    df = df_to_mongo(df)
    # add metadata to file
    dataframe_id = kwargs['dataframe'][DATAFRAME_ID]
    url = kwargs['url']
    for e in df:
        e[DATAFRAME_ID] = dataframe_id
        e[SOURCE] = url
        # insert data into collection
        TABLE.insert(e)
