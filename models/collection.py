import json

from bson import json_util

from config.db import db
from lib.constants import BAMBOO_ID, SOURCE
from lib.utils import df_to_mongo


def delete(id):
    db().collections.remove({BAMBOO_ID: id})


def get(id, query=None):
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
    query[BAMBOO_ID] = id
    return db().collections.find(query)

def save(df, **kwargs):
    df = df_to_mongo(df)
    # add metadata to file
    for e in df:
        e[BAMBOO_ID] = kwargs['digest']
        e[SOURCE] = kwargs['url']
        # insert data into collection
        db().collections.insert(e)
