import json

from bson import json_util

from config.db import Database
from lib.constants import DATASET_ID, SOURCE
from lib.utils import df_to_mongo, mongo_to_df
from models.abstract_model import AbstractModel


class Observation(AbstractModel):

    __collectionname__ = 'observations'

    @classmethod
    def delete(cls, dataset):
        cls.collection.remove({DATASET_ID: dataset[DATASET_ID]})

    @classmethod
    def find(cls, dataset, query=None, as_df=False):
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
        query[DATASET_ID] = dataset[DATASET_ID]
        cursor = cls.collection.find(query)
        if as_df:
            return mongo_to_df(cursor)
        return cursor

    @classmethod
    def save(cls, dframe, dataset, **kwargs):
        dframe = df_to_mongo(dframe)
        # add metadata to file
        dataframe_id = dataset[DATASET_ID]
        url = kwargs.get('url')
        rows = []
        for row in dframe:
            row[DATASET_ID] = dataframe_id
            row[SOURCE] = url
            # insert data into collection
            cls.collection.insert(row)
            rows.append(row)
        return rows
