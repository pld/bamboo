import json

from bson import json_util

from config.db import Database
from lib.constants import DATASET_OBSERVATION_ID, SOURCE, DB_BATCH_SIZE
from lib.mongo import df_to_mongo, mongo_to_df
from models.abstract_model import AbstractModel


class Observation(AbstractModel):

    __collectionname__ = 'observations'

    @classmethod
    def delete(cls, dataset):
        cls.collection.remove({
            DATASET_OBSERVATION_ID: dataset[DATASET_OBSERVATION_ID]
        })

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
                return e.__str__()
        else:
            query = {}
        query[DATASET_OBSERVATION_ID] = dataset[DATASET_OBSERVATION_ID]
        # TODO encode query for mongo
        cursor = cls.collection.find(query)
        if as_df:
            return mongo_to_df(cursor)
        return cursor

    @classmethod
    def save(cls, dframe, dataset):
        """
        Convert dframe to mongo format, iterate through rows adding ids,
        insert chunk by chunk.
        """
        observations = df_to_mongo(dframe)
        # add metadata to file
        dataset_observation_id = dataset[DATASET_OBSERVATION_ID]
        rows = []
        for row in observations:
            row[DATASET_OBSERVATION_ID] = dataset_observation_id
            rows.append(row)
            if len(rows) > DB_BATCH_SIZE:
                # insert data into collection
                cls.collection.insert(rows)
                rows = []
        if len(rows):
            cls.collection.insert(rows)

    @classmethod
    def update(cls, dframe, dataset):
        cls.delete(dataset)
        cls.save(dframe, dataset)
        return cls.find(dataset, as_df=True)
