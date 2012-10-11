import json

from bson import json_util

from bamboo.config.settings import DB_BATCH_SIZE
from bamboo.core.frame import DATASET_OBSERVATION_ID
from bamboo.lib.datetools import parse_timestamp_query
from bamboo.lib.jsontools import JSONError
from bamboo.lib.utils import call_async
from bamboo.models.abstract_model import AbstractModel


class Observation(AbstractModel):

    __collectionname__ = 'observations'

    @classmethod
    def delete_all(cls, dataset, query={}):
        """
        Delete the observations for *dataset*.
        """
        query.update({
            DATASET_OBSERVATION_ID: dataset.dataset_observation_id
        })
        cls.collection.remove(query, safe=True)

    @classmethod
    def find(cls, dataset, query=None, select=None, limit=None, order_by=None):
        """
        Try to parse query if exists, then get all rows for ID matching query,
        or if no query all.  Decode rows from mongo and return.

        order_by: sort resulting rows according to a column value (ASC or DESC)
            Examples:   order_by='mycolumn'
                        order_by='-mycolumn'

        limit: apply a limit on the number of rows returned.
            limit is applied AFTER ordering.
        """
        try:
            query = (query and json.loads(
                query, object_hook=json_util.object_hook)) or {}

            query = parse_timestamp_query(query, dataset.schema)
        except ValueError, e:
            raise JSONError('cannot decode query: %s' % e.__str__())

        if select:
            try:
                select = json.loads(select, object_hook=json_util.object_hook)
            except ValueError, e:
                raise JSONError('cannot decode select: %s' % e.__str__())

        query[DATASET_OBSERVATION_ID] = dataset.dataset_observation_id
        return super(cls, cls).find(query, select, as_dict=True,
                                    limit=limit, order_by=order_by)

    def save(self, dframe, dataset):
        """
        Convert *dframe* to mongo format, iterate through rows adding ids for
        *dataset*, insert in chuncks of size *DB_BATCH_SIZE*.
        """
        # build schema for the dataset after having read it from file.
        if not dataset.SCHEMA in dataset.record:
            dataset.build_schema(dframe)

        # add metadata to dataset
        dataset.update({
            dataset.NUM_COLUMNS: len(dframe.columns),
            dataset.NUM_ROWS: len(dframe),
        })

        dataset_observation_id = dataset.dataset_observation_id
        rows = []

        labels_to_slugs = dataset.build_labels_to_slugs()

        # if column name is not in map assume it is already slugified
        # (i.e. NOT a label)
        dframe.columns = [labels_to_slugs.get(column, column) for column in
                          dframe.columns.tolist()]

        for row_index, row in dframe.iterrows():
            row = row.to_dict()
            row[DATASET_OBSERVATION_ID] = dataset_observation_id
            rows.append(row)
            if len(rows) > DB_BATCH_SIZE:
                # insert data into collection
                self.collection.insert(rows, safe=True)
                rows = []

        if len(rows):
            self.collection.insert(rows, safe=True)

        call_async(dataset.summarize, dataset, dataset)
