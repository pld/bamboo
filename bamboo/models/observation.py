import json

from bson import json_util
from pandas import Series

from bamboo.core.frame import DATASET_OBSERVATION_ID
from bamboo.lib.datetools import parse_timestamp_query
from bamboo.lib.jsontools import JSONError
from bamboo.lib.utils import call_async
from bamboo.models.abstract_model import AbstractModel


class Observation(AbstractModel):

    __collectionname__ = 'observations'

    @classmethod
    def delete_all(cls, dataset, query={}):
        """Delete the observations for *dataset*.

        Args:
            dataset: The dataset to delete observations for.
            query: An optional query to restrict deletion.
        """
        query.update({
            DATASET_OBSERVATION_ID: dataset.dataset_observation_id
        })
        cls.collection.remove(query, safe=True)

    @classmethod
    def find(cls, dataset, query=None, select=None, limit=0, order_by=None):
        """Return observation rows matching parameters.

        Args:
            dataset: Dataset to return rows for.
            query: Optional query to restrict matching rows to.
            select: Optional select to limit returned values.
            limit: Limit on the number of returned rows.
            order_by: Order parameter for rows.

        Returns:
            A list of dictionaries matching the passed in *query* and other
            parameters.

        Raises:
            JSONError: An error is the query could not be parsed.
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

        labels_to_slugs = dataset.build_labels_to_slugs()

        # if column name is not in map assume it is already slugified
        # (i.e. NOT a label)
        columns = dframe.columns = [
            labels_to_slugs.get(column, column) for column in
            dframe.columns.tolist()]

        id_column = Series([dataset.dataset_observation_id] * len(dframe))
        id_column.name = DATASET_OBSERVATION_ID
        dframe = dframe.join(id_column)

        rows = [row.to_dict() for (_, row) in dframe.iterrows()]
        self.batch_save(rows)

        # add metadata to dataset
        dataset.update({
            dataset.NUM_COLUMNS: len(columns),
            dataset.NUM_ROWS: len(dframe),
            dataset.STATE: self.STATE_READY,
        })

        call_async(dataset.summarize, dataset)
