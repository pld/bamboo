from bamboo.core.frame import DATASET_OBSERVATION_ID
from bamboo.lib.async import call_async
from bamboo.lib.datetools import parse_timestamp_query
from bamboo.models.abstract_model import AbstractModel


class Observation(AbstractModel):

    __collectionname__ = 'observations'

    @classmethod
    def delete_all(cls, dataset, query={}):
        """Delete the observations for `dataset`.

        :param dataset: The dataset to delete observations for.
        :param query: An optional query to restrict deletion.
        """
        query.update({
            DATASET_OBSERVATION_ID: dataset.dataset_observation_id
        })

        super(cls, Observation()).delete(query)

    @classmethod
    def find(cls, dataset, query=None, select=None, limit=0, order_by=None,
             as_cursor=False):
        """Return observation rows matching parameters.

        :param dataset: Dataset to return rows for.
        :param query: Optional query to restrict matching rows to.
        :param select: Optional select to limit returned values.
        :param limit: Limit on the number of returned rows.
        :param order_by: Order parameter for rows.

        :raises: `JSONError` if the query could not be parsed.

        :returns: A list of dictionaries matching the passed in `query` and
            other parameters.
        """
        if query is not None:
            if dataset.schema:
                query = parse_timestamp_query(query, dataset.schema)
        else:
            query = {}

        query[DATASET_OBSERVATION_ID] = dataset.dataset_observation_id

        return super(cls, cls).find(
            query, select, as_dict=True, limit=limit, order_by=order_by,
            as_cursor=as_cursor)

    @classmethod
    def update(cls, dframe, dataset):
        dataset.build_schema(dframe)

        # must have MONGO_RESERVED_KEY_id as index     
        if not DATASET_OBSERVATION_ID in dframe.columns:
            cls.batch_update(dataset.encode_dframe_columns(dframe).reset_index())
        else:
            cls.batch_update(dframe.reset_index())

        # add metadata to dataset, discount ID column
        dataset.update({
            dataset.NUM_ROWS: len(dframe),
            dataset.STATE: cls.STATE_READY,
        })
        # TODO make summary update-friendly
        dataset.summarize(dframe, update=True)

    @classmethod
    def save(cls, dframe, dataset):
        """Save data in `dframe` with the `dataset`.

        Encode `dframe` for MongoDB, and add fields to identify it with the
        passed in `dataset`. All column names in `dframe` are converted to
        slugs using the dataset's schema.  The dataset is update to store the
        size of the stored data. A background task to cache a summary of the
        dataset is launched.

        :param dframe: The DataFrame (or BambooFrame) to store.
        :param dataset: The dataset to store the dframe in.
        """
        # build schema for the dataset after having read it from file.
        if not dataset.schema:
            dataset.build_schema(dframe)

        if not DATASET_OBSERVATION_ID in dframe.columns:
            cls.batch_save(dataset.encode_dframe_columns(dframe))
        else:
            cls.batch_save(dframe)

        # add metadata to dataset, discount ID column
        dataset.update({
            dataset.NUM_ROWS: len(dframe),
            dataset.STATE: cls.STATE_READY,
        })
        dataset.summarize(dframe)
