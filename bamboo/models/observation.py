from bamboo.core.frame import BambooFrame, DATASET_OBSERVATION_ID, INDEX
from bamboo.lib.async import call_async
from bamboo.lib.datetools import parse_timestamp_query
from bamboo.lib.query_args import QueryArgs
from bamboo.models.abstract_model import AbstractModel


class Observation(AbstractModel):

    __collectionname__ = 'observations'

    @classmethod
    def delete(cls, dataset, index):
        query = {INDEX: index,
                 DATASET_OBSERVATION_ID: dataset.dataset_observation_id}

        super(cls, cls()).delete(query)

    @classmethod
    def delete_all(cls, dataset, query={}):
        """Delete the observations for `dataset`.

        :param dataset: The dataset to delete observations for.
        :param query: An optional query to restrict deletion.
        """
        query.update({
            DATASET_OBSERVATION_ID: dataset.dataset_observation_id
        })

        super(cls, cls()).delete(query)

    @classmethod
    def find(cls, dataset, query_args=QueryArgs(), as_cursor=False):
        """Return observation rows matching parameters.

        :param dataset: Dataset to return rows for.
        :param query_args: An optional QueryArgs to hold the query arguments.

        :raises: `JSONError` if the query could not be parsed.

        :returns: A list of dictionaries matching the passed in `query` and
            other parameters.
        """
        query = query_args.query

        if dataset.schema:
            query = parse_timestamp_query(query, dataset.schema)

        query[DATASET_OBSERVATION_ID] = dataset.dataset_observation_id
        query_args.query = query

        return super(cls, cls).find(
            query_args, as_dict=True, as_cursor=as_cursor)

    @classmethod
    def update_from_dframe(cls, dframe, dataset):
        dataset.build_schema(dframe)

        # must have MONGO_RESERVED_KEY_id as index
        if not DATASET_OBSERVATION_ID in dframe.columns:
            cls.batch_update(dataset.encode_dframe_columns(
                dframe).reset_index())
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
    def find_one(cls, dataset, index):
        """Return row by index.

        :param dataset: The dataset to find the row for.
        :param index: The index of the row to find.
        """
        query = {INDEX: index,
                 DATASET_OBSERVATION_ID: dataset.dataset_observation_id}

        return super(cls, cls).find_one(query)

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
        # Build schema for the dataset after having read it from file.
        if not dataset.schema:
            dataset.build_schema(dframe)

        dframe = cls.__add_index_to_dframe(dframe)

        if not DATASET_OBSERVATION_ID in dframe.columns:
            # This dframe has not been saved before, encode its columns.
            cls.batch_save(dataset.encode_dframe_columns(dframe))
        else:
            cls.batch_save(dframe)

        # XXX do we need this? (merge)
        dframe.remove_bamboo_reserved_keys()

        # add metadata to dataset, discount ID column
        dataset.update({
            dataset.NUM_ROWS: len(dframe),
            dataset.STATE: cls.STATE_READY,
        })
        dataset.summarize(dframe)

    @classmethod
    def update(cls, dataset, index, record):
        """Update a dataset row by index.

        The record dictionary will update, not replace, the data in the row at
        index.

        :param dataset: The dataset to update a row for.
        :param dex: The index of the row to update.
        :param record: The dictionary to update the row with.
        """
        observation = cls.find_one(dataset, index)
        super(cls, observation).update(record)

    @classmethod
    def __add_index_to_dframe(self, dframe):
        if not INDEX in dframe.columns:
            # No index, create index for this dframe.

            if not 'index' in dframe.columns:
                # Custom index not supplied, use pandas default index.
                dframe = dframe.reset_index()

            dframe.rename(columns={'index': INDEX}, inplace=True)

        return BambooFrame(dframe)
