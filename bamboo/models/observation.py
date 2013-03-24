from math import ceil

from pymongo.errors import AutoReconnect

from bamboo.core.frame import BambooFrame, DATASET_ID, INDEX
from bamboo.lib.query_args import QueryArgs
from bamboo.lib.utils import replace_keys
from bamboo.models.abstract_model import AbstractModel


class Observation(AbstractModel):

    __collectionname__ = 'observations'
    ENCODING = 'encoding'
    DATASET_ID_ENCODING = DATASET_ID + ENCODING

    @classmethod
    def delete(cls, dataset, index):
        query = {INDEX: index,
                 DATASET_ID: dataset.dataset_id}

        super(cls, cls()).delete(query)

    @classmethod
    def delete_all(cls, dataset, query={}):
        """Delete the observations for `dataset`.

        :param dataset: The dataset to delete observations for.
        :param query: An optional query to restrict deletion.
        """
        query.update({
            DATASET_ID: dataset.dataset_id
        })

        super(cls, cls()).delete(cls.encode_query(query, dataset))

    @classmethod
    def encoding(cls, dataset):
        return super(cls, cls).find_one({
            cls.DATASET_ID_ENCODING: dataset.dataset_id
        }).record[cls.ENCODING]

    @classmethod
    def encode_query(cls, query, dataset):
        encoding = cls.encoding(dataset)
        return replace_keys(query, encoding)

    @classmethod
    def decoding(cls, dataset):
        return {v: k for (k, v) in cls.encoding(dataset).items()}

    @classmethod
    def find(cls, dataset, query_args=None, as_cursor=False):
        """Return observation rows matching parameters.

        :param dataset: Dataset to return rows for.
        :param query_args: An optional QueryArgs to hold the query arguments.

        :raises: `JSONError` if the query could not be parsed.

        :returns: A list of dictionaries matching the passed in `query` and
            other parameters.
        """
        if query_args:
            query = query_args.query
            query.update({DATASET_ID: dataset.dataset_id})
            query_args.query = cls.encode_query(query, dataset)
        else:
            query_args = QueryArgs()

        return super(cls, cls).find(
            query_args, as_dict=True, as_cursor=as_cursor)

    @classmethod
    def update_from_dframe(cls, dframe, dataset):
        dataset.build_schema(dframe)

        # must have MONGO_RESERVED_KEY_id as index
        if not DATASET_ID in dframe.columns:
            cls.__batch_update(
                dataset.encode_dframe_columns(dframe).reset_index())
        else:
            cls.__batch_update(dframe.reset_index())

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
                 DATASET_ID: dataset.dataset_id}

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

        # Encode Columns
        dframe = dataset.encode_dframe_columns(dframe)

        if not DATASET_ID in dframe.columns:
            # This dframe has not been saved before, add an ID column.
            dframe = dataset.add_id_column(dframe)

        encoding = cls.__batch_save(dframe)
        record = {cls.DATASET_ID_ENCODING: dataset.dataset_id,
                  cls.ENCODING: encoding}
        super(cls, cls()).delete({cls.DATASET_ID_ENCODING: dataset.dataset_id})
        super(cls, cls()).save(record)

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

    @classmethod
    def __batch_save(cls, dframe):
        """Save records in batches to avoid document size maximum setting.

        :param dframe: A DataFrame to save in the current model.
        """
        def command(records):
            cls.collection.insert(records)

        batch_size = cls.DB_SAVE_BATCH_SIZE

        return cls.__batch_command_wrapper(command, dframe, batch_size)

    @classmethod
    def __batch_update(cls, dframe):
        """Update records in batches to avoid document size maximum setting.

        DataFrame must have column with record (object) ids.

        :param dfarme: The DataFrame to update.
        """
        def command(records):
            # mongodb has no batch updates (fail)
            for record in records:
                spec = {'_id': record['MONGO_RESERVED_KEY_id']}
                del record['MONGO_RESERVED_KEY_id']
                doc = {"$set": record}
                cls.collection.update(spec, doc)

        batch_size = cls.DB_SAVE_BATCH_SIZE

        cls.__batch_command_wrapper(command, dframe, batch_size)

    @classmethod
    def __batch_command_wrapper(cls, command, dframe, batch_size):
        try:
            return cls.__batch_command(command, dframe, batch_size)
        except AutoReconnect:
            batch_size /= 2

            # If batch size drop is less than MIN_BATCH_SIZE, assume the
            # records are too large or there is another error and fail.
            if batch_size >= cls.MIN_BATCH_SIZE:
                cls.__batch_command_wrapper(command, dframe, batch_size)

    @classmethod
    def __batch_command(cls, command, dframe, batch_size):
        batches = int(ceil(float(len(dframe)) / batch_size))

        columns = [DATASET_ID] + sorted(dframe.columns - [DATASET_ID])
        encoding = {v: str(i) for (i, v) in enumerate(columns)}

        for batch in xrange(0, batches):
            start = batch * batch_size
            end = start + batch_size
            records = [replace_keys(row.to_dict(), encoding) for (_, row)
                       in dframe[start:end].iterrows()]
            command(records)

        return encoding
