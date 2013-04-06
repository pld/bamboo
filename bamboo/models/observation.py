from math import ceil

from pymongo.errors import AutoReconnect

from bamboo.core.frame import BambooFrame, DATASET_ID, INDEX
from bamboo.lib.datetools import parse_timestamp_query
from bamboo.lib.mongo import MONGO_ID, MONGO_RESERVED_KEY_ID
from bamboo.lib.query_args import QueryArgs
from bamboo.lib.utils import invert_dict, replace_keys
from bamboo.models.abstract_model import AbstractModel


class Observation(AbstractModel):

    __collectionname__ = 'observations'
    ENCODING = 'encoding'
    DATASET_ID_ENCODING = DATASET_ID + ENCODING

    @classmethod
    def delete(cls, dataset, index):
        query = {INDEX: index,
                 DATASET_ID: dataset.dataset_id}
        query = cls.encode(query, dataset=dataset)

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
        query = cls.encode(query, dataset=dataset)

        super(cls, cls()).delete(query)

    @classmethod
    def encoding(cls, dataset):
        record = super(cls, cls).find_one({
            cls.DATASET_ID_ENCODING: dataset.dataset_id
        }).record
        return record[cls.ENCODING] if record else None

    @classmethod
    def encode(cls, dict_, dataset=None, encoding=None):
        if dataset:
            encoding = cls.encoding(dataset)
        return replace_keys(dict_, encoding) if encoding else dict_

    @classmethod
    def decoding(cls, dataset):
        return invert_dict(cls.encoding(dataset))

    @classmethod
    def find(cls, dataset, query_args=None, as_cursor=False):
        """Return observation rows matching parameters.

        :param dataset: Dataset to return rows for.
        :param query_args: An optional QueryArgs to hold the query arguments.

        :raises: `JSONError` if the query could not be parsed.

        :returns: A list of dictionaries matching the passed in `query` and
            other parameters.
        """
        id_query = {DATASET_ID: dataset.dataset_id}
        encoding = cls.encoding(dataset) or {}

        if query_args:
            query = parse_timestamp_query(query_args.query, dataset.schema)
            query.update(id_query)
            query_args.query = cls.encode(query, encoding=encoding)

            order_by = query_args.order_by
            query_args.order_by = order_by and cls.encode(
                dict(order_by), encoding=encoding).items()

            select = query_args.select
            query_args.select = select and cls.encode(
                select, encoding=encoding)
        else:
            query_args = QueryArgs()
            query_args.query = cls.encode(id_query, encoding=encoding)

        distinct = query_args.distinct

        records = super(cls, cls).find(query_args, as_dict=True,
                                       as_cursor=(as_cursor or distinct))

        if query_args.distinct:
            records = records.distinct(encoding.get(distinct, distinct))

        return records

    @classmethod
    def update_from_dframe(cls, dframe, dataset):
        dataset.build_schema(dframe)

        encoded_dframe = dframe.reset_index()

        if not DATASET_ID in encoded_dframe.columns:
            encoded_dframe = dataset.encode_dframe_columns(encoded_dframe)

        cls.__batch_update(encoded_dframe)

        cls.__update_dataset_stats(dframe, dataset)

    @classmethod
    def find_one(cls, dataset, index, decode=True):
        """Return row by index.

        :param dataset: The dataset to find the row for.
        :param index: The index of the row to find.
        """
        query = {INDEX: index,
                 DATASET_ID: dataset.dataset_id}
        query = cls.encode(query, dataset=dataset)
        decoding = cls.decoding(dataset)
        record = super(cls, cls).find_one(query, as_dict=True)

        return cls(cls.encode(record, encoding=decoding) if decode else record)

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

        # Add indx and encode columns.
        encoded_dframe = dataset.encode_dframe_columns(
            cls.__add_index_to_dframe(dframe))


        encoding = cls.__batch_save(encoded_dframe)

        # Store newly encoded columns.
        record = {cls.DATASET_ID_ENCODING: dataset.dataset_id,
                  cls.ENCODING: encoding}
        super(cls, cls()).delete({cls.DATASET_ID_ENCODING: dataset.dataset_id})
        super(cls, cls()).save(record)

        cls.__update_dataset_stats(dframe, dataset)

    @classmethod
    def update(cls, dataset, index, record):
        """Update a dataset row by index.

        The record dictionary will update, not replace, the data in the row at
        index.

        :param dataset: The dataset to update a row for.
        :param dex: The index of the row to update.
        :param record: The dictionary to update the row with.
        """
        observation = cls.find_one(dataset, index, decode=False)
        record = cls.encode(record, dataset=dataset)
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
        def command(records, current_dframe):
            cls.collection.insert(records)

        batch_size = cls.DB_SAVE_BATCH_SIZE

        return cls.__batch_command_wrapper(command, dframe, batch_size)

    @classmethod
    def __batch_update(cls, dframe):
        """Update records in batches to avoid document size maximum setting.

        DataFrame must have column with record (object) ids.

        :param dfarme: The DataFrame to update.
        """
        def command(records, encoding):
            # Encode the reserved key to access the row ID.
            mongo_reserved_key_id = encoding[MONGO_RESERVED_KEY_ID]

            # MongoDB has no batch updates.
            for record in records:
                spec = {MONGO_ID: record[mongo_reserved_key_id]}
                del record[mongo_reserved_key_id]
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

        encoding = cls.__make_encoding(dframe)

        for batch in xrange(0, batches):
            start = batch * batch_size
            end = start + batch_size
            current_dframe = dframe[start:end]
            records = cls.__encode_records(current_dframe, encoding)
            command(records, encoding)

        return encoding

    @classmethod
    def __make_encoding(cls, dframe):
        columns = [DATASET_ID] + sorted(dframe.columns - [DATASET_ID])
        return {v: str(i) for (i, v) in enumerate(columns)}

    @classmethod
    def __encode_records(cls, dframe, encoding):
        return [replace_keys(row.to_dict(), encoding) for (_, row)
                in dframe.iterrows()]

    @classmethod
    def __update_dataset_stats(cls, dframe, dataset):
        # add metadata to dataset, discount ID column
        dataset.update({
            dataset.NUM_ROWS: len(dframe),
            dataset.STATE: cls.STATE_READY,
        })
        dataset.summarize(dframe)
