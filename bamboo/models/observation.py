from math import ceil

from pandas import concat, DataFrame
from pymongo.errors import AutoReconnect

from bamboo.core.frame import BambooFrame, DATASET_ID, INDEX
from bamboo.lib.datetools import now, parse_timestamp_query
from bamboo.lib.mongo import MONGO_ID, MONGO_ID_ENCODED
from bamboo.lib.parsing import parse_columns
from bamboo.lib.query_args import QueryArgs
from bamboo.lib.utils import combine_dicts, invert_dict, replace_keys
from bamboo.models.abstract_model import AbstractModel


def encode(dframe, dataset, add_index=True):
    """Encode the columns for `dataset` to slugs and add ID column.

    The ID column is the dataset_id for dataset.  This is
    used to link observations to a specific dataset.

    :param dframe: The DataFrame to encode.
    :param dataset: The Dataset to use a mapping for.
    :param add_index: Add index to the DataFrame, default True.

    :returns: A modified `dframe` as a BambooFrame.
    """
    dframe = BambooFrame(dframe)

    if add_index:
        dframe = dframe.add_index()

    dframe = dframe.add_id_column(dataset.dataset_id)
    encoded_columns_map = dataset.schema.rename_map_for_dframe(dframe)

    return dframe.rename(columns=encoded_columns_map)


def update_calculations(record, dataset):
    calculations = dataset.calculations(include_aggs=False)

    if len(calculations):
        dframe = DataFrame(data=record, index=[0])
        labels_to_slugs = dataset.schema.labels_to_slugs

        for c in calculations:
            columns = parse_columns(dataset, c.formula, c.name, dframe=dframe)
            record[labels_to_slugs[c.name]] = columns[0][0]

    return record


class Observation(AbstractModel):

    __collectionname__ = 'observations'

    DELETED_AT = '-1'  # use a short code for key
    ENCODING = 'enc'
    ENCODING_DATASET_ID = '%s_%s' % (DATASET_ID, ENCODING)

    @classmethod
    def delete(cls, dataset, index):
        """Delete observation at index for dataset.

        :param dataset: The dataset to delete the observation from.
        :param index: The index of the observation to delete.
        """
        query = {INDEX: index,
                 DATASET_ID: dataset.dataset_id}
        query = cls.encode(query, dataset=dataset)

        cls.__soft_delete(query)

    @classmethod
    def delete_all(cls, dataset, query=None):
        """Delete the observations for `dataset`.

        :param dataset: The dataset to delete observations for.
        :param query: An optional query to restrict deletion.
        """
        query = query or {}
        query.update({DATASET_ID: dataset.dataset_id})
        query = cls.encode(query, dataset=dataset)

        super(cls, cls()).delete(query)

    @classmethod
    def delete_columns(cls, dataset, columns):
        """Delete a column from the dataset."""
        encoding = cls.encoding(dataset)

        cls.unset({cls.ENCODING_DATASET_ID: dataset.dataset_id},
                  {"%s.%s" % (cls.ENCODING, c): 1 for c in columns})

        cls.unset(
            cls.encode({DATASET_ID: dataset.dataset_id}, encoding=encoding),
            cls.encode({c: 1 for c in columns}, encoding=encoding))

    @classmethod
    def delete_encoding(cls, dataset):
        query = {cls.ENCODING_DATASET_ID: dataset.dataset_id}

        super(cls, cls()).delete(query)

    @classmethod
    def encoding(cls, dataset, encoded_dframe=None):
        record = super(cls, cls).find_one({
            cls.ENCODING_DATASET_ID: dataset.dataset_id
        }).record

        if record is None and encoded_dframe is not None:
            encoding = cls.__make_encoding(encoded_dframe)
            cls.__store_encoding(dataset, encoding)

            return cls.encoding(dataset)

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
    def find(cls, dataset, query_args=None, as_cursor=False,
             include_deleted=False):
        """Return observation rows matching parameters.

        :param dataset: Dataset to return rows for.
        :param include_deleted: If True, return delete records, default False.
        :param query_args: An optional QueryArgs to hold the query arguments.

        :raises: `JSONError` if the query could not be parsed.

        :returns: A list of dictionaries matching the passed in `query` and
            other parameters.
        """
        encoding = cls.encoding(dataset) or {}
        query_args = query_args or QueryArgs()

        query_args.query = parse_timestamp_query(query_args.query,
                                                 dataset.schema)
        query_args.encode(encoding, {DATASET_ID: dataset.dataset_id})

        if not include_deleted:
            query = query_args.query
            query[cls.DELETED_AT] = 0
            query_args.query = query

        # exclude deleted at column
        query_args.select = query_args.select or {cls.DELETED_AT: 0}

        distinct = query_args.distinct
        records = super(cls, cls).find(query_args, as_dict=True,
                                       as_cursor=(as_cursor or distinct))

        return records.distinct(encoding.get(distinct, distinct)) if distinct\
            else records

    @classmethod
    def update_from_dframe(cls, dframe, dataset):
        dataset.build_schema(dframe)
        encoded_dframe = encode(dframe.reset_index(), dataset, add_index=False)
        encoding = cls.encoding(dataset)

        cls.__batch_update(encoded_dframe, encoding)
        cls.__store_encoding(dataset, encoding)
        dataset.update_stats(dframe, update=True)

    @classmethod
    def find_one(cls, dataset, index, decode=True):
        """Return row by index.

        :param dataset: The dataset to find the row for.
        :param index: The index of the row to find.
        """
        query = {INDEX: index, DATASET_ID: dataset.dataset_id,
                 cls.DELETED_AT: 0}
        query = cls.encode(query, dataset=dataset)
        decoding = cls.decoding(dataset)
        record = super(cls, cls).find_one(query, as_dict=True)

        return cls(cls.encode(record, encoding=decoding) if decode else record)

    @classmethod
    def append(cls, dframe, dataset):
        """Append an additional dframe to an existing dataset.

        :params dframe: The DataFrame to append.
        :params dataset: The DataSet to add `dframe` to.
        """
        encoded_dframe = encode(dframe, dataset)
        encoding = cls.encoding(dataset, encoded_dframe)

        cls.__batch_save(encoded_dframe, encoding)
        dataset.clear_summary_stats()

    @classmethod
    def save(cls, dframe, dataset):
        """Save data in `dframe` with the `dataset`.

        Encode `dframe` for MongoDB, and add fields to identify it with the
        passed in `dataset`. All column names in `dframe` are converted to
        slugs using the dataset's schema.  The dataset is update to store the
        size of the stored data.

        :param dframe: The DataFrame (or BambooFrame) to store.
        :param dataset: The dataset to store the dframe in.
        """
        # Build schema for the dataset after having read it from file.
        if not dataset.schema:
            dataset.build_schema(dframe)

        encoded_dframe = encode(dframe, dataset)
        encoding = cls.encoding(dataset, encoded_dframe)

        cls.__batch_save(encoded_dframe, encoding)
        dataset.update_stats(dframe)

    @classmethod
    def update(cls, dataset, index, record):
        """Update a dataset row by index.

        The record dictionary will update, not replace, the data in the row at
        index.

        :param dataset: The dataset to update a row for.
        :param dex: The index of the row to update.
        :param record: The dictionary to update the row with.
        """
        previous_record = cls.find_one(dataset, index).record
        previous_record.pop(MONGO_ID)
        record = combine_dicts(previous_record, record)
        record = update_calculations(record, dataset)

        record = cls.encode(record, dataset=dataset)

        cls.delete(dataset, index)

        super(cls, cls()).save(record)

    @classmethod
    def batch_read_dframe_from_cursor(cls, dataset, observations, distinct,
                                      limit):
        """Read a DataFrame from a MongoDB Cursor in batches."""
        dframes = []
        batch = 0
        decoding = cls.decoding(dataset)

        while True:
            start = batch * cls.DB_READ_BATCH_SIZE
            end = start + cls.DB_READ_BATCH_SIZE

            if limit > 0 and end > limit:
                end = limit

            # if there is a limit this may occur, and we are done
            if start >= end:
                break

            current_observations = [
                replace_keys(ob, decoding) for ob in observations[start:end]]

            # if the batches exhausted the data
            if not len(current_observations):
                break

            dframes.append(BambooFrame(current_observations))

            if not distinct:
                observations.rewind()

            batch += 1

        return BambooFrame(concat(dframes) if len(dframes) else [])

    @classmethod
    def __batch_save(cls, dframe, encoding):
        """Save records in batches to avoid document size maximum setting.

        :param dframe: A DataFrame to save in the current model.
        """
        def command(records, encoding):
            cls.collection.insert(records)

        batch_size = cls.DB_SAVE_BATCH_SIZE

        cls.__batch_command_wrapper(command, dframe, encoding, batch_size)

    @classmethod
    def __batch_update(cls, dframe, encoding):
        """Update records in batches to avoid document size maximum setting.

        DataFrame must have column with record (object) ids.

        :param dfarme: The DataFrame to update.
        """
        def command(records, encoding):
            # Encode the reserved key to access the row ID.
            mongo_reserved_key_id = encoding.get(
                MONGO_ID_ENCODED, MONGO_ID_ENCODED)

            # MongoDB has no batch updates.
            for record in records:
                spec = {MONGO_ID: record[mongo_reserved_key_id]}
                del record[mongo_reserved_key_id]
                doc = {'$set': record}
                cls.collection.update(spec, doc)

        cls.__batch_command_wrapper(command, dframe, encoding,
                                    cls.DB_SAVE_BATCH_SIZE)

    @classmethod
    def __batch_command_wrapper(cls, command, dframe, encoding, batch_size):
        try:
            cls.__batch_command(command, dframe, encoding, batch_size)
        except AutoReconnect:
            batch_size /= 2

            # If batch size drop is less than MIN_BATCH_SIZE, assume the
            # records are too large or there is another error and fail.
            if batch_size >= cls.MIN_BATCH_SIZE:
                cls.__batch_command_wrapper(
                    command, dframe, encoding, batch_size)

    @classmethod
    def __batch_command(cls, command, dframe, encoding, batch_size):
        batches = int(ceil(float(len(dframe)) / batch_size))

        for batch in xrange(0, batches):
            start = batch * batch_size
            end = start + batch_size
            current_dframe = dframe[start:end]
            records = cls.__encode_records(current_dframe, encoding)
            command(records, encoding)

    @classmethod
    def __make_encoding(cls, dframe, start=0):
        # Ensure that DATASET_ID is first so that we can guarantee an index.
        columns = [DATASET_ID] + sorted(dframe.columns - [DATASET_ID])
        return {v: str(start + i) for (i, v) in enumerate(columns)}

    @classmethod
    def __encode_records(cls, dframe, encoding):
        return [cls.__encode_record(row.to_dict(), encoding)
                for (_, row) in dframe.iterrows()]

    @classmethod
    def __encode_record(cls, row, encoding):
        encoded = replace_keys(row, encoding)
        encoded[cls.DELETED_AT] = 0

        return encoded

    @classmethod
    def __soft_delete(cls, query):
        cls.collection.update(query,
                              {'$set': {cls.DELETED_AT: now().isoformat()}})

    @classmethod
    def __store_encoding(cls, dataset, encoding):
        """Store encoded columns with dataset.

        :param dataset: The dataset to store the encoding with.
        :param encoding: The encoding for dataset.
        """
        record = {cls.ENCODING_DATASET_ID: dataset.dataset_id,
                  cls.ENCODING: encoding}
        super(cls, cls()).delete({cls.ENCODING_DATASET_ID: dataset.dataset_id})
        super(cls, cls()).save(record)
