import re
import uuid
from time import gmtime, strftime

from celery.task import task
from pandas import rolling_window

from bamboo.core.calculator import calculate_updates, dframe_from_update,\
    propagate
from bamboo.core.frame import BambooFrame, BAMBOO_RESERVED_KEY_PREFIX,\
    DATASET_ID, INDEX, PARENT_DATASET_ID
from bamboo.core.summary import summarize
from bamboo.lib.async import call_async
from bamboo.lib.exceptions import ArgumentError
from bamboo.lib.io import ImportableDataset
from bamboo.lib.query_args import QueryArgs
from bamboo.lib.schema_builder import Schema
from bamboo.lib.utils import combine_dicts, to_list
from bamboo.models.abstract_model import AbstractModel
from bamboo.models.calculation import Calculation
from bamboo.models.observation import Observation

# The format pandas encodes multicolumns in.
strip_pattern = re.compile("\(u'|', u'|'\)")


@task(ignore_result=True)
def delete_task(dataset, query=None):
    """Background task to delete dataset and its associated observations."""
    Observation.delete_all(dataset, query=query)

    if query is None:
        super(dataset.__class__, dataset).delete(
            {DATASET_ID: dataset.dataset_id})
        Observation.delete_encoding(dataset)


class Dataset(AbstractModel, ImportableDataset):

    __collectionname__ = 'datasets'

    # caching keys
    STATS = '_stats'
    ALL = '_all'

    # metadata
    AGGREGATED_DATASETS = BAMBOO_RESERVED_KEY_PREFIX + 'linked_datasets'
    ATTRIBUTION = 'attribution'
    CREATED_AT = 'created_at'
    DESCRIPTION = 'description'
    ID = 'id'
    JOINED_DATASETS = 'joined_datasets'
    LABEL = 'label'
    LICENSE = 'license'
    NUM_COLUMNS = 'num_columns'
    NUM_ROWS = 'num_rows'
    MERGED_DATASETS = 'merged_datasets'
    PARENT_IDS = 'parent_ids'
    PENDING_UPDATES = 'pending_updates'
    SCHEMA = 'schema'
    UPDATED_AT = 'updated_at'

    def __init__(self, record=None):
        super(Dataset, self).__init__(record)
        self.__dframe = None

    @property
    def aggregated_datasets(self):
        return [(self.split_groups(group), self.find_one(_id)) for (
            group, _id) in self.aggregated_datasets_dict.items()]

    @property
    def aggregated_datasets_dict(self):
        return self.record.get(self.AGGREGATED_DATASETS, {})

    @property
    def attribution(self):
        return self.record.get(self.ATTRIBUTION)

    @property
    def columns(self):
        return self.schema.keys() if self.num_rows else []

    @property
    def dataset_id(self):
        return self.record[DATASET_ID]

    @property
    def description(self):
        return self.record.get(self.DESCRIPTION)

    @property
    def joined_datasets(self):
        # TODO: fetch all datasets in single DB call
        # (let Dataset.find take a list of IDs)
        return [
            (direction, self.find_one(other_dataset_id), on,
             self.find_one(joined_dataset_id))
            for direction, other_dataset_id, on, joined_dataset_id in
            self.joined_dataset_ids]

    @property
    def joined_dataset_ids(self):
        return [
            tuple(_list) for _list in self.record.get(self.JOINED_DATASETS, [])
        ]

    @property
    def label(self):
        return self.record.get(self.LABEL)

    @property
    def labels(self):
        return [column[self.LABEL] for column in self.schema.values()]

    @property
    def license(self):
        return self.record.get(self.LICENSE)

    @property
    def merged_datasets(self):
        return self.__linked_datasets(self.merged_dataset_ids)

    @property
    def merged_datasets_with_map(self):
        results = self.merged_dataset_info

        if len(results):
            mappings, ids = zip(*results)
            results = zip(mappings, self.__linked_datasets(ids))

        return results

    @property
    def merged_dataset_ids(self):
        results = self.merged_dataset_info
        return zip(*results)[-1] if results else results

    @property
    def merged_dataset_info(self):
        return self.record.get(self.MERGED_DATASETS, [])

    @property
    def num_columns(self):
        return self.record.get(self.NUM_COLUMNS, 0)

    @property
    def num_rows(self):
        return self.record.get(self.NUM_ROWS, 0)

    @property
    def on_columns_for_rhs_of_joins(self):
        return [on for direction, _, on, __ in
                self.joined_datasets if direction == 'left']

    @property
    def parent_ids(self):
        query_args = QueryArgs(select={PARENT_DATASET_ID: 1},
                               distinct=PARENT_DATASET_ID)
        return self.observations(query_args)

    @property
    def pending_updates(self):
        return self.record[self.PENDING_UPDATES]

    @property
    def schema(self):
        schema_dict = {}

        if self.record:
            schema_dict = self.record.get(self.SCHEMA)

        return Schema.safe_init(schema_dict)

    @property
    def stats(self):
        return self.record.get(self.STATS, {})

    @property
    def updatable_keys(self):
        return [self.LABEL, self.DESCRIPTION, self.LICENSE, self.ATTRIBUTION]

    @property
    def __is_cached(self):
        return self.__dframe is not None

    @classmethod
    def create(cls, dataset_id=None):
        return super(cls, cls).create(dataset_id)

    @classmethod
    def find(cls, dataset_id):
        """Return datasets for `dataset_id`."""
        query_args = QueryArgs(query={DATASET_ID: dataset_id})
        return super(cls, cls).find(query_args)

    @classmethod
    def find_one(cls, dataset_id):
        """Return dataset for `dataset_id`."""
        return super(cls, cls).find_one({DATASET_ID: dataset_id})

    def __linked_datasets(self, ids):
        return [self.find_one(_id) for _id in ids]

    def add_joined_dataset(self, new_data):
        """Add the ID of `new_dataset` to the list of joined datasets."""
        self.__add_linked_data(self.JOINED_DATASETS, self.joined_dataset_ids,
                               new_data)

    def add_merged_dataset(self, mapping, new_dataset):
        """Add the ID of `new_dataset` to the list of merged datasets."""
        self.__add_linked_data(self.MERGED_DATASETS, self.merged_dataset_info,
                               [mapping, new_dataset.dataset_id])

    def add_observations(self, new_data):
        """Update `dataset` with `new_data`."""
        update_id = uuid.uuid4().hex
        self.add_pending_update(update_id)

        new_data = to_list(new_data)

        # fetch data before other updates
        new_dframe_raw = dframe_from_update(self, new_data)

        call_async(calculate_updates, self, new_data,
                   new_dframe_raw=new_dframe_raw, update_id=update_id)

    def add_pending_update(self, update_id):
        self.collection.update(
            {'_id': self.record['_id']},
            {'$push': {self.PENDING_UPDATES: update_id}})

    def aggregated_dataset(self, groups):
        groups = to_list(groups)
        _id = self.aggregated_datasets_dict.get(self.join_groups(groups))

        return self.find_one(_id) if _id else None

    def append_observations(self, dframe):
        Observation.append(dframe, self)
        self.update({self.NUM_ROWS: self.num_rows + len(dframe)})

        # to update cardinalities here we need to refetch the full DataFrame.
        dframe = self.dframe(keep_parent_ids=True)
        self.build_schema(dframe)
        self.update_stats(dframe)

    def build_schema(self, dframe, overwrite=False, set_num_columns=True):
        """Build schema for a dataset.

        If no schema exists, build a schema from the passed `dframe` and store
        that schema for this dataset.  Otherwise, if a schema does exist, build
        a schema for the passed `dframe` and merge this schema with the current
        schema.  Keys in the new schema replace keys in the current schema but
        keys in the current schema not in the new schema are retained.

        If `set_num_columns` is True the number of columns will be set to the
        number of keys (columns) in the new schema.

        :param dframe: The DataFrame whose schema to merge with the current
            schema.
        :param overwrite: If true replace schema, otherwise update.
        :param set_num_columns: If True also set the number of columns.
        """
        new_schema = self.schema.rebuild(dframe, overwrite)
        self.set_schema(new_schema,
                        set_num_columns=(set_num_columns or overwrite))

    def calculations(self, include_aggs=True, only_aggs=False):
        """Return the calculations for this dataset.

        :param include_aggs: Include aggregations, default True.
        :param only_aggs: Exclude non-aggregations, default False.
        """
        return Calculation.find(self, include_aggs, only_aggs)

    def cardinality(self, col):
        return self.schema.cardinality(col)

    def clear_cache(self):
        self.__dframe = None

        return self

    def clear_summary_stats(self, group=None, column=None):
        """Remove summary stats for `group` and optional `column`.

        By default will remove all stats.

        :param group: The group to remove stats for, default None.
        :param column: The column to remove stats for, default None.
        """
        stats = self.stats

        if stats:
            if column:
                stats_for_field = stats.get(group or self.ALL)

                if stats_for_field:
                    stats_for_field.pop(column, None)
            elif group:
                stats.pop(group, None)
            else:
                stats = {}

            self.update({self.STATS: stats})

    def count(self, query_args=None):
        """Return the count of rows matching query in dataset.

        :param query_args: An optional QueryArgs to hold the query arguments.
        """
        query_args = query_args or QueryArgs()
        obs = self.observations(query_args, as_cursor=True)

        count = len(obs) if query_args.distinct else obs.count()

        limit = query_args.limit
        if limit > 0 and count > limit:
            count = limit

        return count

    def delete(self, query=None, countdown=0):
        """Delete this dataset.

        :param countdown: Delete dataset after this number of seconds.
        """
        call_async(delete_task, self.clear_cache(), query=query,
                   countdown=countdown)

    def delete_columns(self, columns):
        """Delete column `column` from this dataset.

        :param column: The column to delete.
        """
        columns = set(self.schema.keys()).intersection(set(to_list(columns)))

        if not len(columns):
            raise ArgumentError("Columns: %s not in dataset.")

        Observation.delete_columns(self, columns)
        new_schema = self.schema

        for c in columns:
            del new_schema[c]

        self.set_schema(new_schema, set_num_columns=True)

        return columns

    def delete_observation(self, index):
        """Delete observation at index.

        :params index: The index of an observation to delete.
        """
        Observation.delete(self, index)

        dframe = self.dframe()
        self.update({self.NUM_ROWS: len(dframe)})
        self.build_schema(dframe, overwrite=True)
        call_async(propagate, self, update={'delete': index})

    def dframe(self, query_args=None, keep_parent_ids=False, padded=False,
               index=False, reload_=False, keep_mongo_keys=False):
        """Fetch the dframe for this dataset.

        :param query_args: An optional QueryArgs to hold the query arguments.
        :param keep_parent_ids: Do not remove parent IDs from the dframe,
            default False.
        :param padded: Used for joining, default False.
        :param index: Return the index with dframe, default False.
        :param reload_: Force refresh of data, default False.
        :param keep_mongo_keys: Used for updating documents, default False.

        :returns: Return BambooFrame with contents based on query parameters
            passed to MongoDB. BambooFrame will not have parent ids if
            `keep_parent_ids` is False.
        """
        # bypass cache if we need specific version
        cacheable = not (query_args or keep_parent_ids or padded)

        # use cached copy if we have already fetched it
        if cacheable and not reload_ and self.__is_cached:
            return self.__dframe

        query_args = query_args or QueryArgs()
        observations = self.observations(query_args, as_cursor=True)

        if query_args.distinct:
            return BambooFrame(observations)

        dframe = Observation.batch_read_dframe_from_cursor(
            self, observations, query_args.distinct, query_args.limit)

        dframe.decode_mongo_reserved_keys(keep_mongo_keys=keep_mongo_keys)

        excluded = [keep_parent_ids and PARENT_DATASET_ID, index and INDEX]
        dframe.remove_bamboo_reserved_keys(filter(bool, excluded))

        if index:
            dframe = BambooFrame(dframe.rename(columns={INDEX: 'index'}))

        dframe = self.__maybe_pad(dframe, padded)

        if cacheable:
            self.__dframe = dframe

        return dframe

    def has_pending_updates(self, update_id):
        """Check if this dataset has pending updates.

        Call the update identfied by `update_id` the current update. A dataset
        has pending updates if, not including the current update, there are any
        pending updates and the update at the top of the queue is not the
        current update.

        :param update_id: An update to exclude when checking for pending
            updates.
        :returns: True if there are pending updates, False otherwise.
        """
        self.reload()
        pending_updates = self.pending_updates

        return pending_updates[0] != update_id and len(
            set(pending_updates) - set([update_id]))

    def info(self, update=None):
        """Return or update meta-data for this dataset.

        :param update: Dictionary to update info with, default None.
        :returns: Dictionary of info for this dataset.
        """
        if update:
            update_dict = {key: value for key, value in update.items()
                           if key in self.updatable_keys}
            self.update(update_dict)

        return {
            self.ID: self.dataset_id,
            self.LABEL: self.label,
            self.DESCRIPTION: self.description,
            self.SCHEMA: self.schema,
            self.LICENSE: self.license,
            self.ATTRIBUTION: self.attribution,
            self.CREATED_AT: self.record.get(self.CREATED_AT),
            self.UPDATED_AT: self.record.get(self.UPDATED_AT),
            self.NUM_COLUMNS: self.num_columns,
            self.NUM_ROWS: self.num_rows,
            self.STATE: self.state,
            self.PARENT_IDS: self.parent_ids,
        }

    def is_dimension(self, col):
        return self.schema.is_dimension(col)

    def is_factor(self, col):
        return self.is_dimension(col) or self.schema.is_date_simpletype(col)

    def join(self, other, on):
        """Join with dataset `other` on the passed columns.

        :param other: The other dataset to join.
        :param on: The column in this and the `other` dataset to join on.
        """
        merged_dframe = self.dframe()

        if not len(merged_dframe.columns):
            # Empty dataset, simulate columns
            merged_dframe = self.place_holder_dframe()

        merged_dframe = merged_dframe.join_dataset(other, on)
        merged_dataset = self.create()

        if self.num_rows and other.num_rows:
            merged_dataset.save_observations(merged_dframe)
        else:
            merged_dataset.build_schema(merged_dframe, set_num_columns=True)
            merged_dataset.ready()

        self.add_joined_dataset(
            ('right', other.dataset_id, on, merged_dataset.dataset_id))
        other.add_joined_dataset(
            ('left', self.dataset_id, on, merged_dataset.dataset_id))

        return merged_dataset

    def observations(self, query_args=None, as_cursor=False):
        """Return observations for this dataset.

        :param query_args: An optional QueryArgs to hold the query arguments.
        :param as_cursor: Return the observations as a cursor.
        """
        return Observation.find(self, query_args or QueryArgs(),
                                as_cursor=as_cursor)

    def place_holder_dframe(self, dframe=None):
        columns = self.schema.keys()
        if dframe is not None:
            columns = [col for col in columns if col not in dframe.columns[1:]]

        return BambooFrame([[''] * len(columns)], columns=columns)

    def reload(self):
        """Reload the dataset from DB and clear any cache."""
        dataset = Dataset.find_one(self.dataset_id)
        self.record = dataset.record
        self.clear_cache()

        return self

    def remove_parent_observations(self, parent_id):
        """Remove obervations for this dataset with the passed `parent_id`.

        :param parent_id: Remove observations with this ID as their parent
            dataset ID.
        """
        Observation.delete_all(self, {PARENT_DATASET_ID: parent_id})
        # clear the cached dframe
        self.__dframe = None

    def replace_observations(self, dframe, overwrite=False,
                             set_num_columns=True):
        """Remove all rows for this dataset and save the rows in `dframe`.

        :param dframe: Replace rows in this dataset with this DataFrame's rows.
        :param overwrite: If true replace the schema, otherwise update it.
            Default False.
        :param set_num_columns: If true update the dataset stored number of
            columns.  Default True.

        :returns: BambooFrame equivalent to the passed in `dframe`.
        """
        self.build_schema(dframe, overwrite=overwrite,
                          set_num_columns=set_num_columns)
        Observation.delete_all(self)

        return self.save_observations(dframe)

    def resample(self, date_column, interval, how, query=None):
        """Resample a dataset given a new time frame.

        :param date_column: The date column use as the index for resampling.
        :param interval: The interval code for resampling.
        :param how: How to aggregate in the resample.
        :returns: A BambooFrame of the resampled DataFrame for this dataset.
        """
        query_args = QueryArgs(query=query)
        dframe = self.dframe(query_args).set_index(date_column)
        resampled = dframe.resample(interval, how=how)
        return BambooFrame(resampled.reset_index())

    def rolling(self, win_type, window):
        """Calculate a rolling window over all numeric columns.

        :param win_type: The type of window, see pandas pandas.rolling_window.
        :param window: The number of observations used for calculating the
            window.
        :returns: A BambooFrame of the rolling window calculated for this
            dataset.
        """
        dframe = self.dframe(QueryArgs(select=self.schema.numerics_select))
        return BambooFrame(rolling_window(dframe, window, win_type))

    def save(self, dataset_id=None):
        """Store dataset with `dataset_id` as the unique internal ID.

        Store a new dataset with an ID given by `dataset_id` is exists,
        otherwise reate a random UUID for this dataset. Additionally, set the
        created at time to the current time and the state to pending.

        :param dataset_id: The ID to store for this dataset, default is None.

        :returns: A dict representing this dataset.
        """
        if dataset_id is None:
            dataset_id = uuid.uuid4().hex

        record = {
            DATASET_ID: dataset_id,
            self.AGGREGATED_DATASETS: {},
            self.CREATED_AT: strftime("%Y-%m-%d %H:%M:%S", gmtime()),
            self.STATE: self.STATE_PENDING,
            self.PENDING_UPDATES: [],
        }

        return super(self.__class__, self).save(record)

    def save_observations(self, dframe):
        """Save rows in `dframe` for this dataset.

        :param dframe: DataFrame to save rows from.
        """
        return Observation.save(dframe, self)

    def set_olap_type(self, column, olap_type):
        """Set the OLAP Type for this `column` of dataset.

        Only columns with an original OLAP Type of 'measure' can be modified.
        This includes columns with Simple Type integer, float, and datetime.

        :param column: The column to set the OLAP Type for.
        :param olap_type: The OLAP Type to set. Must be 'dimension' or
            'measure'.
        """
        schema = self.schema
        schema.set_olap_type(column, olap_type)

        self.set_schema(schema, False)

        # Build summary for new type.
        self.summarize(self.dframe(), update=True)

    def set_schema(self, schema, set_num_columns=True):
        """Set the schema from an existing one."""
        update_dict = {self.SCHEMA: schema}

        if set_num_columns:
            update_dict.update({self.NUM_COLUMNS: len(schema.keys())})

        self.update(update_dict)

    def summarize(self, dframe, groups=[], no_cache=False, update=False,
                  flat=False):
        """Build and return a summary of the data in this dataset.

        Return a summary of dframe grouped by `groups`, or the overall
        summary if no groups are specified.

        :param dframe: dframe to summarize
        :param groups: A list of columns to group on.
        :param no_cache: Do not fetch a cached summary.
        :param flat: Return a flattened list of groups.

        :returns: A summary of the dataset as a dict. Numeric columns will be
            summarized by the arithmetic mean, standard deviation, and
            percentiles. Dimensional columns will be summarized by counts.
        """
        self.reload()

        summary = summarize(self, dframe, groups, no_cache, update=update)

        if flat:
            flat_summary = []

            for cols, v in summary.iteritems():
                cols = self.split_groups(cols)

                for k, data in v.iteritems():
                    col_values = self.split_groups(k)
                    col_values = [strip_pattern.sub(',', i)[1:-1]
                                  for i in col_values]
                    flat_summary.append(
                        combine_dicts(dict(zip(cols, col_values)), data))

            summary = flat_summary

        return summary

    def update(self, record):
        """Update dataset `dataset` with `record`."""
        record[self.UPDATED_AT] = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        super(self.__class__, self).update(record)

    def update_observation(self, index, data):
        # check that update is valid
        dframe_from_update(self, [data])
        Observation.update(self, index, data)
        call_async(propagate, self, update={'edit': [index, data]})

    def update_observations(self, dframe):
        return Observation.update_from_dframe(dframe, self)

    def update_complete(self, update_id):
        """Remove `update_id` from this datasets list of pending updates.

        :param update_id: The ID of the completed update.
        """
        self.collection.update(
            {'_id': self.record['_id']},
            {'$pull': {self.PENDING_UPDATES: update_id}})

    def update_stats(self, dframe, update=False):
        """Update store statistics for this dataset.

         :param dframe: Use this DataFrame for summary statistics.
         :param update: Update or replace summary statistics, default False.
        """
        self.update({
            self.NUM_ROWS: len(dframe),
            self.STATE: self.STATE_READY,
        })
        self.summarize(dframe, update=update)

    def __add_linked_data(self, link_key, existing_data, new_data):
        self.update({link_key: existing_data + [new_data]})

    def __maybe_pad(self, dframe, pad):
        if pad:
            if len(dframe.columns):
                on = dframe.columns[0]
                place_holder = self.place_holder_dframe(dframe).set_index(on)
                dframe = BambooFrame(dframe.join(place_holder, on=on))
            else:
                dframe = self.place_holder_dframe()

        return dframe
