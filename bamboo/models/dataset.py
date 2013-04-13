import uuid
from time import gmtime, strftime

from celery.task import task
from pandas import rolling_window, Series

from bamboo.core.calculator import Calculator
from bamboo.core.frame import BambooFrame, BAMBOO_RESERVED_KEY_PREFIX,\
    DATASET_ID, INDEX, PARENT_DATASET_ID
from bamboo.core.summary import summarize
from bamboo.lib.async import call_async
from bamboo.lib.io import ImportableDataset
from bamboo.lib.query_args import QueryArgs
from bamboo.lib.schema_builder import Schema
from bamboo.lib.utils import to_list
from bamboo.models.abstract_model import AbstractModel
from bamboo.models.calculation import Calculation
from bamboo.models.observation import Observation


@task(ignore_result=True)
def delete_task(dataset):
    """Background task to delete dataset and its associated observations."""
    Observation.delete_all(dataset)
    super(dataset.__class__, dataset).delete({DATASET_ID: dataset.dataset_id})


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
    def dataset_id(self):
        return self.record[DATASET_ID]

    @property
    def num_columns(self):
        return self.record.get(self.NUM_COLUMNS, 0)

    @property
    def num_rows(self):
        return self.record.get(self.NUM_ROWS, 0)

    @property
    def schema(self):
        schema_dict = {}

        if self.record:
            schema_dict = self.record.get(self.SCHEMA)

        return Schema.safe_init(schema_dict)

    @property
    def labels(self):
        return [column[self.LABEL] for column in self.schema.values()]

    @property
    def label(self):
        return self.record.get(self.LABEL)

    @property
    def description(self):
        return self.record.get(self.DESCRIPTION)

    @property
    def license(self):
        return self.record.get(self.LICENSE)

    @property
    def attribution(self):
        return self.record.get(self.ATTRIBUTION)

    @property
    def stats(self):
        return self.record.get(self.STATS, {})

    @property
    def aggregated_datasets_dict(self):
        return self.record.get(self.AGGREGATED_DATASETS, {})

    @property
    def aggregated_datasets(self):
        return [(self.split_groups(group), self.find_one(_id)) for (
            group, _id) in self.aggregated_datasets_dict.items()]

    @property
    def joined_dataset_ids(self):
        return [
            tuple(_list) for _list in self.record.get(self.JOINED_DATASETS, [])
        ]

    @property
    def joined_datasets(self):
        # TODO: fetch all datasets in single DB call
        return [
            (direction, self.find_one(other_dataset_id), on,
             self.find_one(joined_dataset_id))
            for direction, other_dataset_id, on, joined_dataset_id in
            self.joined_dataset_ids]

    @property
    def merged_dataset_info(self):
        return self.record.get(self.MERGED_DATASETS, [])

    @property
    def merged_dataset_ids(self):
        results = self.merged_dataset_info
        return zip(*results)[-1] if results else results

    @property
    def merged_datasets(self):
        return self._linked_datasets(self.merged_dataset_ids)

    @property
    def merged_datasets_with_map(self):
        results = self.merged_dataset_info

        if len(results):
            mappings, ids = zip(*results)
            results = zip(mappings, self._linked_datasets(ids))

        return results

    @property
    def pending_updates(self):
        return self.record[self.PENDING_UPDATES]

    @property
    def updatable_keys(self):
        return [self.LABEL, self.DESCRIPTION, self.LICENSE, self.ATTRIBUTION]

    def _linked_datasets(self, ids):
        return [self.find_one(_id) for _id in ids]

    def is_factor(self, col):
        return self.schema.is_dimension(col)

    def cardinality(self, col):
        return self.schema.cardinality(col)

    def aggregated_dataset(self, groups):
        groups = to_list(groups)
        _id = self.aggregated_datasets_dict.get(self.join_groups(groups))

        return self.find_one(_id) if _id else None

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
        if cacheable and not reload_ and self.__dframe is not None:
            return self.__dframe

        query_args = query_args or QueryArgs()
        observations = self.observations(query_args, as_cursor=True)

        if query_args.distinct:
            return BambooFrame(observations)

        dframe = Observation.batch_read_dframe_from_cursor(
            self, observations, query_args.distinct, query_args.limit)

        dframe.decode_mongo_reserved_keys(keep_mongo_keys=keep_mongo_keys)

        excluded = []

        if keep_parent_ids:
            excluded.append(PARENT_DATASET_ID)
        if index:
            excluded.append(INDEX)

        dframe.remove_bamboo_reserved_keys(excluded)

        if index:
            dframe = BambooFrame(dframe.rename(columns={INDEX: 'index'}))

        if padded:
            if len(dframe.columns):
                on = dframe.columns[0]
                place_holder = self.place_holder_dframe(dframe).set_index(on)
                dframe = BambooFrame(dframe.join(place_holder, on=on))
            else:
                dframe = self.place_holder_dframe()

        if cacheable:
            self.__dframe = dframe

        return dframe

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

    def add_joined_dataset(self, new_data):
        """Add the ID of `new_dataset` to the list of joined datasets."""
        self.__add_linked_data(self.JOINED_DATASETS, self.joined_dataset_ids,
                               new_data)

    def add_merged_dataset(self, mapping, new_dataset):
        """Add the ID of `new_dataset` to the list of merged datasets."""
        self.__add_linked_data(self.MERGED_DATASETS, self.merged_dataset_info,
                               [mapping, new_dataset.dataset_id])

    def clear_summary_stats(self, group=ALL, column=None):
        """Remove summary stats for `group` and optional `column`."""
        stats = self.stats

        if stats:
            if column:
                stats_for_field = stats.get(group)

                if stats_for_field:
                    stats_for_field.pop(column, None)
            else:
                stats.pop(group, None)
            self.update({self.STATS: stats})

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

    def delete(self, countdown=0):
        """Delete this dataset.

        :param countdown: Delete dataset after this number of seconds.
        """
        call_async(delete_task, self.clear_cache(), countdown=countdown)

    def summarize(self, dframe, groups=[], no_cache=False, update=False):
        """Build and return a summary of the data in this dataset.

        Return a summary of dframe grouped by `groups`, or the overall
        summary if no groups are specified.

        :param dframe: dframe to summarize
        :param groups: A list of columns to group on.
        :param no_cache: Do not fetch a cached summary.

        :returns: A summary of the dataset as a dict. Numeric columns will be
            summarized by the arithmetic mean, standard deviation, and
            percentiles. Dimensional columns will be summarized by counts.
        """
        self.reload()

        return summarize(self, dframe, groups, no_cache, update=update)

    @classmethod
    def create(cls, dataset_id=None):
        return super(cls, cls).create(dataset_id)

    @classmethod
    def find_one(cls, dataset_id):
        """Return dataset for `dataset_id`."""
        return super(cls, cls).find_one({DATASET_ID: dataset_id})

    @classmethod
    def find(cls, dataset_id):
        """Return datasets for `dataset_id`."""
        query_args = QueryArgs(query={DATASET_ID: dataset_id})
        return super(cls, cls).find(query_args)

    def update(self, record):
        """Update dataset `dataset` with `record`."""
        record[self.UPDATED_AT] = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        super(self.__class__, self).update(record)

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

    def set_schema(self, schema, set_num_columns=True):
        """Set the schema from an existing one."""
        update_dict = {self.SCHEMA: schema}

        if set_num_columns:
            update_dict.update({self.NUM_COLUMNS: len(schema.keys())})

        self.update(update_dict)

    def info(self, update=None):
        """Return or update meta-data for this dataset.

        :param update: Dictionary to update info with, default None.
        :returns: Dictionary of info for this dataset.
        """
        if update:
            update_dict = {key: value for key, value in update.items()
                           if key in self.updatable_keys}
            self.update(update_dict)

        query_args = QueryArgs(select={PARENT_DATASET_ID: 1},
                               distinct=PARENT_DATASET_ID)
        parent_ids = self.observations(query_args)

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
            self.PARENT_IDS: parent_ids,
        }

    def observations(self, query_args=None, as_cursor=False):
        """Return observations for this dataset.

        :param query_args: An optional QueryArgs to hold the query arguments.
        :param as_cursor: Return the observations as a cursor.
        """
        query_args = query_args or QueryArgs()

        return Observation.find(self, query_args, as_cursor=as_cursor)

    def calculations(self):
        """Return the calculations for this dataset."""
        return Calculation.find(self)

    def remove_parent_observations(self, parent_id):
        """Remove obervations for this dataset with the passed `parent_id`.

        :param parent_id: Remove observations with this ID as their parent
            dataset ID.
        """
        Observation.delete_all(self, {PARENT_DATASET_ID: parent_id})
        # clear the cached dframe
        self.__dframe = None

    def add_observations(self, new_data):
        """Update `dataset` with `new_data`."""
        update_id = uuid.uuid4().hex
        self.add_pending_update(update_id)

        new_data = to_list(new_data)

        calculator = Calculator(self)

        new_dframe_raw = calculator.dframe_from_update(
            new_data, self.schema.labels_to_slugs)
        calculator._check_update_is_valid(new_dframe_raw)

        calculator.dataset.clear_cache()
        call_async(calculator.calculate_updates, calculator, new_data,
                   new_dframe_raw=new_dframe_raw, update_id=update_id)

    def add_pending_update(self, update_id):
        self.collection.update(
            {'_id': self.record['_id']},
            {'$push': {self.PENDING_UPDATES: update_id}})

    def delete_observation(self, index):
        """Delete observation at index.

        :params index: The index of an observation to delete.
        """
        Observation.delete(self, index)

        dframe = self.dframe()
        self.update({self.NUM_ROWS: len(dframe)})
        self.build_schema(dframe, overwrite=True)

    def save_observations(self, dframe):
        """Save rows in `dframe` for this dataset.

        :param dframe: DataFrame to save rows from.
        """
        return Observation.save(dframe, self)

    def update_observations(self, dframe):
        return Observation.update_from_dframe(dframe, self)

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

    def drop_columns(self, columns):
        """Remove columns from this dataset's observations.

        :param columns: List of columns to remove from this dataset.
        """
        dframe = self.dframe(keep_parent_ids=True)
        self.replace_observations(dframe.drop(columns, axis=1), overwrite=True)

    def place_holder_dframe(self, dframe=None):
        columns = self.schema.keys()
        if dframe is not None:
            columns = [col for col in columns if col not in dframe.columns[1:]]

        return BambooFrame([[''] * len(columns)], columns=columns)

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

    def reload(self):
        dataset = Dataset.find_one(self.dataset_id)
        self.record = dataset.record
        # XXX do we really need to clear the cached dframe?
        self.clear_cache()

        return self

    def clear_cache(self):
        self.__dframe = None

        return self

    def add_id_column(self, dframe):
        id_column = Series([self.dataset_id] * len(dframe))
        id_column.name = DATASET_ID

        return BambooFrame(dframe.join(id_column))

    def encode_dframe_columns(self, dframe):
        """Encode the columns in `dframe` to slugs and add ID column.

        The ID column is the dataset_id for this dataset.  This is
        used to link observations to a specific dataset.

        :param dframe: The DataFame to rename columns in and add an ID column
            to.
        :returns: A modified `dframe` as a BambooFrame.
        """
        dframe = self.add_id_column(dframe)
        encoded_columns_map = self.schema.rename_map_for_dframe(dframe)
        dframe = dframe.rename(columns=encoded_columns_map)

        return BambooFrame(dframe)

    def new_agg_dataset(self, dframe, groups):
        """Create an aggregated dataset for this dataset.

        Creates and saves a dataset from the given `dframe`.  Then stores this
        dataset as an aggregated dataset given `groups` for `self`.

        :param dframe: The DataFrame to store in the new aggregated dataset.
        :param groups: The groups associated with this aggregated dataset.
        :returns: The newly created aggregated dataset.
        """
        agg_dataset = self.create()
        agg_dataset.save_observations(dframe)

        # store a link to the new dataset
        group_str = self.join_groups(groups)
        agg_datasets_dict = self.aggregated_datasets_dict
        agg_datasets_dict[group_str] = agg_dataset.dataset_id
        self.update({
            self.AGGREGATED_DATASETS: agg_datasets_dict})

        return agg_dataset

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
        dframe = self.dframe()[self.schema.numeric_slugs]
        return BambooFrame(rolling_window(dframe, window, win_type))

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

    def __add_linked_data(self, link_key, existing_data, new_data):
        self.update({link_key: existing_data + [new_data]})
