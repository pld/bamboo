from math import ceil
import simplejson as json
import uuid
from time import gmtime, strftime

from celery.task import task
from celery.contrib.methods import task as class_task
from pandas import concat, Series

from bamboo.config.settings import DB_READ_BATCH_SIZE
from bamboo.core.calculator import Calculator
from bamboo.core.frame import BambooFrame, BAMBOO_RESERVED_KEY_PREFIX,\
    DATASET_ID, DATASET_OBSERVATION_ID, PARENT_DATASET_ID
from bamboo.core.summary import summarize
from bamboo.lib.async import call_async
from bamboo.lib.exceptions import ArgumentError
from bamboo.lib.schema_builder import Schema
from bamboo.lib.utils import split_groups
from bamboo.models.abstract_model import AbstractModel
from bamboo.models.calculation import Calculation
from bamboo.models.observation import Observation


@task
def delete_task(dataset):
    """Background task to delete dataset and its associated observations."""
    Observation.delete_all(dataset)
    super(dataset.__class__, dataset).delete({DATASET_ID: dataset.dataset_id})


class Dataset(AbstractModel):

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
    PENDING_UPDATES = 'pending_updates'
    SCHEMA = 'schema'
    UPDATED_AT = 'updated_at'

    # commonly accessed variables
    @property
    def dataset_observation_id(self):
        return self.record[DATASET_OBSERVATION_ID]

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
    def stats(self):
        return self.record.get(self.STATS, {})

    @property
    def aggregated_datasets_dict(self):
        return self.record.get(self.AGGREGATED_DATASETS, {})

    @property
    def aggregated_datasets(self):
        return {group: self.find_one(_id) for (group, _id) in
                self.aggregated_datasets_dict.items()}

    @property
    def joined_dataset_ids(self):
        return [
            tuple(_list) for _list in self.record.get(self.JOINED_DATASETS, [])
        ]

    @property
    def joined_datasets(self):
        result = []
        # TODO: fetch all datasets in single DB call
        return [
            (direction, self.find_one(other_dataset_id), on,
             self.find_one(joined_dataset_id))
            for direction, other_dataset_id, on, joined_dataset_id in
            self.joined_dataset_ids]

    @property
    def merged_dataset_ids(self):
        return self.record.get(self.MERGED_DATASETS, [])

    @property
    def merged_datasets(self):
        return self._linked_datasets(self.merged_dataset_ids)

    @property
    def pending_updates(self):
        return self.record[self.PENDING_UPDATES]

    def _linked_datasets(self, ids):
        return [self.find_one(_id) for _id in ids]

    def is_factor(self, col):
        return self.schema.is_dimension(col)

    def cardinality(self, col):
        return self.schema.cardinality(col)

    def aggregated_dataset(self, group):
        _id = self.aggregated_datasets_dict.get(group)

        return self.find_one(_id) if _id else None

    def dframe(self, query=None, select=None, distinct=None,
               keep_parent_ids=False, limit=0, order_by=None, padded=False):
        """Fetch the dframe for this dataset.

        :param select: An optional select to limit the fields in the dframe.
        :param keep_parent_ids: Do not remove parent IDs from the dframe,
            default False.
        :param limit: Limit on the number of rows in the returned dframe.
        :param order_by: Sort resulting rows according to a column value and
            sign indicating ascending or descending.

        Example of `order_by`:

          - ``order_by='mycolumn'``
          - ``order_by='-mycolumn'``

        :returns: Return BambooFrame with contents based on query parameters
            passed to MongoDB. BambooFrame will not have parent ids if
            `keep_parent_ids` is False.
        """
        observations = self.observations(
            query=query, select=select, limit=limit, order_by=order_by,
            as_cursor=True)

        dframe = self._batch_read_dframe_from_cursor(
            observations, distinct, limit)

        dframe.decode_mongo_reserved_keys()
        dframe.remove_bamboo_reserved_keys(keep_parent_ids)

        if padded:
            if len(dframe.columns):
                on = dframe.columns[0]
                place_holder = self.place_holder_dframe(dframe).set_index(on)
                dframe = BambooFrame(dframe.join(place_holder, on=on))
            else:
                dframe = self.place_holder_dframe()

        return dframe

    def _batch_read_dframe_from_cursor(self, observations, distinct, limit):
        if distinct:
            observations = observations.distinct(distinct)

        dframes = []
        batch = 0

        while True:
            start = batch * DB_READ_BATCH_SIZE
            end = (batch + 1) * DB_READ_BATCH_SIZE

            if limit > 0 and end > limit:
                end = limit

            # if there is a limit and we are done
            if start >= end:
                break

            current_observations = [ob for ob in observations[start:end]]

            # if the batches exhausted the data
            if not len(current_observations):
                break

            dframes.append(BambooFrame(current_observations))

            if not distinct:
                observations.rewind()

            batch += 1

        return BambooFrame(concat(dframes) if len(dframes) else [])

    def add_joined_dataset(self, new_data):
        """Add the ID of `new_dataset` to the list of joined datasets."""
        self._add_linked_data(self.JOINED_DATASETS, self.joined_dataset_ids,
                              new_data)

    def add_merged_dataset(self, new_dataset):
        """Add the ID of `new_dataset` to the list of merged datasets."""
        self._add_linked_data(self.MERGED_DATASETS, self.merged_dataset_ids,
                              new_dataset.dataset_id)

    def _add_linked_data(self, link_key, existing_data, new_data):
        self.update({link_key: existing_data + [new_data]})

    def clear_summary_stats(self, field=ALL):
        """Remove summary stats for `field`."""
        stats = self.stats

        if stats:
            stats.pop(field, None)
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
            DATASET_OBSERVATION_ID: uuid.uuid4().hex,
            self.AGGREGATED_DATASETS: {},
            self.CREATED_AT: strftime("%Y-%m-%d %H:%M:%S", gmtime()),
            self.STATE: self.STATE_PENDING,
            self.PENDING_UPDATES: [],
        }

        return super(self.__class__, self).save(record)

    def delete(self, countdown=0):
        """Delete this dataset."""
        call_async(delete_task, self, countdown=countdown)

    @class_task
    def summarize(self, query=None, select=None,
                  group_str=None, limit=0, order_by=None):
        """Build and return a summary of the data in this dataset.

        Return a summary of the rows/values filtered by `query` and `select`
        and grouped by `group_str`, or the overall summary if no group is
        specified.

        :param query: An optional MongoDB query to limit the data summarized.
        :param select: An optional select to limit the columns summarized.
        :param group_str: A column in the dataset as a string or a list comma
          separated columns to group on.

        :returns: A JSON summary of the dataset. Numeric columns will be
            summarized by the arithmetic mean, standard deviation, and
            percentiles. Dimensional columns will be summarized by counts.
        """
        # interpret none as all
        if not group_str:
            group_str = self.ALL

        # split group in case of multigroups
        groups = split_groups(group_str)

        # if select append groups to select
        if select:
            select = json.loads(select)
            if not isinstance(select, dict):
                raise ArgumentError('select argument must be a JSON dictionary'
                                    ', found: %s.' % select)
            select.update(dict(zip(groups, [1] * len(groups))))
            select = json.dumps(select)

        dframe = self.dframe(query=query, select=select,
                             limit=limit, order_by=order_by)

        return summarize(self, dframe, groups, group_str, query or select)

    @classmethod
    def find_one(cls, dataset_id):
        """Return dataset for `dataset_id`."""
        return super(cls, cls).find_one({DATASET_ID: dataset_id})

    @classmethod
    def find(cls, dataset_id):
        """Return datasets for `dataset_id`."""
        return super(cls, cls).find({DATASET_ID: dataset_id})

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

    def info(self):
        """Return meta-data for this dataset."""
        return {
            self.ID: self.dataset_id,
            self.LABEL: '',
            self.DESCRIPTION: '',
            self.SCHEMA: self.schema,
            self.LICENSE: '',
            self.ATTRIBUTION: '',
            self.CREATED_AT: self.record.get(self.CREATED_AT),
            self.UPDATED_AT: self.record.get(self.UPDATED_AT),
            self.NUM_COLUMNS: self.num_columns,
            self.NUM_ROWS: self.num_rows,
            self.STATE: self.state,
        }

    def observations(self, query=None, select=None, limit=0, order_by=None,
                     as_cursor=False):
        """Return observations for this dataset.

        :param query: Optional query for MongoDB to limit rows returned.
        :param select: Optional select for MongoDB to limit columns.
        :param limit: If greater than 0, limit number of observations returned
            to this maximum.
        :param order_by: Order the returned observations.
        """
        return Observation.find(self, query, select, limit=limit,
                                order_by=order_by, as_cursor=as_cursor)

    def calculations(self):
        """Return the calculations for this dataset."""
        return Calculation.find(self)

    def remove_parent_observations(self, parent_id):
        """Remove obervations for this dataset with the passed `parent_id`.

        Args:

        :param parent_id: Remove observations with this ID as their parent
            dataset ID.
        """
        Observation.delete_all(self, {PARENT_DATASET_ID: parent_id})

    def add_observations(self, json_data):
        """Update `dataset` with new `data`."""
        record = self.record
        update_id = uuid.uuid4().hex
        self.add_pending_update(update_id)

        new_data = json.loads(json_data)
        calculator = Calculator(self)

        new_dframe_raw = calculator.dframe_from_update(
            new_data, self.schema.labels_to_slugs)
        calculator._check_update_is_valid(new_dframe_raw)

        call_async(calculator.calculate_updates, calculator, new_data,
                   new_dframe_raw=new_dframe_raw, update_id=update_id)

    def add_pending_update(self, update_id):
        self.collection.update(
            {'_id': self.record['_id']},
            {'$push': {self.PENDING_UPDATES: update_id}})

    def save_observations(self, dframe):
        """Save rows in `dframe` for this dataset."""
        Observation().save(dframe, self)
        return self.dframe()

    def replace_observations(self, dframe, overwrite=False,
                             set_num_columns=True):
        """Remove all rows for this dataset and save the rows in `dframe`.

        :param dframe: Replace rows in this dataset with this DataFrame's rows.

        :returns: BambooFrame equivalent to the passed in `dframe`.
        """
        self.build_schema(dframe, overwrite=overwrite,
                          set_num_columns=set_num_columns)
        dframe = self.add_id_column_to_dframe(dframe)
        Observation.delete_all(self)

        return self.save_observations(dframe)

    def drop_columns(self, columns):
        """Remove columns from this dataset's observations.

        :param columns: List of columns to remove from this dataset.
        """
        dframe = self.dframe(keep_parent_ids=True)
        self.replace_observations(dframe.drop(columns, axis=1))

    def place_holder_dframe(self, dframe=None):
        columns = self.schema.keys()
        if dframe is not None:
            columns = [col for col in columns if col not in dframe.columns[1:]]

        return BambooFrame([[''] * len(columns)], columns=columns)

    def join(self, other, on):
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

        return self

    def add_id_column_to_dframe(self, dframe):
        encoded_columns_map = self.schema.rename_map_for_dframe(dframe)

        dframe = dframe.rename(columns=encoded_columns_map)

        id_column = Series([self.dataset_observation_id] * len(dframe))
        id_column.name = DATASET_OBSERVATION_ID

        return dframe.join(id_column)

    def new_agg_dataset(self, dframe, group):
        agg_dataset = self.create()
        agg_dataset.save_observations(dframe)

        # store a link to the new dataset
        agg_datasets_dict = self.aggregated_datasets_dict
        agg_datasets_dict[group] = agg_dataset.dataset_id
        self.update({
            self.AGGREGATED_DATASETS: agg_datasets_dict})

    def has_pending_updates(self, update_id):
        self.reload()
        pending_updates = self.pending_updates

        return pending_updates[0] != update_id and len(
            set(pending_updates) - set([update_id]))

    def update_complete(self, update_id):
        self.collection.update(
            {'_id': self.record['_id']},
            {'$pull': {self.PENDING_UPDATES: update_id}})
