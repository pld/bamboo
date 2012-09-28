import json
import uuid
from time import gmtime, strftime

from celery.contrib.methods import task
import numpy as np
from pandas import concat, Series

from lib.constants import ALL, BAMBOO_RESERVED_KEY_PREFIX, DATASET_ID,\
    DATASET_OBSERVATION_ID, DIMENSION, ERROR, ID, NUM_COLUMNS, NUM_ROWS,\
    PARENT_DATASET_ID, SCHEMA, SIMPLETYPE
from lib.exceptions import MergeError
from lib.schema_builder import SchemaBuilder
from lib.summary import summarize_df, summarize_with_groups
from lib.utils import reserve_encoded, split_groups
from models.abstract_model import AbstractModel
from models.calculation import Calculation
from models.observation import Observation


class Dataset(AbstractModel):

    __collectionname__ = 'datasets'

    STATS = '_stats'

    # metadata
    ATTRIBUTION = 'attribution'
    CARDINALITY = 'cardinality'
    CREATED_AT = 'created_at'
    DESCRIPTION = 'description'
    LABEL = 'label'
    LICENSE = 'license'
    LINKED_DATASETS = BAMBOO_RESERVED_KEY_PREFIX + 'linked_datasets'
    MERGED_DATASETS = 'merged_datasets'
    OLAP_TYPE = 'olap_type'
    UPDATED_AT = 'updated_at'

    # commonly accessed variables
    @property
    def dataset_observation_id(self):
        return self.record[DATASET_OBSERVATION_ID]

    @property
    def dataset_id(self):
        return self.record[DATASET_ID]

    @property
    def stats(self):
        return self.record.get(self.STATS, {})

    @property
    def data_schema(self):
        return self.record.get(SCHEMA)

    @property
    def linked_datasets_dict(self):
        return self.record.get(self.LINKED_DATASETS, {})

    @property
    def linked_datasets(self):
        return dict([(group, self.find_one(_id)) for (group, _id) in
                     self.linked_datasets_dict.items()])

    @property
    def merged_dataset_ids(self):
        return self.record.get(self.MERGED_DATASETS, [])

    @property
    def merged_datasets(self):
        return [self.find_one(_id) for _id in self.merged_dataset_ids]

    def add_merged_dataset(self, new_dataset):
        self.update({
            self.MERGED_DATASETS: self.merged_datasets +
            [new_dataset.dataset_id]})

    def clear_linked_datasets(self):
        self.update({self.LINKED_DATASETS: {}})

    def clear_summary_stats(self, field=ALL):
        """
        Invalidate summary stats for *field*.
        """
        stats = self.stats
        if stats:
            stats.pop(field, None)
            self.update({self.STATS: stats})

    def save(self, dataset_id=None):
        """
        Store dataset with *dataset_id* as the unique internal ID.
        Create a new dataset_id if one is not passed.
        """
        if dataset_id is None:
            dataset_id = uuid.uuid4().hex

        record = {
            self.CREATED_AT: strftime("%Y-%m-%d %H:%M:%S", gmtime()),
            DATASET_ID: dataset_id,
            DATASET_OBSERVATION_ID: uuid.uuid4().hex,
            self.LINKED_DATASETS: {},
        }
        self.collection.insert(record, safe=True)
        self.record = record
        return record

    @task
    def delete(self):
        super(self.__class__, self).delete({DATASET_ID: self.dataset_id})
        Observation.delete_all(self)

    @task
    def summarize(self, query=None, select=None, group_str=ALL):
        """
        Return a summary for the rows/values filtered by *query* and *select*
        and grouped by *group_str* or the overall summary if no group is
        specified.

        *group_str* may be a string of many comma separated groups.
        """
        # split group in case of multigroups
        groups = split_groups(group_str)
        group_key = ALL

        # if select append groups to select
        if select:
            select = json.loads(select)
            select.update(dict(zip(groups, [1] * len(groups))))
            select = json.dumps(select)
        if group_str != ALL:
            group_key = '%s,%s' % (group_str, select)

        # narrow list of observations via query/select
        dframe = self.observations(query, select, as_df=True)

        # do not allow group by numeric types
        for group in groups:
            group_type = self.data_schema.get(group)
            _type = dframe.dtypes.get(group)
            if group != ALL and (group_type is None or
                                 group_type[self.OLAP_TYPE] != DIMENSION):
                return {ERROR: "group: '%s' is not a dimension." % group}

        # check cached stats for group and update as necessary
        stats = self.stats
        if query or not stats.get(group_key):
            stats = {ALL: summarize_df(dframe)} if group_str == ALL \
                else summarize_with_groups(
                    dframe, stats, group_key, groups, select)
            if not query:
                self.update({self.STATS: stats})
        stats_to_return = stats.get(group_key)

        return stats_to_return if group_str == ALL else {
            group_str: stats_to_return}

    @classmethod
    def find_one(cls, dataset_id):
        return super(cls, cls).find_one({DATASET_ID: dataset_id})

    @classmethod
    def find(cls, dataset_id):
        return super(cls, cls).find({DATASET_ID: dataset_id})

    def update(self, _dict):
        """
        Update dataset *dataset* with *_dict*.
        """
        _dict[self.UPDATED_AT] = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        self.record.update(_dict)
        self.collection.update({DATASET_ID: self.dataset_id}, self.record,
                               safe=True)

    def build_schema(self, dframe):
        """
        Build schema for a dataset.
        """
        schema_builder = SchemaBuilder(self)
        schema = schema_builder.schema_from_data_and_dtypes(dframe)
        self.update({SCHEMA: schema})

    def schema(self):
        return {
            ID: self.dataset_id,
            self.LABEL: '',
            self.DESCRIPTION: '',
            SCHEMA: self.data_schema,
            self.LICENSE: '',
            self.ATTRIBUTION: '',
            self.CREATED_AT: self.record.get(self.CREATED_AT),
            self.UPDATED_AT: self.record.get(self.UPDATED_AT),
            NUM_COLUMNS: self.record.get(NUM_COLUMNS),
            NUM_ROWS: self.record.get(NUM_ROWS),
        }

    def build_labels_to_slugs(self):
        """
        Map the column labels back to their slugified versions
        """
        return dict([
            (column_attrs[self.LABEL], reserve_encoded(column_name)) for
            (column_name, column_attrs) in self.data_schema.items()])

    def observations(self, query=None, select=None, as_df=False):
        return Observation.find(self, query, select, as_df)

    def calculations(self):
        return Calculation.find(self)

    def remove_parent_observations(self, parent_id):
        Observation.delete_all(self, {PARENT_DATASET_ID: parent_id})

    @classmethod
    def merge(cls, datasets):
        if len(datasets) < 2:
            raise MergeError(
                'merge requires 2 datasets (found %s)' % len(datasets))

        dframes = []
        for dataset in datasets:
            dframe = dataset.observations(as_df=True)
            column = Series([dataset.dataset_id] * len(dframe))
            column.name = PARENT_DATASET_ID
            dframes.append(dframe.join(column))

        return concat(dframes, ignore_index=True)
