import json
import uuid
from time import gmtime, strftime

from celery.contrib.methods import task
import numpy as np
from pandas import DataFrame

from bamboo.core.calculator import Calculator
from bamboo.core.summary import summarize
from bamboo.lib.constants import BAMBOO_RESERVED_KEY_PREFIX,\
    BAMBOO_RESERVED_KEYS, DATASET_ID,\
    DATASET_OBSERVATION_ID, ID, NUM_COLUMNS, NUM_ROWS,\
    PARENT_DATASET_ID, SCHEMA
from bamboo.lib.mongo import mongo_to_df, reserve_encoded
from bamboo.lib.schema_builder import schema_from_data_and_dtypes, SIMPLETYPE
from bamboo.lib.utils import call_async, split_groups
from bamboo.models.abstract_model import AbstractModel
from bamboo.models.calculation import Calculation
from bamboo.models.observation import Observation


class Dataset(AbstractModel):

    __collectionname__ = 'datasets'

    # caching keys
    STATS = '_stats'
    ALL = '_all'

    # metadata
    ATTRIBUTION = 'attribution'
    CARDINALITY = 'cardinality'
    CREATED_AT = 'created_at'
    DESCRIPTION = 'description'
    LABEL = 'label'
    LICENSE = 'license'
    AGGREGATED_DATASETS = BAMBOO_RESERVED_KEY_PREFIX + 'linked_datasets'
    MERGED_DATASETS = 'merged_datasets'
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
    def schema(self):
        return self.record.get(SCHEMA)

    @property
    def aggregated_datasets_dict(self):
        return self.record.get(self.AGGREGATED_DATASETS, {})

    @property
    def aggregated_datasets(self):
        return dict([(group, self.find_one(_id)) for (group, _id) in
                     self.aggregated_datasets_dict.items()])

    @property
    def merged_dataset_ids(self):
        return self.record.get(self.MERGED_DATASETS, [])

    @property
    def merged_datasets(self):
        return [self.find_one(_id) for _id in self.merged_dataset_ids]

    def dframe(self, query=None, select=None, with_reserved_keys=False,
               with_parent_ids=False):
        observations = self.observations(query=query, select=select)
        if with_reserved_keys:
            return DataFrame(observations)
        else:
            dframe = mongo_to_df(observations)
            reserved_keys = list(set(BAMBOO_RESERVED_KEYS).intersection(
                set(dframe.columns.tolist())))
            if with_parent_ids and PARENT_DATASET_ID in reserved_keys:
                reserved_keys.remove(PARENT_DATASET_ID)
            for column in reserved_keys:
                del dframe[column]
            return dframe

    def add_merged_dataset(self, new_dataset):
        self.update({
            self.MERGED_DATASETS: self.merged_datasets +
            [new_dataset.dataset_id]})

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
            self.AGGREGATED_DATASETS: {},
        }
        return super(self.__class__, self).save(record)

    @task
    def delete(self):
        super(self.__class__, self).delete({DATASET_ID: self.dataset_id})
        Observation.delete_all(self)

    @task
    def summarize(self, query=None, select=None, group_str=None):
        """
        Return a summary for the rows/values filtered by *query* and *select*
        and grouped by *group_str* or the overall summary if no group is
        specified.

        *group_str* may be a string of many comma separated groups.
        """
        # interpret none as all
        if not group_str:
            group_str = self.ALL

        # split group in case of multigroups
        groups = split_groups(group_str)

        # if select append groups to select
        if select:
            select = json.loads(select)
            select.update(dict(zip(groups, [1] * len(groups))))
            select = json.dumps(select)

        # narrow list of observations via query/select
        dframe = self.dframe(query=query, select=select)

        return summarize(self, dframe, groups, group_str, query or select)

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
        schema = schema_from_data_and_dtypes(self, dframe)
        self.update({SCHEMA: schema})

    def info(self):
        return {
            ID: self.dataset_id,
            self.LABEL: '',
            self.DESCRIPTION: '',
            SCHEMA: self.schema,
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
            (column_name, column_attrs) in self.schema.items()])

    def observations(self, query=None, select=None):
        return Observation.find(self, query, select)

    def calculations(self):
        return Calculation.find(self)

    def remove_parent_observations(self, parent_id):
        Observation.delete_all(self, {PARENT_DATASET_ID: parent_id})

    def add_observations(self, json_data):
        """
        Update *dataset* with new *data*.
        """
        calculator = Calculator(self)
        call_async(calculator.calculate_updates, self, calculator,
                   json.loads(json_data))

    def replace_observations(self, dframe):
        """
        Update this dataset by overwriting all observations with the given
        *dframe*.
        """
        self.build_schema(dframe)
        Observation.delete_all(self)
        Observation().save(dframe, self)
        return self.dframe()

