import json
import uuid
from time import gmtime, strftime

from celery.task import task
from celery.contrib.methods import task as class_task
import numpy as np

from bamboo.core.calculator import Calculator
from bamboo.core.frame import BambooFrame, BAMBOO_RESERVED_KEY_PREFIX,\
    DATASET_ID, DATASET_OBSERVATION_ID, PARENT_DATASET_ID
from bamboo.core.summary import summarize
from bamboo.lib.mongo import reserve_encoded
from bamboo.lib.schema_builder import DIMENSION, OLAP_TYPE,\
    schema_from_data_and_dtypes, SIMPLETYPE
from bamboo.lib.utils import call_async, split_groups
from bamboo.models.abstract_model import AbstractModel
from bamboo.models.calculation import Calculation
from bamboo.models.observation import Observation


@task
def delete_task(dataset):
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
    CARDINALITY = 'cardinality'
    CREATED_AT = 'created_at'
    DESCRIPTION = 'description'
    ID = 'id'
    LABEL = 'label'
    LICENSE = 'license'
    NUM_COLUMNS = 'num_columns'
    NUM_ROWS = 'num_rows'
    MERGED_DATASETS = 'merged_datasets'
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
    def schema(self):
        return self.record.get(self.SCHEMA)

    @property
    def stats(self):
        return self.record.get(self.STATS, {})

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

    def is_factor(self, col):
        return self.schema[col][OLAP_TYPE] == DIMENSION

    def cardinality(self, col):
        if self.is_factor(col):
            return self.schema[col][self.CARDINALITY]

    def dframe(self, query=None, select=None, keep_parent_ids=False,
               limit=0, order_by=None):
        observations = self.observations(query=query, select=select,
                                         limit=limit, order_by=order_by)
        dframe = BambooFrame(observations)
        dframe.decode_mongo_reserved_keys()
        dframe.remove_bamboo_reserved_keys(keep_parent_ids)
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

        Datasets are initially in a **pending** state.  After the dataset has
        been uploaded and inserted into the database it is in a **ready** state
        .
        """
        if dataset_id is None:
            dataset_id = uuid.uuid4().hex

        record = {
            DATASET_ID: dataset_id,
            DATASET_OBSERVATION_ID: uuid.uuid4().hex,
            self.AGGREGATED_DATASETS: {},
            self.CREATED_AT: strftime("%Y-%m-%d %H:%M:%S", gmtime()),
            self.STATE: self.STATE_PENDING,
        }
        return super(self.__class__, self).save(record)

    def delete(self):
        call_async(delete_task, self)

    @class_task
    def summarize(self, query=None, select=None,
                  group_str=None, limit=0, order_by=None):
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
        dframe = self.dframe(query=query, select=select,
                             limit=limit, order_by=order_by)

        return summarize(self, dframe, groups, group_str, query or select)

    @classmethod
    def find_one(cls, dataset_id):
        return super(cls, cls).find_one({DATASET_ID: dataset_id})

    @classmethod
    def find(cls, dataset_id):
        return super(cls, cls).find({DATASET_ID: dataset_id})

    def update(self, record):
        """
        Update dataset *dataset* with *record*.
        """
        record[self.UPDATED_AT] = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        super(self.__class__, self).update(record)
        self.record = self.__class__.find_one(self.dataset_id).record

    def build_schema(self, dframe):
        """
        Build schema for a dataset.
        """
        schema = schema_from_data_and_dtypes(self, dframe)
        self.update({self.SCHEMA: schema})

    def info(self):
        return {
            self.ID: self.dataset_id,
            self.LABEL: '',
            self.DESCRIPTION: '',
            self.SCHEMA: self.schema,
            self.LICENSE: '',
            self.ATTRIBUTION: '',
            self.CREATED_AT: self.record.get(self.CREATED_AT),
            self.UPDATED_AT: self.record.get(self.UPDATED_AT),
            self.NUM_COLUMNS: self.record.get(self.NUM_COLUMNS),
            self.NUM_ROWS: self.record.get(self.NUM_ROWS),
        }

    def build_labels_to_slugs(self):
        """
        Map the column labels back to their slugified versions
        """
        return dict([
            (column_attrs[self.LABEL], reserve_encoded(column_name)) for
            (column_name, column_attrs) in self.schema.items()])

    def observations(self, query=None, select=None, limit=0, order_by=None):
        return Observation.find(self, query, select,
                                limit=limit, order_by=order_by)

    def calculations(self):
        return Calculation.find(self)

    def remove_parent_observations(self, parent_id):
        Observation.delete_all(self, {PARENT_DATASET_ID: parent_id})

    def add_observations(self, json_data):
        """
        Update *dataset* with new *data*.
        """
        calculator = Calculator(self)
        call_async(calculator.calculate_updates, calculator,
                   json.loads(json_data))

    def save_observations(self, dframe):
        """
        Save rows in *dframe* for this dataset.
        """
        Observation().save(dframe, self)
        return self.dframe()

    def replace_observations(self, dframe):
        """
        Remove all rows for this dataset and save the rows in *dframe*.
        """
        self.build_schema(dframe)
        Observation.delete_all(self)
        return self.save_observations(dframe)
