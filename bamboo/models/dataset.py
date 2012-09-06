import uuid
from time import gmtime, strftime

from lib.constants import ATTRIBUTION, CREATED_AT, DATASET_ID,\
    DATASET_OBSERVATION_ID, DESCRIPTION, DTYPE_TO_OLAP_TYPE_MAP,\
    DTYPE_TO_SIMPLETYPE_MAP, ID, LABEL, LICENSE, LINKED_DATASETS, OLAP_TYPE,\
    SCHEMA, SIMPLETYPE, STATS, UPDATED_AT
from lib.utils import slugify_columns, type_for_data_and_dtypes
from models.abstract_model import AbstractModel


class Dataset(AbstractModel):

    __collectionname__ = 'datasets'

    # commonly accessed variables
    @property
    def dataset_observation_id(self):
        return self.record[DATASET_OBSERVATION_ID]

    @property
    def dataset_id(self):
        return self.record[DATASET_ID]

    @property
    def stats(self):
        return self.record.get(STATS, {})

    @property
    def data_schema(self):
        return self.record[SCHEMA]

    @property
    def linked_datasets(self):
        return self.record.get(LINKED_DATASETS, {})

    def save(self, dataset_id=None):
        """
        Store dataset with *dataset_id* as the unique internal ID.
        Create a new dataset_id if one is not passed.
        """
        if dataset_id is None:
            dataset_id = uuid.uuid4().hex

        record = {
            CREATED_AT: strftime("%Y-%m-%d %H:%M:%S", gmtime()),
            DATASET_ID: dataset_id,
            DATASET_OBSERVATION_ID: uuid.uuid4().hex,
            LINKED_DATASETS: {},
        }
        self.collection.insert(record, safe=True)
        self.record = record
        return record

    def delete(self):
        super(self.__class__, self).delete({DATASET_ID: self.dataset_id})

    @classmethod
    def find(cls, dataset_id):
        return super(cls, cls).find({DATASET_ID: dataset_id})

    def update(self, _dict):
        """
        Update dataset *dataset* with *_dict*.
        """
        _dict[UPDATED_AT] = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        self.record.update(_dict)
        self.collection.update({DATASET_ID: self.dataset_id}, self.record,
                               safe=True)

    def _schema_from_data_and_dtypes(self, dframe, dtypes):
        """
        Build schema from the dframe, *dframe*, and a dict of dtypes, *dtypes*.
        """
        column_names = dtypes.keys()
        encoded_names = dict(zip(column_names, slugify_columns(column_names)))
        return dict([(encoded_names[name], {
            LABEL: name,
            OLAP_TYPE: type_for_data_and_dtypes(DTYPE_TO_OLAP_TYPE_MAP,
                                                dframe[name], dtype.type),
            SIMPLETYPE: type_for_data_and_dtypes(DTYPE_TO_SIMPLETYPE_MAP,
                                                 dframe[name], dtype.type),
        }) for (name, dtype) in dtypes.items()])

    def build_schema(self, dframe, dtypes):
        """
        Build schema for a dataset.
        """
        schema = self._schema_from_data_and_dtypes(dframe, dtypes.to_dict())
        self.update({SCHEMA: schema})

    def update_schema(self, dframe, dtypes):
        """
        Update schema to include new columns.
        """
        # merge in new schema dicts
        new_schema = self._schema_from_data_and_dtypes(dframe, dtypes)
        new_schema.update(self.data_schema)

        # store new complete schema with dataset
        self.update({SCHEMA: new_schema})

    def schema(self):
        return {
            ID: self.dataset_id,
            LABEL: '',
            DESCRIPTION: '',
            SCHEMA: self.data_schema,
            LICENSE: '',
            ATTRIBUTION: '',
            CREATED_AT: self.record[CREATED_AT],
            UPDATED_AT: self.record[UPDATED_AT],
        }

    def build_labels_to_slugs(self):
        """
        Map the column labels back to their slugified versions
        """
        return dict([(column_attrs[LABEL], column_name) for
                     (column_name, column_attrs) in self.data_schema.items()])