import uuid
from time import gmtime, strftime

from lib.constants import ATTRIBUTION, CREATED_AT, DATASET_ID,\
    DATASET_OBSERVATION_ID, DESCRIPTION, DTYPE_TO_OLAP_TYPE_MAP,\
    DTYPE_TO_SIMPLETYPE_MAP, ID, LABEL, LICENSE, LINKED_DATASETS, OLAP_TYPE,\
    SCHEMA, SIMPLETYPE, UPDATED_AT
from lib.utils import slugify_columns
from models.abstract_model import AbstractModel


class Dataset(AbstractModel):

    __collectionname__ = 'datasets'

    @classmethod
    def find(cls, dataset_id):
        return cls.collection.find({DATASET_ID: dataset_id})

    @classmethod
    def find_one(cls, dataset_id):
        return cls.collection.find_one({DATASET_ID: dataset_id})

    @classmethod
    def create(cls, dataset_id=None):
        if dataset_id is None:
            dataset_id = uuid.uuid4().hex
        return cls.save(dataset_id)

    @classmethod
    def save(cls, dataset_id):
        """
        Store dataset with *dataset_id* as the unique internal ID.
        """
        record = {
            CREATED_AT: strftime("%Y-%m-%d %H:%M:%S", gmtime()),
            DATASET_ID: dataset_id,
            DATASET_OBSERVATION_ID: uuid.uuid4().hex,
            LINKED_DATASETS: {},
        }
        cls.collection.insert(record)
        return record

    @classmethod
    def delete(cls, dataset_id):
        """
        Delete dataset with *dataset_id*.
        """
        cls.collection.remove({DATASET_ID: dataset_id})

    @classmethod
    def update(cls, dataset, _dict):
        """
        Update dataset *dataset* with *_dict*.
        """
        _dict[UPDATED_AT] = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        dataset.update(_dict)
        cls.collection.update({DATASET_ID: dataset[DATASET_ID]}, dataset)

    @classmethod
    def _schema_from_dtypes(cls, dtypes):
        """
        Build schema from a dict of dtypes, *dtypes*.
        """
        column_names = dtypes.keys()
        encoded_names = dict(zip(column_names, slugify_columns(column_names)))
        return dict([(encoded_names[name], {
            LABEL: name,
            OLAP_TYPE: DTYPE_TO_OLAP_TYPE_MAP[dtype.type],
            SIMPLETYPE: DTYPE_TO_SIMPLETYPE_MAP[dtype.type],
        }) for (name, dtype) in dtypes.items()])

    @classmethod
    def build_schema(cls, dataset, dtypes):
        schema = cls._schema_from_dtypes(dtypes.to_dict())
        cls.update(dataset, {SCHEMA: schema})

    @classmethod
    def update_schema(cls, dataset, dtypes):
        # merge in new schema dicts
        schema = cls._schema_from_dtypes(dtypes)
        schema.update(dataset[SCHEMA])

        # store new complete schema with dataset
        cls.update(dataset, {SCHEMA: schema})

    @classmethod
    def schema(cls, dataset):
        return {
            ID: dataset[DATASET_ID],
            LABEL: '',
            DESCRIPTION: '',
            SCHEMA: dataset[SCHEMA],
            LICENSE: '',
            ATTRIBUTION: '',
            CREATED_AT: dataset[CREATED_AT],
            UPDATED_AT: dataset[UPDATED_AT],
        }
