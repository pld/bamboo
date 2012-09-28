from datetime import datetime
import numpy as np

from lib.constants import BAMBOO_RESERVED_KEYS, DATETIME, DIMENSION, MEASURE,\
    MONGO_RESERVED_KEY_STRS, SIMPLETYPE
from lib.utils import slugify_columns


class SchemaBuilder(object):
    # map from numpy objects to olap_types
    DTYPE_TO_OLAP_TYPE_MAP = {
        np.object_: DIMENSION,
        np.bool_: DIMENSION,
        np.float64: MEASURE,
        np.int64: MEASURE,
        datetime: MEASURE,
    }

    # simpletypes
    STRING = 'string'
    INTEGER = 'integer'
    FLOAT = 'float'
    BOOLEAN = 'boolean'

    # map from numpy objects to simpletypes
    DTYPE_TO_SIMPLETYPE_MAP = {
        np.bool_:   BOOLEAN,
        np.float64: FLOAT,
        np.int64:   INTEGER,
        np.object_: STRING,
        datetime: DATETIME,
    }

    def __init__(self, dataset):
        self.dataset = dataset

    def schema_from_data_and_dtypes(self, dframe):
        """
        Build schema from the dframe, *dframe*, and a dict of dtypes, *dtypes*.
        """
        dtypes = dframe.dtypes.to_dict()

        column_names = list()
        names_to_labels = dict()
        reserved_keys = MONGO_RESERVED_KEY_STRS + BAMBOO_RESERVED_KEYS

        # use existing labels for existing columns
        for name in dtypes.keys():
            if name not in reserved_keys:
                column_names.append(name)
                if self.dataset.data_schema:
                    schema_for_name = self.dataset.data_schema.get(name)
                    if schema_for_name:
                        names_to_labels[name] = schema_for_name[
                            self.dataset.LABEL]

        encoded_names = dict(zip(column_names, slugify_columns(column_names)))

        schema = {}
        for (name, dtype) in dtypes.items():
            if name not in BAMBOO_RESERVED_KEYS:
                column_schema = {
                    self.dataset.LABEL: names_to_labels.get(name, name),
                    self.dataset.OLAP_TYPE: self._olap_type_for_data_and_dtype(
                        dframe[name], dtype),
                    SIMPLETYPE: self._simpletype_for_data_and_dtype(
                        dframe[name], dtype),
                }

                if column_schema[self.dataset.OLAP_TYPE] == DIMENSION:
                    column_schema[self.dataset.CARDINALITY] = dframe[
                        name].nunique()
                schema[encoded_names[name]] = column_schema

        return schema

    def _olap_type_for_data_and_dtype(self, column, dtype):
        return self._type_for_data_and_dtypes(
            self.DTYPE_TO_OLAP_TYPE_MAP, column, dtype.type)

    def _simpletype_for_data_and_dtype(self, column, dtype):
        return self._type_for_data_and_dtypes(
            self.DTYPE_TO_SIMPLETYPE_MAP, column, dtype.type)

    def _type_for_data_and_dtypes(self, type_map, column, dtype_type):
        has_datetime = any([isinstance(field, datetime) for field in column])
        return type_map[datetime if has_datetime else dtype_type]
