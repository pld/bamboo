from datetime import datetime
import numpy as np

from bamboo.core.frame import BAMBOO_RESERVED_KEYS
from bamboo.lib.mongo import MONGO_RESERVED_KEY_STRS
from bamboo.lib.utils import slugify_columns


OLAP_TYPE = 'olap_type'
SIMPLETYPE = 'simpletype'

# simpletypes
BOOLEAN = 'boolean'
DATETIME = 'datetime'
INTEGER = 'integer'
FLOAT = 'float'
STRING = 'string'

# olap_types
DIMENSION = 'dimension'
MEASURE = 'measure'

# map from numpy objects to olap_types
DTYPE_TO_OLAP_TYPE_MAP = {
    np.object_: DIMENSION,
    np.bool_: DIMENSION,
    np.float64: MEASURE,
    np.int64: MEASURE,
    datetime: MEASURE,
}

# map from numpy objects to simpletypes
DTYPE_TO_SIMPLETYPE_MAP = {
    np.bool_:   BOOLEAN,
    np.float64: FLOAT,
    np.int64:   INTEGER,
    np.object_: STRING,
    datetime: DATETIME,
}


def schema_from_data_and_dtypes(dataset, dframe):
    """Build schema from the DataFrame and the dataset.

    Args:

    - dataset: The dataset to store the schema in.
    - dframe: The DataFrame to build a schema for.

    Returns:
        A dictionary schema.
    """
    dtypes = dframe.dtypes.to_dict()

    column_names = list()
    names_to_labels = dict()
    reserved_keys = MONGO_RESERVED_KEY_STRS + BAMBOO_RESERVED_KEYS

    # use existing labels for existing columns
    for name in dtypes.keys():
        if name not in reserved_keys:
            column_names.append(name)
            if dataset.schema:
                schema_for_name = dataset.schema.get(name)
                if schema_for_name:
                    names_to_labels[name] = schema_for_name[
                        dataset.LABEL]

    encoded_names = dict(zip(column_names, slugify_columns(column_names)))

    schema = {}
    for (name, dtype) in dtypes.items():
        if name not in BAMBOO_RESERVED_KEYS:
            column_schema = {
                dataset.LABEL: names_to_labels.get(name, name),
                OLAP_TYPE: _olap_type_for_data_and_dtype(
                    dframe[name], dtype),
                SIMPLETYPE: _simpletype_for_data_and_dtype(
                    dframe[name], dtype),
            }
            try:
                column_schema[dataset.CARDINALITY] = dframe[
                    name].nunique()
            except AttributeError as e:
                pass
            schema[encoded_names[name]] = column_schema

    return schema


def _olap_type_for_data_and_dtype(column, dtype):
    return _type_for_data_and_dtypes(
        DTYPE_TO_OLAP_TYPE_MAP, column, dtype.type)


def _simpletype_for_data_and_dtype(column, dtype):
    return _type_for_data_and_dtypes(
        DTYPE_TO_SIMPLETYPE_MAP, column, dtype.type)


def _type_for_data_and_dtypes(type_map, column, dtype_type):
    has_datetime = any([isinstance(field, datetime) for field in column])
    return type_map[datetime if has_datetime else dtype_type]
