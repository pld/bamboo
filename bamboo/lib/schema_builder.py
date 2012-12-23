from datetime import datetime
import numpy as np
import re

from bamboo.core.frame import BAMBOO_RESERVED_KEYS
from bamboo.core.parser import Parser
from bamboo.lib.mongo import MONGO_RESERVED_KEY_STRS


OLAP_TYPE = 'olap_type'
SIMPLETYPE = 'simpletype'

# olap_types
DIMENSION = 'dimension'
MEASURE = 'measure'

# simpletypes
BOOLEAN = 'boolean'
DATETIME = 'datetime'
INTEGER = 'integer'
FLOAT = 'float'
STRING = 'string'

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

RE_ENCODED_COLUMN = re.compile(r'\W')


class Schema(dict):
    def is_date_simpletype(self, column_schema):
        column_schema = self[column_schema]
        return column_schema[SIMPLETYPE] == DATETIME

    def is_dimension(self, col):
        col_schema = self.get(col)
        return col_schema and col_schema[OLAP_TYPE] == DIMENSION


def schema_from_data_and_dtypes(dataset, dframe):
    """Build schema from the DataFrame and the dataset.

    :param dataset: The dataset to store the schema in.
    :param dframe: The DataFrame to build a schema for.

    :returns: A dictionary schema.
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

    encoded_names = dict(zip(column_names, _slugify_columns(column_names)))

    schema = Schema()

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
            except TypeError as e:
                # E.g. dates with and without offset can not be compared and
                # raise a type error.
                pass
            schema[encoded_names[name]] = column_schema

    return schema


def _slugify_columns(column_names):
    """Convert list of strings into unique slugs.

    Convert non-alphanumeric characters in column names into underscores and
    ensure that all column names are unique.

    :param column_names: A list of strings.

    :returns: A list of slugified names with a one-to-one mapping to
        `column_names`.
    """

    encoded_names = []

    for column_name in column_names:
        new_col_name = RE_ENCODED_COLUMN.sub('_', column_name).lower()
        while new_col_name in encoded_names + Parser.reserved_words:
            new_col_name += '_'
        encoded_names.append(new_col_name)

    return encoded_names


def _olap_type_for_data_and_dtype(column, dtype):
    return _type_for_data_and_dtypes(
        DTYPE_TO_OLAP_TYPE_MAP, column, dtype.type)


def _simpletype_for_data_and_dtype(column, dtype):
    return _type_for_data_and_dtypes(
        DTYPE_TO_SIMPLETYPE_MAP, column, dtype.type)


def _type_for_data_and_dtypes(type_map, column, dtype_type):
    has_datetime = any([isinstance(field, datetime) for field in column])
    return type_map[datetime if has_datetime else dtype_type]
