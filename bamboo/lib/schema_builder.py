from datetime import datetime
import numpy as np
import re

from bamboo.core.frame import BAMBOO_RESERVED_KEYS
from bamboo.core.parser import Parser
from bamboo.lib.mongo import MONGO_RESERVED_KEY_STRS, reserve_encoded


CARDINALITY = 'cardinality'
OLAP_TYPE = 'olap_type'
SIMPLETYPE = 'simpletype'
LABEL = 'label'

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
DTYPE_TO_OLAP_TYPE = {
    np.object_: DIMENSION,
    np.bool_: DIMENSION,
    np.float64: MEASURE,
    np.int64: MEASURE,
    datetime: MEASURE,
}

# map from numpy objects to simpletypes
DTYPE_TO_SIMPLETYPE = {
    np.bool_:   BOOLEAN,
    np.float64: FLOAT,
    np.int64:   INTEGER,
    np.object_: STRING,
    datetime: DATETIME,
}
SIMPLETYPE_TO_DTYPE = {
    FLOAT: np.float64,
    INTEGER: np.int64,
}

RE_ENCODED_COLUMN = re.compile(r'\W')


def is_simpletype(col_schema, simpletype):
    return col_schema[SIMPLETYPE] == simpletype


class Schema(dict):
    @classmethod
    def safe_init(cls, arg):
        """Make schema with potential arg of None."""
        return cls() if arg is None else cls(arg)

    @property
    def labels_to_slugs(self):
        """Build dict from column labels to slugs."""
        return {
            column_attrs[LABEL]: reserve_encoded(column_name) for
            (column_name, column_attrs) in self.items()
        }

    @property
    def numeric_slugs(self):
        return [slug for slug, col_schema in self.items()
                if is_simpletype(col_schema, FLOAT)]

    def cardinality(self, column):
        if self.is_dimension(column):
            return self[column].get(CARDINALITY)

    def is_date_simpletype(self, column_schema):
        column_schema = self[column_schema]
        return is_simpletype(column_schema, DATETIME)

    def is_dimension(self, column):
        col_schema = self.get(column)
        return col_schema and col_schema[OLAP_TYPE] == DIMENSION

    def rebuild(self, dframe, overwrite=False):
        """Rebuild a schema for a dframe.

        :param dframe: The DataFrame whose schema to merge with the current
            schema.
        :param overwrite: If true replace schema, otherwise update.
        """
        current_schema = self
        new_schema = schema_from_dframe(dframe, self)

        if current_schema and not overwrite:
            # merge new schema with existing schema
            current_schema.update(new_schema)
            new_schema = current_schema

        return new_schema

    def rename_map_for_dframe(self, dframe):
        """Return a map from dframe columns to slugs.

        :param dframe: The DataFrame to produce the map for.
        """
        labels_to_slugs = self.labels_to_slugs

        # if column name is not in map assume it is already slugified
        # (i.e. NOT a label)
        return {
            column: labels_to_slugs[column] for column in
            dframe.columns.tolist() if self._resluggable_column(
                column, labels_to_slugs, dframe)
        }

    def convert_type(self, slug, value):
        column_schema = self.get(slug)
        if column_schema:
            type_func = SIMPLETYPE_TO_DTYPE.get(column_schema[SIMPLETYPE])
            if type_func:
                value = type_func(value)
        return value

    def _resluggable_column(self, column, labels_to_slugs, dframe):
        """Test if column should be slugged.

        A column should be slugged if:
            1. The `column` is a key in `labels_to_slugs` and
            2. The `column` is not a value in `labels_to_slugs` or
                1. The `column` label is not equal to the `column` slug and
                2. The slug is not in the `dframe`'s columns

        :param column: The column to reslug.
        :param labels_to_slugs: The labels to slugs map (only build once).
        :param dframe: The DataFrame that column is in.
        """
        return (column in labels_to_slugs.keys() and (
                not column in labels_to_slugs.values() or (
                    labels_to_slugs[column] != column and
                    labels_to_slugs[column] not in dframe.columns
                )))


def schema_from_dframe(dframe, schema=None):
    """Build schema from the DataFrame and a schema.

    :param dframe: The DataFrame to build a schema for.
    :param schema: Existing schema, optional.

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
            if schema:
                schema_for_name = schema.get(name)
                if schema_for_name:
                    names_to_labels[name] = schema_for_name[
                        LABEL]

    encoded_names = dict(zip(column_names, _slugify_columns(column_names)))

    schema = Schema()

    for (name, dtype) in dtypes.items():
        if name not in BAMBOO_RESERVED_KEYS:
            column_schema = {
                LABEL: names_to_labels.get(name, name),
                OLAP_TYPE: _olap_type_for_data_and_dtype(
                    dframe[name], dtype),
                SIMPLETYPE: _simpletype_for_data_and_dtype(
                    dframe[name], dtype),
            }
            try:
                column_schema[CARDINALITY] = dframe[
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
        slug = RE_ENCODED_COLUMN.sub('_', column_name).lower()
        slug = make_unique(slug, encoded_names + Parser.reserved_words)
        encoded_names.append(slug)

    return encoded_names


def make_unique(name, reserved_names):
    """Return a slug ensuring name is not in `reserved_names`.

    :param name: The name to make unique.
    :param reserved_names: A list of names the column must not be included in.
    """
    while name in reserved_names:
        name += '_'

    return name


def _olap_type_for_data_and_dtype(column, dtype):
    return _type_for_data_and_dtypes(
        DTYPE_TO_OLAP_TYPE, column, dtype.type)


def _simpletype_for_data_and_dtype(column, dtype):
    return _type_for_data_and_dtypes(
        DTYPE_TO_SIMPLETYPE, column, dtype.type)


def _type_for_data_and_dtypes(type_map, column, dtype_type):
    has_datetime = any([isinstance(field, datetime) for field in column])
    return type_map[datetime if has_datetime else dtype_type]
