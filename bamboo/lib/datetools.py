from calendar import timegm
from datetime import datetime

from dateutil.parser import parse as date_parse
import numpy as np
from pandas import Series

from bamboo.lib.schema_builder import DATETIME, SIMPLETYPE
from bamboo.lib.utils import is_float_nan


def recognize_dates(dframe):
    """
    Check if object columns in a dataframe can be parsed as dates.
    If yes, rewrite column with values parsed as dates.
    """
    for idx, dtype in enumerate(dframe.dtypes):
        if dtype.type == np.object_:
            dframe = _convert_column_to_date(dframe, dframe.columns[idx])
    return dframe


def recognize_dates_from_schema(dataset, dframe):
    # if it is a date column, recognize dates
    dframe_columns = dframe.columns.tolist()
    for column, column_schema in dataset.schema.items():
        if column in dframe_columns and\
                column_schema[SIMPLETYPE] == DATETIME:
            dframe = _convert_column_to_date(dframe, column)
    return dframe


def _is_potential_date(value):
    return not (is_float_nan(value) or isinstance(value, bool))


def _convert_column_to_date(dframe, column):
    try:
        new_column = Series([
            date_parse(field) if _is_potential_date(field) else field for
            field in dframe[column].tolist()])
        dframe[column] = new_column
    except ValueError:
        # it is not a correctly formatted date
        pass
    except OverflowError:
        # it is a number that is too large to be a date
        pass
    return dframe


def parse_str_to_unix_time(value):
    return parse_date_to_unix_time(date_parse(value))


def parse_date_to_unix_time(date):
    return timegm(date.utctimetuple())


def col_is_date_simpletype(column_schema):
    return column_schema[SIMPLETYPE] == DATETIME


def parse_timestamp_query(query, schema):
    """
    Interpret date column queries as JSON.
    """
    if query != {}:
        datetime_columns = [
            column for (column, schema) in
            schema.items() if
            schema[SIMPLETYPE] == DATETIME and column in query.keys()]
        for date_column in datetime_columns:
            query[date_column] = dict([(
                key,
                datetime.fromtimestamp(int(value))) for (key, value) in
                query[date_column].items()])
    return query
