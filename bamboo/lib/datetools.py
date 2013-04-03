from calendar import timegm
from datetime import datetime

from dateutil.parser import parse as date_parse
import numpy as np

from bamboo.lib.utils import is_float_nan


def recognize_dates(dframe):
    """Convert data columns to datetimes.

    Check if object columns in a dataframe can be parsed as dates.
    If yes, rewrite column with values parsed as dates.

    :param dframe: The DataFrame to convert columns in.

    :returns: A DataFrame with column values convert to datetime types.
    """
    for i, dtype in enumerate(dframe.dtypes):
        if dtype.type == np.object_:
            column = dframe.columns[i]
            new_column = _convert_column_to_date(dframe, column)

            if not new_column is None:
                dframe[column] = new_column

    return dframe


def recognize_dates_from_schema(dframe, schema):
    """Convert columes to datetime if column in *schema* is of type datetime.

    :param dframe: The DataFrame to convert columns in.
    :param schema: Schema to define columns of type datetime.

    :returns: A DataFrame with column values convert to datetime types.
    """
    dframe_columns = dframe.columns.tolist()

    for column, column_schema in schema.items():
        if column in dframe_columns and\
                schema.is_date_simpletype(column):
            new_column = _convert_column_to_date(dframe, column)

            if not new_column is None:
                dframe[column] = new_column

    return dframe


def _is_potential_date(value):
    return not (is_float_nan(value) or isinstance(value, bool))


def _convert_column_to_date(dframe, column):
    """Inline conversion of column in dframe to date type."""
    try:
        return dframe[column].apply(parse_date)
    except AttributeError:
        # it is already a datetime
        pass
    except ValueError:
        # it is not a correctly formatted date
        pass
    except OverflowError:
        # it is a number that is too large to be a date
        pass


def parse_date(x):
    try:
        return date_parse(x) if _is_potential_date(x) else x
    except ValueError:
        return datetime.strptime(x, '%d%b%Y')


def parse_str_to_unix_time(value):
    return parse_date_to_unix_time(date_parse(value))


def parse_date_to_unix_time(date):
    return timegm(date.utctimetuple())


def safe_parse_date_to_unix_time(date):
    if isinstance(date, datetime):
        date = parse_date_to_unix_time(date)

    return date


def parse_timestamp_query(query, schema):
    """Interpret date column queries as JSON."""
    if query:
        datetime_columns = [
            column for (column, col_schema) in
            schema.items() if
            schema.is_date_simpletype(column) and column in query.keys()]
        for date_column in datetime_columns:
            query[date_column] = {
                key: datetime.fromtimestamp(int(value)) for (key, value) in
                query[date_column].items()
            }

    return query
