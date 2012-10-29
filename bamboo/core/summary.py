import numpy as np

from bamboo.lib.jsontools import series_to_jsondict
from bamboo.lib.schema_builder import DIMENSION, OLAP_TYPE
from bamboo.lib.mongo import dict_for_mongo


MAX_CARDINALITY_FOR_COUNT = 10000
SUMMARY = 'summary'


class ColumnTypeError(Exception):
    """Exception when grouping on a non-dimensional column."""
    pass


def summarize_series(dtype, data):
    """Call summary function dependent on dtype type.

    Args:

    - dtype: The dtype of the column to be summarized.
    - data: The data to be summarized.

    Returns:
        The appropriate summarization for the type of *dtype*.
    """
    return {
        np.object_: data.value_counts(),
        np.float64: data.describe(),
        np.int64: data.describe(),
    }.get(dtype.type, None)


def summarizable(dframe, col, groups, dataset):
    """Check if column should be summarized.

    Args:

    - dframe: DataFrame to check unique values in.
    - col: Column to check for factor and number of uniques.
    - groups: List of groups if summarizing with group, can be empty.
    - dataset: Dataset to pull schema from.

    Returns:
        True if column, with parameters should be summarized, otherwise False.
    """
    if dataset.is_factor(col):
        cardinality = dframe[col].nunique() if len(groups) else\
            dataset.cardinality(col)
        if cardinality > MAX_CARDINALITY_FOR_COUNT:
            return False
    return not col in groups


def summarize_df(dframe, groups=[], dataset=None):
    """Calculate summary statistics."""
    dtypes = dframe.dtypes
    return dict([
        (col, {
            SUMMARY: series_to_jsondict(summarize_series(dtypes[col], data))
        }) for col, data in dframe.iteritems() if summarizable(
            dframe, col, groups, dataset)
    ])


def summarize_with_groups(dframe, groups, dataset):
    """Calculate summary statistics for group."""
    return series_to_jsondict(
        dframe.groupby(groups).apply(summarize_df, groups, dataset))


def summarize(dataset, dframe, groups, group_str, no_cache):
    """Raises a ColumnTypeError if grouping on a non-dimensional column."""
    # do not allow group by numeric types
    for group in groups:
        group_type = dataset.schema.get(group)
        _type = dframe.dtypes.get(group)
        if group != dataset.ALL and (
                group_type is None or group_type[OLAP_TYPE] != DIMENSION):
            raise ColumnTypeError("group: '%s' is not a dimension." % group)

    # check cached stats for group and update as necessary
    stats = dataset.stats
    if no_cache or not stats.get(group_str):
        group_stats = summarize_df(dframe, dataset=dataset) if\
            group_str == dataset.ALL else\
            summarize_with_groups(dframe, groups, dataset)
        stats.update({group_str: group_stats})
        if not no_cache:
            dataset.update({dataset.STATS: dict_for_mongo(stats)})
    stats_to_return = stats.get(group_str)

    return stats_to_return if group_str == dataset.ALL else {
        group_str: stats_to_return}
