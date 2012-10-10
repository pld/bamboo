import numpy as np

from bamboo.lib.jsontools import series_to_jsondict
from bamboo.lib.mongo import MONGO_RESERVED_KEYS
from bamboo.lib.schema_builder import DIMENSION, OLAP_TYPE


SUMMARY = 'summary'


class ColumnTypeError(Exception):
    pass


def summarize_series(dtype, data):
    """
    Call summary function dependent on dtype type
    """
    return {
        np.object_: data.value_counts(),
        np.float64: data.describe(),
        np.int64: data.describe(),
    }.get(dtype.type, None)


def summarize_df(dframe, groups=[]):
    """
    Calculate summary statistics
    """
    dtypes = dframe.dtypes
    # TODO handle empty data
    # before we wrapped with filter(lambda d: d[DATA] and len(d[DATA]), array)
    return dict([
        (col, {
            SUMMARY: series_to_jsondict(summarize_series(dtypes[col], data))
        }) for col, data in dframe.iteritems() if not col in groups
    ])


def summarize_with_groups(dframe, groups):
    """
    Calculate summary statistics for group.
    """
    return series_to_jsondict(
        dframe.groupby(groups).apply(summarize_df, groups))


def summarize(dataset, dframe, groups, group_str, no_cache):
    """
    Raises a ColumnTypeError if grouping on a non-dimensional column.
    """
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
        group_stats = summarize_df(dframe) if group_str == dataset.ALL else\
            summarize_with_groups(dframe, groups)
        stats.update({group_str: group_stats})
        if not no_cache:
            dataset.update({dataset.STATS: stats})
    stats_to_return = stats.get(group_str)

    return stats_to_return if group_str == dataset.ALL else {
        group_str: stats_to_return}
