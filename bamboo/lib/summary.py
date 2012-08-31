import numpy as np

from lib.constants import ALL, MONGO_RESERVED_KEYS, SUMMARY
from lib.utils import series_to_jsondict


def summarize_series(dtype, data):
    """
    Call summary function dependent on dtype type
    """
    return {
        np.object_: data.value_counts(),
        np.float64: data.describe(),
        np.int64: data.describe(),
    }.get(dtype.type, None)


def summarize_df(dframe):
    """
    Calculate summary statistics
    """
    dtypes = dframe.dtypes
    # TODO handle empty data
    # before we wrapped with filter(lambda d: d[DATA] and len(d[DATA]), array)
    return dict([
        (col, {
            SUMMARY: series_to_jsondict(summarize_series(dtypes[col], data))
        }) for col, data in dframe.iteritems()
        if not col in MONGO_RESERVED_KEYS
    ])


def summarize_with_groups(dframe, stats, group):
    """
    Calculate summary statistics for group.
    """
    grouped_stats = series_to_jsondict(
        dframe.groupby(group).apply(summarize_df))
    stats.update({group: grouped_stats})
    return stats
