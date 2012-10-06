import numpy as np

from bamboo.lib.mongo import MONGO_RESERVED_KEYS
from bamboo.lib.utils import series_to_jsondict


SUMMARY = 'summary'


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


def summarize(dframe, groups, is_with_groups):
    return summarize_df(dframe) if is_with_groups else summarize_with_groups(
        dframe, groups)
