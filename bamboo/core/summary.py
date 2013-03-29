import numpy as np

from bamboo.lib.jsontools import series_to_jsondict
from bamboo.lib.mongo import dict_from_mongo, dict_for_mongo


MAX_CARDINALITY_FOR_COUNT = 10000
SUMMARY = 'summary'


class ColumnTypeError(Exception):
    """Exception when grouping on a non-dimensional column."""
    pass


def summarize_series(dtype, data):
    """Call summary function dependent on dtype type.

    :param dtype: The dtype of the column to be summarized.
    :param data: The data to be summarized.

    :returns: The appropriate summarization for the type of `dtype`.
    """
    return {
        np.object_: data.value_counts(),
        np.bool_: data.value_counts(),
        np.float64: data.describe(),
        np.int64: data.describe(),
    }.get(dtype.type, None)


def summarizable(dframe, col, groups, dataset):
    """Check if column should be summarized.

    :param dframe: DataFrame to check unique values in.
    :param col: Column to check for factor and number of uniques.
    :param groups: List of groups if summarizing with group, can be empty.
    :param dataset: Dataset to pull schema from.

    :returns: True if column, with parameters should be summarized, otherwise
        False.
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

    return {
        col: {
            SUMMARY: series_to_jsondict(summarize_series(dtypes[col], data))
        } for col, data in dframe.iteritems() if summarizable(
            dframe, col, groups, dataset)
    }


def summarize_with_groups(dframe, groups, dataset):
    """Calculate summary statistics for group."""
    return series_to_jsondict(
        dframe.groupby(groups).apply(summarize_df, groups, dataset))


def summarize(dataset, dframe, groups, no_cache, update=False):
    """Raises a ColumnTypeError if grouping on a non-dimensional column."""
    # do not allow group by numeric types
    for group in groups:
        if not dataset.schema.is_dimension(group):
            raise ColumnTypeError("group: '%s' is not a dimension." % group)

    group_str = dataset.join_groups(groups) or dataset.ALL

    # check cached stats for group and update as necessary
    stats = dataset.stats
    group_stats = stats.get(group_str)

    if no_cache or not group_stats or update:
        group_stats = summarize_with_groups(dframe, groups, dataset) if\
            groups else summarize_df(dframe, dataset=dataset)

        if not no_cache:
            if update:
                #adding the new group_stats back to the old stat
                original_group_stats = stats.get(group_str, {})
                original_group_stats.update(group_stats)
                group_stats = original_group_stats

            stats.update({group_str: group_stats})
            dataset.update({dataset.STATS: dict_for_mongo(stats)})

    stats_dict = dict_from_mongo(group_stats)

    if groups:
        stats_dict = {group_str: stats_dict}

    return stats_dict
