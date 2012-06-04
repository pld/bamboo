import numpy as np

from lib.constants import ALL, DATA, NAME, STATS
from lib.utils import series_to_jsondict
from models.dataset import Dataset
from models.observation import Observation


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
    return [{
                NAME: col,
                DATA: series_to_jsondict(summarize_series(dtypes[col], data))
    } for col, data in dframe.iteritems()]


def summarize_with_groups(dframe, stats, group=None):
    """
    Calculate summary statistics including for group if exists
    """
    if not stats:
        stats = {ALL : summarize_df(dframe)}
    if group:
        grouped_stats = series_to_jsondict(
                dframe.groupby(group).apply(summarize_df))
        stats.update(grouped_stats)
    return stats


def summarize(dataset, query, select, group):
    observations = Observation.find(dataset, query, select, as_df=True)
    stats = dataset.get(STATS)
    # if not saved stats, create and save
    if not stats or not stats.get(group):
        stats = summarize_with_groups(observations, stats, group)
        Dataset.update(dataset, {STATS: stats})
    return stats
