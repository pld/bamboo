import numpy as np

from lib.constants import ALL, DATA, NAME
from lib.utils import series_to_jsondict
from models.observation import Observation

def parse_value_counts(data):
    'Calculate and remove null from value counts'
    value_counts = data.value_counts()
    if value_counts.get('null'):
        return value_counts.drop(['null'])
    return value_counts

def summarize_series(dtype, data):
    'Call summary function dependent on dtype type'
    return {
        np.object_: parse_value_counts(data),
        np.float64: data.describe(),
    }.get(dtype.type, None)

def summarize_df(dframe):
    'Calculate summary statistics'
    dtypes = dframe.dtypes
    return filter(lambda d: d[DATA] and len(d[DATA]), [{
                NAME: c,
                DATA: series_to_jsondict(summarize_series(dtypes[c], i))
    } for c, i in dframe.iteritems()])

def summarize_with_groups(dframe, group=None):
    'Calculate summary statistics including for group if exists'
    stats = {ALL : summarize_df(dframe)}
    if group:
        grouped_stats = series_to_jsondict(
                dframe.groupby(group).apply(summarize_df))
        stats.update(grouped_stats)
    return stats

def summarize(dataset, query, group):
    observations = Observation.find(dataset, query, as_df=True)
    return summarize_with_groups(observations, group)
