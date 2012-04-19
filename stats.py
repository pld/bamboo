import numpy as np
from pandas import pivot_table
from constants import DATA, NAME
from utils import series_to_jsondict

def parse_value_counts(data):
    value_counts = data.value_counts()
    if value_counts.get('null'):
        return value_counts.drop(['null'])
    return value_counts

def summarize_series(dtype, data):
    return {
        np.object_: parse_value_counts(data),
        np.float64: data.describe(),
    }.get(dtype.type, None)
    return None

def summarize_df(df):
    dtypes = df.dtypes
    return filter(lambda d: d[DATA] and len(d[DATA]), [{
                NAME: c,
                DATA: series_to_jsondict(summarize_series(dtypes[c], i))
    } for c, i in df.iteritems()])
