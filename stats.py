import numpy as np

def parse_value_counts(value_counts):
    if value_counts.get('null'):
        return value_counts.drop(['null'])
    return value_counts


def summary_stats(dtype, data):
    value_counts = parse_value_counts(data.value_counts())
    return {
        np.object_: value_counts,
        np.float64: data.describe(),
    }.get(dtype.type, None)
    return None
