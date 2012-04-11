import numpy as np


def summary_stats(dtype, data):
    value_counts = data.value_counts()
    if len(value_counts) > 1 and value_counts.max() > 1:
        return {
            np.object_: value_counts,
            np.float64: data.describe(),
        }.get(dtype.type, None)
    return None
