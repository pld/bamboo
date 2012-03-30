import numpy as np


def summary_stats(dtype, data):
    return {
        np.object_: data.value_counts(),
        np.float64: data.describe(),
    }.get(dtype.type, None)
