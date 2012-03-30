import numpy


def summary_stats(dtype, data):
    return {
        numpy.object_: data.value_counts(),
        numpy.float64: data.describe(),
    }.get(dtype.type, None)
