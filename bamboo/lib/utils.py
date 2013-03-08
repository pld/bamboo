from math import isnan
from sys import maxint
from datetime import datetime
from memory_profiler import memory_usage as mu


def minint():
    return -maxint - 1


def parse_int(value, default):
    return _parse_type(int, value, default)


def parse_float(value, default=None):
    return _parse_type(float, value, default)


def _parse_type(_type, value, default):
    try:
        return _type(value)
    except ValueError:
        return value if default is None else default


def is_float_nan(num):
    """Return True is `num` is a float and NaN."""
    return isinstance(num, float) and isnan(num)

global_count_dict = {}
def print_time(string, count_list=None, memory=True):
    # keep a dict in here of counts:
    # e.g. {'cache_hits': 4, 'db_fetches': 5}
    # memory_prfiler as an option to print out memory_usage
    count_msg = ''
    if count_list:
        for item in count_list:
            if not global_count_dict.has_key(item):
                global_count_dict[item] = 1
            else:
                global_count_dict[item] += 1
            count_msg += "\n%s--> %s: %s" % (' '*18, item, global_count_dict[item])
    memory_msg = ''
    if memory:
        memory_msg += "%s (MB)" % mu()
    print "[%s] %s %s%s"\
        % (datetime.now().strftime('%H:%M:%S,%f'), string, memory_msg, count_msg)
