from datetime import datetime
from memory_profiler import memory_usage as mu


global_count_dict = {}


def print_time(string, count_list=None, memory=False):
    # keep a dict in here of counts:
    # e.g. {'cache_hits': 4, 'db_fetches': 5}
    # memory_prfiler as an option to print out memory_usage
    count_msg = ''
    if count_list:
        for item in count_list:
            if not item in global_count_dict:
                global_count_dict[item] = 1
            else:
                global_count_dict[item] += 1
            count_msg += "\n%s--> %s: %s" % \
                (' ' * 18, item, global_count_dict[item])
    memory_msg = ''
    if memory:
        memory_msg += "%s (MB)" % mu()
    print "[%s] %s %s%s" % (datetime.now().strftime('%H:%M:%S,%f'),
                            string, memory_msg, count_msg)
