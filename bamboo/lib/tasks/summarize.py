from celery.task import task
import numpy as np

from lib.constants import ALL, ERROR, STATS
from lib.summary import summarize_df, summarize_with_groups
from models.dataset import Dataset
from models.observation import Observation


@task
def summarize(dataset, query, select, group):
    """
    Return a summary for the rows/values filtered by *query* and *select*
    and grouped by *group* or the overall summary if no group is specified.
    """
    # narrow list of observations via query/select
    dframe = Observation.find(dataset, query, select, as_df=True)

    # do not allow group by numeric types
    # TODO check schema for valid groupby columns once included
    _type = dframe.dtypes.get(group)
    if group != ALL and (_type is None or _type.type != np.object_):
        return {ERROR: "group: '%s' is not categorical." % group}

    # check cached stats for group and update as necessary
    stats = dataset.get(STATS, {})
    if not stats.get(group):
        stats = {ALL: summarize_df(dframe)} if group == ALL \
            else summarize_with_groups(dframe, stats, group)
        Dataset.update(dataset, {STATS: stats})
    stats_to_return = stats.get(group)

    return stats_to_return if group == ALL else {group: stats_to_return}
