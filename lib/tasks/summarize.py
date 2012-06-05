from celery.task import task

from lib.constants import STATS
from lib.summary import summarize_with_groups
from models.dataset import Dataset
from models.observation import Observation


@task
def summarize(dataset, query, select, group):
    observations = Observation.find(dataset, query, select, as_df=True)
    stats = dataset.get(STATS) or {}
    # if not saved stats, create and save
    if not stats.get(group):
        stats = summarize_with_groups(observations, stats, group)
        Dataset.update(dataset, {STATS: stats})
    return stats
