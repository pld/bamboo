import os

from celery.task import task
from pandas import read_csv

from bamboo.lib.constants import DATASET_ID
from bamboo.lib.utils import recognize_dates
from bamboo.models.dataset import Dataset
from bamboo.models.observation import Observation


@task
def import_dataset(dataset, dframe=None, _file=None, delete=False):
    """
    For reading a URL and saving the corresponding dataset.
    """
    if _file:
        dframe = recognize_dates(read_csv(_file))
    if delete:
        os.unlink(_file)
    Observation().save(dframe, dataset)
