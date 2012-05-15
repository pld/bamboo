import urllib2

from celery.task import task
from pandas import read_csv

from lib.constants import DATASET_ID
from models.dataset import Dataset
from models.observation import Observation


@task
def import_dataset(_file, dataset):
    """
    For reading a URL and saving the corresponding dataset.
    """

    try:
        dframe = read_csv(_file, na_values=['n/a'])
        Observation.save(dframe, dataset)
    except (IOError, urllib2.HTTPError):
        # error reading file/url, delete dataset
        Dataset.delete(dataset[DATASET_ID])
