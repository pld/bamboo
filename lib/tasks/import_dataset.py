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
    dframe = read_csv(_file)
    Observation.save(dframe, dataset)
