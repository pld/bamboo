from celery.task import task
from pandas import read_csv

from lib.constants import DATASET_ID
from lib.utils import recognize_dates
from models.dataset import Dataset
from models.observation import Observation


@task
def import_dataset(dataset, dframe=None, _file=None):
    """
    For reading a URL and saving the corresponding dataset.
    """
    if _file:
        dframe = recognize_dates(read_csv(_file))
    Observation().save(dframe, dataset)
