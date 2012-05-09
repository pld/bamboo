import re
import urllib2
import uuid
import threading

from pandas import read_csv

from lib.constants import DATASET_ID
from models.dataset import Dataset
from models.observation import Observation


class DatasetImporter(threading.Thread):
    """
    Thread for reading a URL and saving the corresponding dataset.
    """

    def __init__(self, _file, dataset):
        self._file = _file
        self._dataset = dataset
        threading.Thread.__init__(self)

    def run(self):
        try:
            dframe = read_csv(self._file, na_values=['n/a'])
            Observation.save(dframe, self._dataset)
        except (IOError, urllib2.HTTPError):
            # error reading file/url, delete dataset
            Dataset.delete(self._dataset[DATASET_ID])


def open_data_file(url):
    open_url = lambda d: urllib2.urlopen(d['url'])
    protocols = {
        'http':  open_url,
        'https': open_url,
        'file':  lambda d: d['path'],
    }
    regex = re.compile(
        '^(?P<url>(?P<protocol>%s):\/\/(?P<path>.+))$' \
        % '|'.join(protocols.keys())
    )
    match = re.match(regex, url)
    if match:
        args = match.groupdict()
        return protocols[args['protocol']](args)
    return None

def create_dataset_from_url(url):
    """
    Load a URL, read from a CSV, create a dataset and return the unique ID.
    """

    _file = open_data_file(url)
    if not _file:
        # could not get a file handle
        return 'could not get a filehandle'

    dataset_id = uuid.uuid4().hex
    dataset = Dataset.create(dataset_id)
    dataset_importer = DatasetImporter(_file, dataset)
    dataset_importer.start()

    return dataset_id
