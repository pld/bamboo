import os
import re
import tempfile
import urllib2

from celery.task import task
from pandas import read_csv

from bamboo.core.frame import DATASET_ID
from bamboo.models.observation import Observation
from bamboo.lib.datetools import recognize_dates
from bamboo.lib.utils import call_async
from bamboo.models.dataset import Dataset


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


def open_data_file(url, allow_local_file=False):
    """
    Handle url and file handles
    """
    open_url = lambda d: urllib2.urlopen(d['url'])
    protocols = {
        'http':  open_url,
        'https': open_url,
    }
    if allow_local_file:
        protocols.update({'file': lambda d: d['path']})

    regex = re.compile(
        '^(?P<url>(?P<protocol>%s):\/\/(?P<path>.+))$'
        % '|'.join(protocols.keys())
    )
    match = re.match(regex, url)
    if match:
        args = match.groupdict()
        return protocols[args['protocol']](args)
    return None


def create_dataset_from_url(url, allow_local_file=False):
    """
    Load a URL, read from a CSV, create a dataset and return the unique ID.

    Raises an IOError or urllib2.HTTPError if bad file or URL given.
    """
    _file = open_data_file(url, allow_local_file)

    if not _file:
        raise IOError

    dataset = Dataset()
    dataset.save()
    call_async(import_dataset, dataset, dataset, _file=_file)

    return dataset.dataset_id


def create_dataset_from_csv(csv_file):
    """
    Create a dataset from the uploaded .csv file.
    """
    # need to write out to a named tempfile in order
    # to get a handle for pandas *read_csv* function
    tmpfile = tempfile.NamedTemporaryFile(delete=False)
    tmpfile.write(csv_file.file.read())

    # pandas needs a closed file for *read_csv*
    tmpfile.close()

    dataset = Dataset()
    dataset.save()

    call_async(
        import_dataset, dataset, dataset, _file=tmpfile.name, delete=True)

    return {ID: dataset.dataset_id}
