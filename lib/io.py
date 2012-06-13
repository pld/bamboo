import re
import tempfile
import urllib2
import uuid

from lib.constants import ERROR, ID
from lib.tasks.import_dataset import import_dataset
from models.dataset import Dataset


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
        protocols.update({'file':  lambda d: d['path']})

    regex = re.compile(
        '^(?P<url>(?P<protocol>%s):\/\/(?P<path>.+))$' \
        % '|'.join(protocols.keys())
    )
    match = re.match(regex, url)
    if match:
        args = match.groupdict()
        return protocols[args['protocol']](args)
    return None


def read_uploaded_file(_file, chunk_size=8192):
    data = ''
    for line in _file.file.xreadlines():
        data += line
    return data


def create_dataset_from_url(url, allow_local_file=False):
    """
    Load a URL, read from a CSV, create a dataset and return the unique ID.
    """
    _file = None

    try:
        _file = open_data_file(url, allow_local_file)
    except (IOError, urllib2.HTTPError):
        # error reading file/url, return
        pass

    if not _file:
        # could not get a file handle
        return {ERROR: 'could not get a filehandle for: %s' % url}

    dataset_id = uuid.uuid4().hex
    dataset = Dataset.create(dataset_id)
    import_dataset(_file, dataset)

    return {ID: dataset_id}


def create_dataset_from_csv(csv_file):
    """
    Create a dataset from the uploaded .csv file.
    """
    dataset_id = uuid.uuid4().hex
    dataset = Dataset.create(dataset_id)

    # need to write out to a named tempfile in order
    # to get a handle in order for pandas read_csv
    with tempfile.NamedTemporaryFile() as tmpfile:
        tmpfile.write(read_uploaded_file(csv_file))
        import_dataset(tmpfile.name, dataset)

    return {ID: dataset_id}
