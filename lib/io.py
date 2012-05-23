import re
import urllib2
import uuid

from lib.tasks.import_dataset import import_dataset
from models.dataset import Dataset


def open_data_file(url):
    """
    Handle url and file handles
    """

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

    _file = None

    try:
        _file = open_data_file(url)
    except (IOError, urllib2.HTTPError):
        # error reading file/url, return
        pass

    if not _file:
        # could not get a file handle
        return {'error': 'could not get a filehandle for: %s' % url}

    dataset_id = uuid.uuid4().hex
    dataset = Dataset.create(dataset_id)
    import_dataset(_file, dataset)

    return {'id': dataset_id}
