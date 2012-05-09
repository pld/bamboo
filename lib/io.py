import re
import urllib2

from pandas import read_csv

from models.dataset import Dataset


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

    # reading large csvs leads to poor memory allocation
    # run in subprocess
    if not _file:
        # could not get a file handle
        return
    try:
        dframe = read_csv(_file, na_values=['n/a'])
    except (IOError, HTTPError):
        return # error reading file/url
    return Dataset.create(dframe, url=url)
