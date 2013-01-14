from functools import partial
import simplejson as json
import os
import tempfile

from celery.task import task
import pandas as pd

from bamboo.core.frame import BambooFrame
from bamboo.lib.async import call_async
from bamboo.models.dataset import Dataset


@task
def import_dataset(dataset, dframe=None, file_reader=None):
    """For reading a URL and saving the corresponding dataset.

    Import the `dframe` into the `dataset` if passed.  If a
    `filepath_or_buffer` is passed load as a dframe.  All exceptions are caught
    and on exception the dataset is marked as failed and set for
    deletion after 24 hours.

    :param dataset: The dataset to import into.
    :param dframe: The DataFrame to import, default None.
    :param filepath_or_buffer: Link to file to import, default None.
    :param delete: Delete filepath_or_buffer after import, default False.
    """
    try:
        if file_reader:
            dframe = file_reader()

        dataset.save_observations(dframe)
    except Exception as e:
        dataset.failed()
        dataset.delete(countdown=86400)


def _file_reader(name, delete=False):
    try:
        return BambooFrame(
            pd.read_csv(name)).recognize_dates()
    finally:
        if delete:
            os.unlink(name)


def create_dataset_from_url(url, allow_local_file=False):
    """Load a URL, read from a CSV, create a dataset and return the unique ID.

    :param url: URL to load file from.
    :param allow_local_file: Allow URL to refer to a local file.

    :raises: `IOError` for an unreadable file or a bad URL.

    :returns: The created dataset.
    """
    if not allow_local_file and isinstance(url, basestring)\
            and url[0:4] == 'file':
        raise IOError

    dataset = Dataset()
    dataset.save()
    call_async(import_dataset, dataset, file_reader=partial(_file_reader, url))

    return dataset


def create_dataset_from_csv(csv_file):
    """Create a dataset from a CSV file.

    .. note::

        Write to a named tempfile in order  to get a handle for pandas'
        `read_csv` function.

    :param csv_file: The CSV File to create a dataset from.

    :returns: The created dataset.
    """
    tmpfile = tempfile.NamedTemporaryFile(delete=False)
    tmpfile.write(csv_file.file.read())

    # pandas needs a closed file for *read_csv*
    tmpfile.close()

    dataset = Dataset()
    dataset.save()

    call_async(import_dataset, dataset,
               file_reader=partial(_file_reader, tmpfile.name, delete=True))

    return dataset


def create_dataset_from_json(json_file):
    content = json_file.file.read()

    dataset = Dataset()
    dataset.save()

    def file_reader(content):
        return pd.DataFrame(json.loads(content))

    call_async(import_dataset, dataset,
               file_reader=partial(file_reader, content))

    return dataset


def create_dataset_from_schema(schema):
    """Create a dataset from a SDF schema file (JSON).

    :param schema: The SDF (JSON) file to create a dataset from.

    :returns: The created dataset.
    """
    try:
        schema = json.loads(schema.file.read())
    except AttributeError:
        schema = json.loads(schema)

    dataset = Dataset()
    dataset.save()
    dataset.set_schema(schema)

    call_async(import_dataset, dataset)

    return dataset
