from functools import partial
import simplejson as json
import os
import tempfile

from celery.exceptions import RetryTaskError
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
        if isinstance(e, RetryTaskError):
            raise e
        else:
            dataset.failed()
            dataset.delete(countdown=86400)


def _file_reader(name, delete=False):
    try:
        return BambooFrame(
            pd.read_csv(name)).recognize_dates()
    finally:
        if delete:
            os.unlink(name)


def import_data_from_url(dataset, url, allow_local_file=False):
    """Load a URL, read from a CSV, add data to dataset.

    :param dataset: Dataset to save in.
    :param url: URL to load file from.
    :param allow_local_file: Allow URL to refer to a local file.

    :raises: `IOError` for an unreadable file or a bad URL.

    :returns: The created dataset.
    """
    if not allow_local_file and isinstance(url, basestring)\
            and url[0:4] == 'file':
        raise IOError

    call_async(import_dataset, dataset, file_reader=partial(_file_reader, url))

    return dataset


def import_data_from_csv(dataset, csv_file):
    """Import data from a CSV file.

    .. note::

        Write to a named tempfile in order  to get a handle for pandas'
        `read_csv` function.

    :param dataset: Dataset to save in.
    :param csv_file: The CSV File to create a dataset from.

    :returns: The created dataset.
    """
    tmpfile = tempfile.NamedTemporaryFile(delete=False)
    tmpfile.write(csv_file.file.read())

    # pandas needs a closed file for *read_csv*
    tmpfile.close()

    call_async(import_dataset, dataset,
               file_reader=partial(_file_reader, tmpfile.name, delete=True))

    return dataset


def import_data_from_json(dataset, json_file):
    """Impor data from a JSON file.

    :param dataset: Dataset to save in.
    :param json_file: JSON file to import.
    """
    content = json_file.file.read()

    def file_reader(content):
        return pd.DataFrame(json.loads(content))

    call_async(import_dataset, dataset,
               file_reader=partial(file_reader, content))

    return dataset


def import_schema_for_dataset(dataset, schema):
    """Create a dataset from a SDF schema file (JSON).

    :param schema: The SDF (JSON) file to create a dataset from.

    :returns: The created dataset.
    """
    try:
        schema = json.loads(schema.file.read())
    except AttributeError:
        schema = json.loads(schema)

    dataset.set_schema(schema)

    call_async(import_dataset, dataset)

    return dataset
