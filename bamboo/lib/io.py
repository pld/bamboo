import simplejson as json
import os
import tempfile

from celery.task import task
import pandas as pd

from bamboo.core.frame import BambooFrame
from bamboo.lib.async import call_async
from bamboo.models.dataset import Dataset


@task
def import_dataset(dataset, dframe=None, filepath_or_buffer=None,
                   delete=False):
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
        if filepath_or_buffer:
            dframe = BambooFrame(
                pd.read_csv(filepath_or_buffer)).recognize_dates()

        dataset.save_observations(dframe)
    except Exception as e:
        dataset.failed()
        dataset.delete(countdown=86400)
    finally:
        if delete and filepath_or_buffer:
            os.unlink(filepath_or_buffer)


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
    call_async(import_dataset, dataset, filepath_or_buffer=url)

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

    call_async(import_dataset, dataset, filepath_or_buffer=tmpfile.name,
               delete=True)

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
