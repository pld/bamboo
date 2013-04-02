from functools import partial
import simplejson as json
import os
import tempfile

from celery.exceptions import RetryTaskError
from celery.task import task
import pandas as pd

from bamboo.core.frame import BambooFrame
from bamboo.lib.async import call_async


@task(ignore_result=True)
def import_dataset(dataset, file_reader, delete=False):
    """For reading a URL and saving the corresponding dataset.

    Import a DataFrame using the provided `file_reader` function. All
    exceptions are caught and on exception the dataset is marked as failed and
    set for deletion after 24 hours.

    :param dataset: The dataset to import into.
    :param file_reader: Function for reading the dataset.
    :param delete: Delete filepath_or_buffer after import, default False.
    """
    try:
        dframe = file_reader()
        dataset.save_observations(dframe)
    except Exception as e:
        if isinstance(e, RetryTaskError):
            raise e
        else:
            dataset.failed(e.__str__())
            dataset.delete(countdown=86400)


def csv_file_reader(name, na_values=[], delete=False):
    try:
        return BambooFrame(pd.read_csv(
            name, encoding='utf-8', na_values=na_values)).recognize_dates()
            #pd.read_csv(name, encoding='utf-8')).recognize_dates()
    finally:
        if delete:
            os.unlink(name)


def json_file_reader(content):
    return BambooFrame(json.loads(content)).recognize_dates()


class ImportableDataset(object):
    def import_from_url(self, url, na_values=[], allow_local_file=False):
        """Load a URL, read from a CSV, add data to dataset.

        :param url: URL to load file from.
        :param allow_local_file: Allow URL to refer to a local file.

        :raises: `IOError` for an unreadable file or a bad URL.

        :returns: The created dataset.
        """
        if not allow_local_file and isinstance(url, basestring)\
                and url[0:4] == 'file':
            raise IOError

        call_async(
            import_dataset, self, partial(
                csv_file_reader, url, na_values=na_values))

        return self

    def import_from_csv(self, csv_file, na_values=[]):
        """Import data from a CSV file.

        .. note::

            Write to a named tempfile in order  to get a handle for pandas'
            `read_csv` function.

        :param csv_file: The CSV File to create a dataset from.

        :returns: The created dataset.
        """
        if 'file' in dir(csv_file):
            csv_file = csv_file.file

        tmpfile = tempfile.NamedTemporaryFile(delete=False)
        tmpfile.write(csv_file.read())

        # pandas needs a closed file for *read_csv*
        tmpfile.close()

        call_async(import_dataset, self, partial(
            csv_file_reader, tmpfile.name, na_values=na_values, delete=True))

        return self

    def import_from_json(self, json_file):
        """Impor data from a JSON file.

        :param json_file: JSON file to import.
        """
        content = json_file.file.read()

        call_async(import_dataset, self,
                   partial(json_file_reader, content))

        return self

    def import_schema(self, schema):
        """Create a dataset from a SDF schema file (JSON).

        :param schema: The SDF (JSON) file to create a dataset from.

        :returns: The created dataset.
        """
        try:
            schema = json.loads(schema.file.read())
        except AttributeError:
            schema = json.loads(schema)

        self.set_schema(schema)
        self.ready()

        return self
