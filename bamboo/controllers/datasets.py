import cherrypy
import urllib2

from bamboo.controllers.abstract_controller import AbstractController
from bamboo.core.merge import merge_dataset_ids, MergeError
from bamboo.core.summary import ColumnTypeError
from bamboo.lib.jsontools import JSONError
from bamboo.lib.io import create_dataset_from_url, create_dataset_from_csv
from bamboo.lib.utils import call_async
from bamboo.models.calculation import Calculation
from bamboo.models.dataset import Dataset
from bamboo.models.observation import Observation


class Datasets(AbstractController):
    'Datasets controller'

    SELECT_ALL_FOR_SUMMARY = 'all'

    # modes for dataset GET
    MODE_INFO = 'info'
    MODE_RELATED = 'related'
    MODE_SUMMARY = 'summary'

    def delete(self, dataset_id):
        """
        Delete the dataset with hash *dataset_id* from mongo
        """
        dataset = Dataset.find_one(dataset_id)
        result = None

        if dataset.record:
            task = call_async(dataset.delete, dataset, dataset)
            result = {self.SUCCESS: 'deleted dataset: %s' % dataset_id}
        return self.dump_or_error(result, 'id not found')

    def info(self, dataset_id):
        dataset = Dataset.find_one(dataset_id)
        result = None
        error = 'id not found'

        try:
            if dataset.record:
                result = dataset.info()
        except (ColumnTypeError, JSONError) as e:
            error = e.__str__()

        return self.dump_or_error(result, error)

    def summary(self, dataset_id, query=None, select=None,
            group=None, limit=0, order_by=None):
        dataset = Dataset.find_one(dataset_id)
        result = None
        error = 'id not found'

        try:
            if dataset.record:
                if select is None:
                    error = 'no select'
                else:
                    if select == self.SELECT_ALL_FOR_SUMMARY:
                        select = None
                    result = dataset.summarize(dataset, query, select,
                                               group, limit=limit,
                                               order_by=order_by)
        except (ColumnTypeError, JSONError) as e:
            error = e.__str__()

        return self.dump_or_error(result, error)

    def related(self, dataset_id):
        dataset = Dataset.find_one(dataset_id)
        result = None
        error = 'id not found'

        try:
            if dataset.record:
                result = dataset.aggregated_datasets_dict
        except (ColumnTypeError, JSONError) as e:
            error = e.__str__()

        return self.dump_or_error(result, error)

    def show(self, dataset_id, query=None, select=None,
            group=None, limit=0, order_by=None):
        """
        Based on *mode* perform different operations on the dataset specified
        by *dataset_id*.

        - *info*: return the meta-data and schema of the dataset.
        - *related*: return the dataset_ids of linked datasets for the dataset.
        - *summary*: return summary statistics for the dataset.

          - The *select* argument is required, it can be 'all' or a MongoDB
            JSON query
          - If *group* is passed group the summary.
          - If *query* is passed restrict summary to rows matching query.

        - no mode passed: Return the raw data for the dataset.
          - Restrict to *query* and *select* if passed.

        Returns an error message if dataset_id does not exists, mode does not
        exist, or the JSON for query or select is improperly formatted.
        Otherwise, returns the result from above dependent on mode.
        """
        dataset = Dataset.find_one(dataset_id)
        result = None
        error = 'id not found'

        try:
            if dataset.record:
                return dataset.dframe(
                    query=query, select=select,
                    limit=limit, order_by=order_by).to_json()
        except (ColumnTypeError, JSONError) as e:
            error = e.__str__()

        return self.dump_or_error(result, error)

    def create(self, merge=None, url=None, csv_file=None, datasets=None):
        """
        If *url* is provided read data from URL *url*.
        If *csv_file* is provided read data from *csv_file*.
        If neither are provided return an error message.  Also return an error
        message if an improperly formatted value raises a ValueError, e.g. an
        improperly formatted CSV file.

        The follow words are reserved and will lead to unexpected behavior if
        used as column names:

            - sum
            - date
            - years
            - and
            - or
            - not
            - in
            - default

        """
        result = None
        error = 'url or csv_file required'

        try:
            if merge:
                dataset = merge_dataset_ids(datasets)
            elif url:
                dataset = create_dataset_from_url(url)
            elif csv_file:
                dataset = create_dataset_from_csv(csv_file)
            result = {Dataset.ID: dataset.dataset_id}
        except (ValueError, MergeError) as e:
            error = e.__str__()
        except IOError:
            error = 'could not get a filehandle for: %s' % csv_file
        except urllib2.URLError:
            error = 'could not load: %s' % url

        return self.dump_or_error(result, error)

    def PUT(self, dataset_id):
        """
        Update the *dataset_id* with the body as JSON.
        """
        result = None
        error = 'dataset for this id does not exist'
        dataset = Dataset.find_one(dataset_id)
        if dataset:
            dataset.add_observations(cherrypy.request.body.read())
            result = {Dataset.ID: dataset_id}
        return self.dump_or_error(result, error)
