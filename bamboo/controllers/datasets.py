import cherrypy
import urllib2

from bamboo.controllers.abstract_controller import AbstractController,\
    ArgumentError
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
        def _action(dataset):
            return dataset.info()
        return self._safe_get_and_call(dataset_id, _action)

    def summary(self, dataset_id, query=None, select=None,
            group=None, limit=0, order_by=None):
        def _action(dataset, query=query, select=select, group=group,
                    limit=limit, order_by=order_by):
            if select is None:
                raise ArgumentError('no select')
            if select == self.SELECT_ALL_FOR_SUMMARY:
                select = None
            return dataset.summarize(dataset, query, select,
                                       group, limit=limit,
                                       order_by=order_by)
        return self._safe_get_and_call(dataset_id, _action)

    def related(self, dataset_id):
        def _action(dataset):
            return dataset.aggregated_datasets_dict
        return self._safe_get_and_call(dataset_id, _action)

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
        def _action(dataset, query=query, select=select,
                    limit=limit, order_by=order_by):
            return dataset.dframe(
                query=query, select=select,
                limit=limit, order_by=order_by).to_jsondict()

        return self._safe_get_and_call(dataset_id, _action)

    def merge(self, datasets=None):
        result = None
        error = 'merge failed'

        try:
            dataset = merge_dataset_ids(datasets)
            result = {Dataset.ID: dataset.dataset_id}
        except (ValueError, MergeError) as e:
            error = e.__str__()

        return self.dump_or_error(result, error)

    def create(self, url=None, csv_file=None):
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
            if url:
                dataset = create_dataset_from_url(url)
            elif csv_file:
                dataset = create_dataset_from_csv(csv_file)
            result = {Dataset.ID: dataset.dataset_id}
        except IOError:
            error = 'could not get a filehandle for: %s' % csv_file
        except urllib2.URLError:
            error = 'could not load: %s' % url

        return self.dump_or_error(result, error)

    def update(self, dataset_id):
        """
        Update the *dataset_id* with the body as JSON.
        """
        result = None
        error = 'dataset for this id does not exist'
        dataset = Dataset.find_one(dataset_id)
        if dataset.record:
            dataset.add_observations(cherrypy.request.body.read())
            result = {Dataset.ID: dataset_id}
        return self.dump_or_error(result, error)

    def _safe_get_and_call(self, dataset_id, action, **kwargs):
        dataset = Dataset.find_one(dataset_id)
        error = 'id not found'
        result = None

        try:
            if dataset.record:
                result = action(dataset, **kwargs)
        except (ArgumentError, ColumnTypeError, JSONError) as e:
            error = e.__str__()

        return self.dump_or_error(result, error)
