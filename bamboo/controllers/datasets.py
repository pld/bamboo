import cherrypy
import urllib2

from bamboo.controllers.abstract_controller import AbstractController,\
    ArgumentError
from bamboo.core.merge import merge_dataset_ids, MergeError
from bamboo.core.summary import ColumnTypeError
from bamboo.lib.jsontools import JSONError
from bamboo.lib.io import create_dataset_from_url, create_dataset_from_csv
from bamboo.models.calculation import Calculation
from bamboo.models.dataset import Dataset


class Datasets(AbstractController):
    """
    The Datasets Controller provides access to data.  Datasets can store data
    from uploaded CSVs or URLs pointing to CSVs.  Additional rows can be passed
    as JSON and added to a dataset.

    All actions in the Datasets Controller can optionally take a *callback*
    parameter.  If passed the returned result will be wrapped this the
    parameter value.  E.g., is ``callback=parseResults`` the returned value
    will be ``parseResults([some-JSON])``, where ``some-JSON`` is the function
    return value.
    """
    SELECT_ALL_FOR_SUMMARY = 'all'

    def delete(self, dataset_id, callback=False):
        """
        Delete the dataset with hash *dataset_id* from mongo
        """
        dataset = Dataset.find_one(dataset_id)
        result = None

        if dataset.record:
            dataset.delete()
            result = {self.SUCCESS: 'deleted dataset: %s' % dataset_id}
        return self.dump_or_error(result, 'id not found', callback)

    def info(self, dataset_id, callback=False):
        """
        Return the data for *dataset_id*. Returns an error message if
        *dataset_id* does not exist.
        """
        def _action(dataset):
            return dataset.info()
        return self._safe_get_and_call(dataset_id, _action, callback)

    def summary(self, dataset_id, query=None, select=None,
                group=None, limit=0, order_by=None, callback=False):
        """

          - The *select* argument is required, it can be 'all' or a MongoDB
            JSON query
          - If *group* is passed group the summary.
          - If *query* is passed restrict summary to rows matching query.

        Returns an error message if *dataset_id* does not exist or the JSON for
        query or select is improperly formatted.
        """
        def _action(dataset, query=query, select=select, group=group,
                    limit=limit, order_by=order_by):
            if select is None:
                raise ArgumentError('no select')
            if select == self.SELECT_ALL_FOR_SUMMARY:
                select = None
            return dataset.summarize(dataset, query, select,
                                     group, limit=limit,
                                     order_by=order_by)
        return self._safe_get_and_call(dataset_id, _action, callback)

    def related(self, dataset_id, callback=False):
        """
        Return a dict of aggregated data for the given *dataset_id*.
        The dict is of the form {group: id}.

        Returns an error message if *dataset_id* does not exist.
        """
        def _action(dataset):
            return dataset.aggregated_datasets_dict
        return self._safe_get_and_call(dataset_id, _action, callback)

    def show(self, dataset_id, query=None, select=None,
             group=None, limit=0, order_by=None, callback=False):
        """
        Return rows for *dataset_id*, passing *query* and *select* onto
        MongoDB.

        Returns an error message if *dataset_id* does not exist or the JSON for
        query or select is improperly formatted.
        """
        def _action(dataset, query=query, select=select,
                    limit=limit, order_by=order_by):
            return dataset.dframe(
                query=query, select=select,
                limit=limit, order_by=order_by).to_jsondict()

        return self._safe_get_and_call(dataset_id, _action, callback)

    def merge(self, datasets=None, callback=False):
        """
        Merge the datasets with the dataset_ids in *datasets*.

        *dataset* should be a JSON encoded array of dataset IDs for existing
        datasets.

        Returns the ID of the new merged datasets created by combining the
        datasets provided as an argument.
        """
        result = None
        error = 'merge failed'

        try:
            dataset = merge_dataset_ids(datasets)
            result = {Dataset.ID: dataset.dataset_id}
        except (ValueError, MergeError) as e:
            error = e.__str__()

        return self.dump_or_error(result, error, callback)

    def create(self, url=None, csv_file=None, callback=False):
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

        return self.dump_or_error(result, error, callback)

    def update(self, dataset_id, callback=False):
        """
        Update the *dataset_id* with the body as JSON.
        """
        result = None
        error = 'dataset for this id does not exist'
        dataset = Dataset.find_one(dataset_id)
        if dataset.record:
            dataset.add_observations(cherrypy.request.body.read())
            result = {Dataset.ID: dataset_id}
        return self.dump_or_error(result, error, callback)

    def _safe_get_and_call(self, dataset_id, action, callback, **kwargs):
        dataset = Dataset.find_one(dataset_id)
        error = 'id not found'
        result = None

        try:
            if dataset.record:
                result = action(dataset, **kwargs)
        except (ArgumentError, ColumnTypeError, JSONError) as e:
            error = e.__str__()

        return self.dump_or_error(result, error, callback)
