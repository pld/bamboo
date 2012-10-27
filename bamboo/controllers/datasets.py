import cherrypy
import urllib2

from bamboo.controllers.abstract_controller import AbstractController,\
    ArgumentError
from bamboo.core.merge import merge_dataset_ids, MergeError
from bamboo.core.summary import ColumnTypeError
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

    Attributes:
        SELECT_ALL_FOR_SUMMARY: a string the the client can pass to
        indicate that all columns should be summarized.
    """
    SELECT_ALL_FOR_SUMMARY = 'all'

    def delete(self, dataset_id):
        """Delete the dataset with hash *dataset_id*.

        This also deletes any observations associated with the dataset_id. The
        dataset and observations are not recoverable.

        Args:
            dataset_id: The dataset ID of the dataset to be deleted.

        Returns:
            A string of success or error if that dataset could not be found.
        """
        dataset = Dataset.find_one(dataset_id)
        result = None

        if dataset.record:
            dataset.delete()
            result = {self.SUCCESS: 'deleted dataset: %s' % dataset_id}
        return self.dump_or_error(result, 'id not found')

    def info(self, dataset_id, callback=False):
        """Fetch and return the meta-data for a dataset.

        Args:
            dataset_id: The dataset ID of the dataset to return meta-data for.
            callback: A JSONP callback function to wrap the result in.

        Returns:
            The data for *dataset_id*. Returns an error message if
            *dataset_id* does not exist.
        """
        def _action(dataset):
            return dataset.info()
        return self._safe_get_and_call(dataset_id, _action, callback=callback)

    def summary(self, dataset_id, query=None, select=None,
                group=None, limit=0, order_by=None, callback=False):
        """Return a summary of the dataset ID given the passed parameters.

        Retrieve the dataset by ID then limit that data using the optional
        *query*, *select* and *limit* parameters. Summarize the resulting data
        potentially grouping by the optional *group* parameter. Order the
        results using *order_by* if passed.

        Args:
            dataset_id: The dataset ID of the dataset to summarize.
            select: This is a required argument, it can be 'all' or a MongoDB
                JSON query
            group: If passed, group the summary by this column or list of
                columns.
            query: If passed restrict summary to rows matching this query.
            limit: If passed limit the rows to summarize to this number.
            order_by: If passed order the result using this column.
            callback: A JSONP callback function to wrap the result in.

        Returns:
            An error message if *dataset_id* does not exist or the JSON for
            query or select is improperly formatted.  Otherwise the summary as
            a JSON string.
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
        return self._safe_get_and_call(dataset_id, _action, callback=callback,
                                       exceptions=(ColumnTypeError,))

    def related(self, dataset_id, callback=False):
        """Return a dict of aggregated data for the given *dataset_id*.

        Args:
            dataset_id: The dataset ID of the dataset to return related data
                for.
            callback: A JSONP callback function to wrap the result in.

        Returns:
            An error message if *dataset_id* does not exist. Otherwise, returns
            a dict of the form {[group]: [id]}.
        """
        def _action(dataset):
            return dataset.aggregated_datasets_dict
        return self._safe_get_and_call(dataset_id, _action, callback=callback)

    def show(self, dataset_id, query=None, select=None,
             group=None, limit=0, order_by=None, callback=False):
        """ Return rows for *dataset_id*, matching the passed parameters.

        Retrieve the dataset by ID then limit that data using the optional
        *query*, *select* and *limit* parameters. Order the results using
        *order_by* if passed.  The return the results grouped by the *group*
        parameter if it is passed.

        Args:
            dataset_id: The dataset ID of the dataset to return.
            select: This is a required argument, it can be 'all' or a MongoDB
                JSON query
            group: If passed, group the resuts by this column or list of
                columns.
            query: If passed restrict results to rows matching this query.
            limit: If passed limit the rows to this number.
            order_by: If passed order the result using this column.
            callback: A JSONP callback function to wrap the result in.

        Returns:
            An error message if *dataset_id* does not exist or the JSON for
            query or select is improperly formatted. Otherwise a JSON string of
            the rows matching the parameters.
        """
        def _action(dataset, query=query, select=select,
                    limit=limit, order_by=order_by):
            return dataset.dframe(
                query=query, select=select,
                limit=limit, order_by=order_by).to_jsondict()

        return self._safe_get_and_call(dataset_id, _action, callback=callback)

    def merge(self, datasets=None):
        """Merge the datasets with the dataset_ids in *datasets*.

        Args:
            dataset: A JSON encoded array of dataset IDs for existing
                datasets.

        Returns:
            An error if the datasets could not be found or less than two
            dataset IDs were passed.  Otherwise, the ID of the new merged
            dataset created by combining the datasets provided as an argument.
        """
        result = None
        error = 'merge failed'

        try:
            dataset = merge_dataset_ids(datasets)
            result = {Dataset.ID: dataset.dataset_id}
        except (ValueError, MergeError) as e:
            error = e.__str__()

        return self.dump_or_error(result, error)

    def create(self, url=None, csv_file=None):
        """Create a dataset by URL or passed CSV file.

        If *url* is provided, create a dataset by downloading a CSV from that
        URL. If *url* is not provided and *csv_file* is provided, create a
        dataset with the data in the passed *csv_file*. If both *url* and
        *csv_file* are provided, *csv_file* is ignored.

        The follow words are reserved and will lead to unexpected behavior if
        used as column names:

            - all
            - and
            - case
            - date
            - default
            - in
            - not
            - or
            - sum
            - years

        Args:
            url: A URL to load a CSV file from. The URL must point to a CSV
                file.
            csv_file: An uploaded CSV file to read from.

        Returns:
            An error message ff neither *url* nor *csv_file* are provided. An
            error message if an improperly formatted value raises a ValueError,
            e.g. an improperly formatted CSV file. An error message if the URL
            could not be loaded. Otherwise returns a JSON string with the
            dataset ID of the newly created dataset.  Note that the dataset
            will not be fully loaded until its state is set to ready.
        """
        result = None
        error = 'url or csv_file required'

        if url or csv_file:
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

    def drop_columns(self, dataset_id, columns):
        """
        Drop columns in dataset with id *dataset_id*.  *columns* is an array of
        columns within the dataset.  Returns an error if any column is not in
        the dataset.
        """
        def _action(dataset, columns=columns):
            dataset.drop_columns(columns)
            return {self.SUCCESS: 'in dataset %s dropped columns: %s' %
                    (dataset_id, columns)}
        return self._safe_get_and_call(dataset_id, _action)
