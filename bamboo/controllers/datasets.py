import urllib2

from bamboo.controllers.abstract_controller import AbstractController
from bamboo.core.frame import NonUniqueJoinError
from bamboo.core.merge import merge_dataset_ids, MergeError
from bamboo.core.summary import ColumnTypeError
from bamboo.lib.exceptions import ArgumentError
from bamboo.lib.io import create_dataset_from_url, create_dataset_from_csv,\
    create_dataset_from_schema
from bamboo.lib.utils import parse_int
from bamboo.models.dataset import Dataset


class Datasets(AbstractController):
    """
    The Datasets Controller provides access to data.  Datasets can store data
    from uploaded CSVs or URLs pointing to CSVs.  Additional rows can be passed
    as JSON and added to a dataset.

    All actions in the Datasets Controller can optionally take a `callback`
    parameter.  If passed the returned result will be wrapped this the
    parameter value.  E.g., is ``callback=parseResults`` the returned value
    will be ``parseResults([some-JSON])``, where ``some-JSON`` is the function
    return value.

    Attributes:

    - SELECT_ALL_FOR_SUMMARY: a string the the client can pass to
    - indicate that all columns should be summarized.

    """
    SELECT_ALL_FOR_SUMMARY = 'all'

    def delete(self, dataset_id):
        """Delete the dataset with hash `dataset_id`.

        This also deletes any observations associated with the dataset_id. The
        dataset and observations are not recoverable.

        :param dataset_id: The dataset ID of the dataset to be deleted.

        :returns: A string of success or error if that dataset could not be
            found.
        """
        def _action(dataset):
            dataset.delete()
            return {self.SUCCESS: 'deleted dataset: %s' % dataset_id}

        return self._safe_get_and_call(dataset_id, _action)

    def info(self, dataset_id, callback=False):
        """Fetch and return the meta-data for a dataset.

        :param dataset_id: The dataset ID of the dataset to return meta-data
            for.
        :param callback: A JSONP callback function to wrap the result in.

        :returns: The data for `dataset_id`. Returns an error message if
            `dataset_id` does not exist.
        """
        def _action(dataset):
            return dataset.info()

        return self._safe_get_and_call(dataset_id, _action, callback=callback)

    def summary(self, dataset_id, query=None, select=None,
                group=None, limit=0, order_by=None, callback=False):
        """Return a summary of the dataset ID given the passed parameters.

        Retrieve the dataset by ID then limit that data using the optional
        `query`, `select` and `limit` parameters. Summarize the resulting data
        potentially grouping by the optional `group` parameter. Order the
        results using `order_by` if passed.

        :param dataset_id: The dataset ID of the dataset to summarize.
        :param select: This is a required argument, it can be 'all' or a
            MongoDB JSON query.
        :param group: If passed, group the summary by this column or list of
            columns.
        :param query: If passed restrict summary to rows matching this query.
        :param limit: If passed limit the rows to summarize to this number.
        :param order_by: If passed order the result using this column.
        :param callback: A JSONP callback function to wrap the result in.

        :returns: An error message if `dataset_id` does not exist or the JSON
            for query or select is improperly formatted.  Otherwise the summary
            as a JSON string.

        :raises: `ArgumentError` if no select is supplied or dataset is not in
            ready state.
        """
        def _action(dataset, select=select, limit=limit):
            if not dataset.is_ready:
                raise ArgumentError('dataset is not finished importing')
            if select is None:
                raise ArgumentError('no select')

            limit = parse_int(limit, 0)

            if select == self.SELECT_ALL_FOR_SUMMARY:
                select = None

            return dataset.summarize(dataset, query, select,
                                     group, limit=limit,
                                     order_by=order_by)

        return self._safe_get_and_call(dataset_id, _action, callback=callback,
                                       exceptions=(ColumnTypeError,))

    def aggregations(self, dataset_id, callback=False):
        """Return a dict of aggregated data for the given `dataset_id`.

        :param dataset_id: The dataset ID of the dataset to return aggregations
            for.
        :param callback: A JSONP callback function to wrap the result in.

        :returns: An error message if `dataset_id` does not exist. Otherwise,
            returns a dict of the form {[group]: [id]}.
        """
        def _action(dataset):
            return dataset.aggregated_datasets_dict

        return self._safe_get_and_call(dataset_id, _action, callback=callback)

    def show(self, dataset_id, query=None, select=None, distinct=None,
             limit=0, order_by=None, format=None, callback=False):
        """ Return rows for `dataset_id`, matching the passed parameters.

        Retrieve the dataset by ID then limit that data using the optional
        `query`, `select` and `limit` parameters. Order the results using
        `order_by` if passed.

        :param dataset_id: The dataset ID of the dataset to return.
        :param select: This is a required argument, it can be 'all' or a
            MongoDB JSON query
        :param distinct: A field to return distinct results for.
        :param query: If passed restrict results to rows matching this query.
        :param limit: If passed limit the rows to this number.
        :param order_by: If passed order the result using this column.
        :param format: Format of output data, 'json' or 'csv'
        :param callback: A JSONP callback function to wrap the result in.

        :returns: An error message if `dataset_id` does not exist or the JSON
            for query or select is improperly formatted. Otherwise a JSON
            string of the rows matching the parameters.
        """
        limit = parse_int(limit, 0)
        content_type = self.CSV if format == self.CSV else self.JSON

        def _action(dataset):
            dframe = dataset.dframe(
                query=query, select=select, distinct=distinct,
                limit=limit, order_by=order_by)

            if distinct:
                return sorted(dframe[0].tolist())

            if content_type == self.CSV:
                return dframe.to_csv_as_string()
            else:
                return dframe.to_jsondict()

        return self._safe_get_and_call(
            dataset_id, _action, callback=callback, content_type=content_type)

    def merge(self, datasets=None):
        """Merge the datasets with the dataset_ids in `datasets`.

        :param dataset: A JSON encoded array of dataset IDs for existing
            datasets.

        :returns: An error if the datasets could not be found or less than two
            dataset IDs were passed.  Otherwise, the ID of the new merged
            dataset created by combining the datasets provided as an argument.
        """

        def _action(dataset):
            dataset = merge_dataset_ids(datasets)
            return {Dataset.ID: dataset.dataset_id}

        return self._safe_get_and_call(
            None, _action, exceptions=(MergeError,), error = 'merge failed')

    def create(self, url=None, csv_file=None, schema=None, perish=0):
        """Create a dataset by URL, CSV or schema file.

        If `url` is provided, create a dataset by downloading a CSV from that
        URL. If `url` is not provided and `csv_file` is provided, create a
        dataset with the data in the passed `csv_file`. If both `url` and
        `csv_file` are provided, `csv_file` is ignored. If `schema` is
        supplied, an empty dataset is created with the associated column
        structure.

        .. note::

            The follow words are reserved and will be slugified by adding
            underscores (or multiple underscores to ensure uniqueness) if used
            as column names:

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

        :param url: A URL to load a CSV file from. The URL must point to a CSV
            file.
        :param csv_file: An uploaded CSV file to read from.
        :param schema: A SDF schema file (JSON)
        :param perish: Number of seconds after which to dlete the dataset.

        :returns: An error message if `url`, `csv_file`, or `scehma` are not
            provided. An error message if an improperly formatted value raises
            a ValueError, e.g. an improperly formatted CSV file. An error
            message if the URL could not be loaded. Otherwise returns a JSON
            string with the dataset ID of the newly created dataset.  Note that
            the dataset will not be fully loaded until its state is set to
            ready.
        """
        result = None
        error = 'url, csv_file or schema required'

        try:
            dataset = None
            if url:
                dataset = create_dataset_from_url(url)
            elif csv_file:
                dataset = create_dataset_from_csv(csv_file)
            elif schema:
                dataset = create_dataset_from_schema(schema)
            if dataset:
                result = {Dataset.ID: dataset.dataset_id}

            perish = parse_int(perish, None)
            if perish:
                dataset.delete(countdown=perish)
        except urllib2.URLError:
            error = 'could not load: %s' % url
        except IOError:
            error = 'could not get a filehandle for: %s' % csv_file

        self.set_response_params(result, success_status_code=201)
        return self.dump_or_error(result, error)

    def update(self, dataset_id, update):
        """Update the `dataset_id` with the new rows as JSON.

        :param dataset_id: The ID of the dataset to update.
        :param update: The JSON to update the dataset with.

        :returns: A JSON dict with the ID of the dataset updated, or with an
            error message.
        """
        def _action(dataset):
            dataset.add_observations(update)
            return {Dataset.ID: dataset_id}

        return self._safe_get_and_call(
            dataset_id, _action, exceptions=(NonUniqueJoinError,))

    def drop_columns(self, dataset_id, columns):
        """Drop columns in dataset.

        Removes all the `columns` from the dataset with ID `dataset_id`.

        :param dataset_id: The ID of the dataset to update.
        :param columns: An array of columns within the dataset.

        :returns: An error if any column is not in the dataset. Otherwise a
            success message.
        """
        def _action(dataset):
            dataset.drop_columns(columns)

            return {self.SUCCESS: 'in dataset %s dropped columns: %s' %
                    (dataset.dataset_id, columns)}

        return self._safe_get_and_call(dataset_id, _action)

    def join(self, dataset_id, other_dataset_id, on=None):
        """Join the columns from two existing datasets.

        The `on` column must exists in both dataset. The values in the `on`
        `on` column of the other dataset must be unique.

        :param dataset_id: The left hand dataset to be joined onto.
        :param other_dataset_id: The right hand to join.
        :param on: A column to join on, this column must be unique in the other
          dataset.

        :returns: Success and merged dataset ID or error message.
        """
        def _action(dataset):
            other_dataset = Dataset.find_one(other_dataset_id)

            if other_dataset.record:
                merged_dataset = dataset.join(other_dataset, on)

                return {
                    self.SUCCESS: 'joined dataset %s to %s on %s' % (
                        other_dataset_id, dataset.dataset_id, on),
                    Dataset.ID: merged_dataset.dataset_id,
                }

        return self._safe_get_and_call(
            dataset_id, _action, exceptions=(KeyError, NonUniqueJoinError))
