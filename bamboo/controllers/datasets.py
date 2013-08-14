import urllib2

from external import bearcart
from pandas import concat
import vincent

from bamboo.controllers.abstract_controller import AbstractController
from bamboo.core.aggregations import AGGREGATIONS
from bamboo.core.frame import NonUniqueJoinError
from bamboo.core.merge import merge_dataset_ids, MergeError
from bamboo.core.summary import ColumnTypeError
from bamboo.lib.exceptions import ArgumentError
from bamboo.lib.jsontools import JSONError, safe_json_loads
from bamboo.lib.utils import parse_int
from bamboo.lib.query_args import QueryArgs
from bamboo.models.dataset import Dataset
from bamboo.models.observation import Observation


def valid_column(dataset, c):
    if c not in dataset.columns:
        raise ArgumentError("'%s' is not a column for this dataset." % c)


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

    - DEFAULT_AGGREGATION: The default aggregation for plotting with an index.
    - SELECT_ALL_FOR_SUMMARY: A string the the client can pass to
    - indicate that all columns should be summarized.

    """
    DEFAULT_AGGREGATION = 'sum'
    SELECT_ALL_FOR_SUMMARY = 'all'

    def delete(self, dataset_id, query=None):
        """Delete the dataset with hash `dataset_id`.

        This also deletes any observations associated with the dataset_id. The
        dataset and observations are not recoverable.

        :param dataset_id: The dataset ID of the dataset to be deleted.

        :returns: A string of success or error if that dataset could not be
            found.
        """
        def action(dataset, query=query):
            message = ' rows matching query: %s' % query if query else ''
            query = safe_json_loads(query)
            dataset.delete(query)

            return self._success('deleted dataset%s' % message, dataset_id)

        return self._safe_get_and_call(dataset_id, action)

    def info(self, dataset_id, callback=False):
        """Fetch and return the meta-data for a dataset.

        :param dataset_id: The dataset ID of the dataset to return meta-data
            for.
        :param callback: A JSONP callback function to wrap the result in.

        :returns: The data for `dataset_id`. Returns an error message if
            `dataset_id` does not exist.
        """
        def action(dataset):
            return dataset.info()

        return self._safe_get_and_call(dataset_id, action, callback=callback)

    def set_info(self, dataset_id, **kwargs):
        """Set the metadata for a dataset.

        :param dataset_id: ID of the dataset to update.
        :param attribution: Text to set the attribution to.
        :param description: Text to set the description to.
        :param label: Text to set the label to.
        :param license: Text to set the license to.

        :returns: Success or error.
        """
        def action(dataset):
            return dataset.info(kwargs)

        return self._safe_get_and_call(dataset_id, action)

    def summary(self, dataset_id, query={}, select=None, group=None, limit=0,
                order_by=None, callback=False, flat=False):
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
        :param flat: Return multigroups as a flat list.
        :param callback: A JSONP callback function to wrap the result in.

        :returns: An error message if `dataset_id` does not exist or the JSON
            for query or select is improperly formatted.  Otherwise the summary
            as a JSON string.

        :raises: `ArgumentError` if no select is supplied or dataset is not in
            ready state.
        """
        def action(dataset, query=query, select=select, limit=limit):
            if not dataset.is_ready:
                raise ArgumentError('dataset is not finished importing')

            limit = parse_int(limit, 0)
            query = self.__parse_query(query)
            select = self.__parse_select(select, required=True)

            groups = dataset.split_groups(group)
            [valid_column(dataset, c) for c in groups]

            # if select append groups to select
            if select:
                select.update(dict(zip(groups, [1] * len(groups))))

            query_args = QueryArgs(query=query,
                                   select=select,
                                   limit=limit,
                                   order_by=order_by)
            dframe = dataset.dframe(query_args)

            return dataset.summarize(dframe, groups=groups,
                                     no_cache=query or select, flat=flat)

        return self._safe_get_and_call(dataset_id, action, callback=callback,
                                       exceptions=(ColumnTypeError,))

    def aggregations(self, dataset_id, callback=False):
        """Return a dict of aggregated data for the given `dataset_id`.

        :param dataset_id: The dataset ID of the dataset to return aggregations
            for.
        :param callback: A JSONP callback function to wrap the result in.

        :returns: An error message if `dataset_id` does not exist. Otherwise,
            returns a dict of the form {[group]: [id]}.
        """
        def action(dataset):
            return dataset.aggregated_datasets_dict

        return self._safe_get_and_call(dataset_id, action, callback=callback)

    def show(self, dataset_id, query=None, select=None, distinct=None, limit=0,
             order_by=None, format=None, callback=False, count=False,
             index=False):
        """ Return rows for `dataset_id`, matching the passed parameters.

        Retrieve the dataset by ID then limit that data using the optional
        `query`, `select` and `limit` parameters. Order the results using
        `order_by` if passed.

        :param dataset_id: The dataset ID of the dataset to return.
        :param select: A MongoDB JSON query for select.
        :param distinct: A field to return distinct results for.
        :param query: If passed restrict results to rows matching this query.
        :param limit: If passed limit the rows to this number.
        :param order_by: If passed order the result using this column.
        :param format: Format of output data, 'json' or 'csv'
        :param callback: A JSONP callback function to wrap the result in.
        :param count: Return the count for this query.
        :param index: Include index with data.

        :returns: An error message if `dataset_id` does not exist or the JSON
            for query or select is improperly formatted. Otherwise a JSON
            string of the rows matching the parameters.
        """
        content_type = self.__content_type_for_format(format)

        def action(dataset, limit=limit, query=query, select=select):
            query_args = self.__parse_query_args(
                limit, order_by, query, select, distinct=distinct,
                dataset=dataset)

            if count:
                return dataset.count(query_args)
            else:
                dframe = dataset.dframe(query_args, index=index)

                if distinct:
                    return sorted(dframe[0].tolist())

            return self.__dataframe_as_content_type(content_type, dframe)

        return self._safe_get_and_call(
            dataset_id, action, callback=callback, content_type=content_type)

    def merge(self, dataset_ids, mapping=None):
        """Merge the datasets with the dataset_ids in `datasets`.

        :param dataset_ids: A JSON encoded array of dataset IDs for existing
            datasets.
        :param mapping: An optional mapping from original column names to
            destination names.

        :returns: An error if the datasets could not be found or less than two
            dataset IDs were passed.  Otherwise, the ID of the new merged
            dataset created by combining the datasets provided as an argument.
        """

        def action(dataset, dataset_ids=dataset_ids, mapping=mapping):
            mapping = safe_json_loads(mapping)

            dataset_ids = safe_json_loads(dataset_ids)
            dataset = merge_dataset_ids(dataset_ids, mapping)

            return {Dataset.ID: dataset.dataset_id}

        return self._safe_get_and_call(
            None, action, exceptions=(MergeError,), error = 'merge failed')

    def create(self, **kwargs):
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
                - today

        :param url: A URL to load a CSV file from. The URL must point to a CSV
            file.
        :param csv_file: An uploaded CSV file to read from.
        :param json_file: An uploaded JSON file to read from.
        :param schema: A SDF schema file (JSON)
        :param na_values: A JSON list of values to interpret as missing data.
        :param perish: Number of seconds after which to delete the dataset.

        :returns: An error message if `url`, `csv_file`, or `scehma` are not
            provided. An error message if an improperly formatted value raises
            a ValueError, e.g. an improperly formatted CSV file. An error
            message if the URL could not be loaded. Otherwise returns a JSON
            string with the dataset ID of the newly created dataset.  Note that
            the dataset will not be fully loaded until its state is set to
            ready.
        """
        return self.__create_or_update(**kwargs)

    def reset(self, dataset_id, **kwargs):
        """Replace a dataset's data.

        Takes equivalent arguments and returns equivalent values as `create`.
        """
        return self.__create_or_update(dataset_id=dataset_id, **kwargs)

    def update(self, dataset_id, update):
        """Update the `dataset_id` with the new rows as JSON.

        :param dataset_id: The ID of the dataset to update.
        :param update: The JSON to update the dataset with.

        :returns: A JSON dict with the ID of the dataset updated, or with an
            error message.
        """
        def action(dataset, update=update):
            update = safe_json_loads(update)
            dataset.add_observations(update)

            return {Dataset.ID: dataset_id}

        return self._safe_get_and_call(
            dataset_id, action, exceptions=(NonUniqueJoinError,))

    def drop_columns(self, dataset_id, columns):
        """Drop columns in dataset.

        Removes all the `columns` from the dataset with ID `dataset_id`.

        :param dataset_id: The ID of the dataset to update.
        :param columns: An array of columns within the dataset.

        :returns: An error if any column is not in the dataset. Otherwise a
            success message.
        """
        def action(dataset):
            deleted = dataset.delete_columns(columns)

            return self._success('dropped columns: %s' % deleted, dataset_id)

        return self._safe_get_and_call(dataset_id, action)

    def join(self, dataset_id, other_dataset_id, on=None):
        """Join the columns from two existing datasets.

        The `on` column must exists in both dataset. The values in the `on`
        `on` column of the other dataset must be unique.

        `on` can either be a single string to join on the same column in both
        datasets, or a comman separated list of the name for the left hand side
        and then the name for the right hand side. E.g.:

            * on=food_type,foodtype

        Will join on the left hand `food_type` column and the right hand
        `foodtype` column.


        :param dataset_id: The left hand dataset to be joined onto.
        :param other_dataset_id: The right hand to join.
        :param on: A column to join on, this column must be unique in the other
          dataset.

        :returns: Success and the new merged dataset ID or error message.
        """
        def action(dataset):
            other_dataset = Dataset.find_one(other_dataset_id)

            if other_dataset.record:
                merged_dataset = dataset.join(other_dataset, on)

                return self._success('joined dataset %s to %s on %s' % (
                    other_dataset_id, dataset_id, on),
                    merged_dataset.dataset_id)

        return self._safe_get_and_call(
            dataset_id, action, exceptions=(KeyError, NonUniqueJoinError))

    def resample(self, dataset_id, date_column, interval, how='mean',
                 query={}, format=None):
        """Resample a dataset.

        Resample a dataset based on the given interval and date column.

        :param dataset_id: The ID of the dataset to resample.
        :param date_column: This column determins the dates that are used in
            resampling.
        :param interval: The interval to use in resampling, any interval valid
            for the pandas resample function is accepted.
        :type interval: String
        :param how: The method of interpolation to use, any method valid for
            the pandas resampling function is accepted.
        :param query: A query to restrict the data which is resampled.
        :param format: The format of the returned DataFrame.

        :returns: A resampled DataFrame.
        """
        content_type = self.__content_type_for_format(format)

        def action(dataset, query=query):
            query = self.__parse_query(query)
            dframe = dataset.resample(date_column, interval, how, query=query)

            return self.__dataframe_as_content_type(content_type, dframe)

        return self._safe_get_and_call(dataset_id, action,
                                       exceptions=(TypeError,),
                                       content_type=content_type)

    def set_olap_type(self, dataset_id, column, olap_type):
        """Set the OLAP Type for this `column` of dataset.

        Only columns with an original OLAP Type of 'measure' can be modified.
        This includes columns with Simple Type integer, float, and datetime.

        :param dataset_id: The ID of the dataset to modify.
        :param column: The column to set the OLAP Type for.
        :param olap_type: The OLAP Type to set. Must be 'dimension' or
            'measure'.

        :returns: A success or error message.
        """

        def action(dataset):
            dataset.set_olap_type(column, olap_type)

            return self._success('set OLAP Type for column "%s" to "%s".' % (
                column, olap_type), dataset_id)

        return self._safe_get_and_call(dataset_id, action)

    def rolling(self, dataset_id, window, win_type='boxcar',
                format=None):
        """Calculate the rolling window over a dataset.

        Calculate a rolling aggregation over the dataset.

        :param dataset_id: The dataset to calculate a rolling aggregation for.
        :param window: The number of observations to include in each window of
            aggregation.
        :type window: Numeric
        :param win_type: The method of aggegating data in the window, default
            'boxcar'.  See pandas docs for details.
        :param format: The format of the returned DtaFrame.

        :returns: A DataFrame of aggregated rolling data.
        """
        content_type = self.__content_type_for_format(format)

        def action(dataset):
            dframe = dataset.rolling(str(win_type), int(window))
            return self.__dataframe_as_content_type(content_type, dframe)

        return self._safe_get_and_call(dataset_id, action,
                                       content_type=content_type)

    def row_delete(self, dataset_id, index):
        """Delete a row from dataset by index.

        :param dataset_id: The dataset to modify.
        :param index: The index to delete in the dataset.

        :returns: Success or error message.
        """
        def action(dataset):
            dataset.delete_observation(parse_int(index))

            return self._success('Deleted row with index "%s".' % index,
                                 dataset_id)

        return self._safe_get_and_call(dataset_id, action)

    def row_show(self, dataset_id, index):
        """Show a row by index.

        :param dataset_id: The dataset to fetch a row from.
        :param index: The index of the row to fetch.

        :returns: The requested row.
        """
        def action(dataset):
            row = Observation.find_one(dataset, parse_int(index))

            if row:
                return row.clean_record

        error_message = "No row exists at index %s" % index
        return self._safe_get_and_call(dataset_id, action, error=error_message)

    def row_update(self, dataset_id, index, data):
        """Update a row in dataset by index.

        Update the row in dataset identified by `index` by updating it with the
        JSON dict `data`.  If there is a column in the DataFrame for which
        there is not a corresponding key in `data` that column will not be
        modified.

        :param dataset_id: The dataset to modify.
        :param index: The index to update.
        :param data: A JSON dict to update the row with.

        :returns: Success or error message.
        """
        def action(dataset, data=data):
            data = safe_json_loads(data)
            dataset.update_observation(parse_int(index), data)

            return self._success('Updated row with index "%s".' % index,
                                 dataset_id)

        return self._safe_get_and_call(dataset_id, action,
                                       exceptions=(NonUniqueJoinError,))

    def plot(self, dataset_id, query=None, select=None, limit=0, group=None,
             order_by=None, index=None, plot_type='line', aggregation='sum',
             palette=None, vega=False, width=750, height=400):
        """Plot a dataset given restrictions.

        :param dataset_id: The dataset ID of the dataset to return.
        :param select: A MongoDB JSON query for select.
        :param distinct: A field to return distinct results for.
        :param query: If passed restrict results to rows matching this query.
        :param limit: If passed limit the rows to this number.
        :param group: Group by this column.
        :param order_by: If passed order the result using this column.
        :param index: If passed set this column as the index.
        :param plot_type: Option type of plot, may be: *area*, *bar*, *line*,
            *scatterplot*, or *stack*.  The default is *line*.
        :param aggregation: The type of aggregation to use.  The default is
            *sum*.
        :param palette: Color palette to use for the graph, accepts any of
            https://github.com/shutterstock/rickshaw#color-schemes, default
            spectrum14.
        :param vega: Output vega JSON.

        :returns: HTML with an embedded plot.
        """
        def action(dataset, select=select):
            query_args = self.__parse_query_args(limit, order_by, query,
                                                 select, dataset=dataset)

            numerics_select = dataset.schema.numerics_select
            query_select = query_args.select

            if query_select is None:
                query_select = numerics_select
            else:
                query_select = {k: v for k, v in query_select.items() if k in
                                numerics_select.keys()}

            if not query_select:
                raise ArgumentError(
                    'No numeric columns for dataset, or no select columns are '
                    'numeric. Select: %s.' % select)

            if index:
                query_select[index] = 1
                valid_column(dataset, index)

            if group:
                query_select[group] = 1
                valid_column(dataset, group)

            query_args.select = query_select

            dframe = dataset.dframe(query_args=query_args).dropna()
            axis = None

            if group or index:
                agg = self.__parse_aggregation(aggregation)

            if index:
                if group:
                    groupby = dframe.groupby(group)
                    dframes = []

                    for g in groupby.groups.keys():
                        renamed = {c: '%s %s' % (c, g) for c in dframe.columns}
                        data = groupby.get_group(g).groupby(index).agg(agg)
                        dframes.append(data.rename(columns=renamed))

                    dframe = concat(dframes).fillna(0).reset_index().groupby(
                        index).agg(agg).sort_index()
                else:
                    dframe = dframe.groupby(index).agg(agg)
            elif group:
                dframe = dframe.groupby(group).agg(agg).reset_index()
                axis = dframe[group].tolist()
                dframe = dframe.drop(group, axis=1)

            if vega:
                dframe.index = axis or dframe.index.map(float)
                vis = vincent.Bar()
                vis.tabular_data(dframe, columns=dframe.columns.tolist()[0:1])

                return vis.vega
            else:
                vis = bearcart.Chart(dframe, plt_type=plot_type, width=width,
                                     height=height, palette=palette,
                                     x_axis=axis or True,
                                     x_time=index is not None)

            return vis.build_html()

        return self._safe_get_and_call(
            dataset_id, action, content_type='text/html')

    def __create_or_update(self, url=None, csv_file=None, json_file=None,
                           schema=None, na_values=[], perish=0,
                           dataset_id=None):
        result = None
        error = 'url, csv_file or schema required'

        try:
            if schema or url or csv_file or json_file:
                if dataset_id is None:
                    dataset = Dataset()
                    dataset.save()
                else:
                    dataset = Dataset.find_one(dataset_id)
                    Observation.delete_all(dataset)

                if schema:
                    dataset.import_schema(schema)

                na_values = safe_json_loads(na_values)

                if url:
                    dataset.import_from_url(url, na_values=na_values)
                elif csv_file:
                    dataset.import_from_csv(csv_file, na_values=na_values)
                elif json_file:
                    dataset.import_from_json(json_file)

                result = {Dataset.ID: dataset.dataset_id}

            perish = parse_int(perish)
            perish and dataset.delete(countdown=perish)
        except urllib2.URLError:
            error = 'could not load: %s' % url
        except IOError:
            error = 'could not get a filehandle for: %s' % csv_file
        except JSONError as e:
            error = e.__str__()

        self.set_response_params(result, success_status_code=201)

        return self._dump_or_error(result, error)

    def __content_type_for_format(self, format):
        return self.CSV if format == 'csv' else self.JSON

    def __dataframe_as_content_type(self, content_type, dframe):
        if content_type == self.CSV:
            return dframe.to_csv_as_string()
        else:
            return dframe.to_jsondict()

    def __parse_select(self, select, required=False):
        if required and select is None:
            raise ArgumentError('no select')

        if select == self.SELECT_ALL_FOR_SUMMARY:
            select = None
        elif select is not None:
            select = safe_json_loads(select, error_title='select')

            if not isinstance(select, dict):
                raise ArgumentError(
                    'select argument must be a JSON dictionary, found: %s.' %
                    select)

        return select

    def __parse_query(self, query):
        return safe_json_loads(query) or {}

    def __parse_aggregation(self, agg):
        return agg if agg in AGGREGATIONS else self.DEFAULT_AGGREGATION

    def __parse_query_args(self, limit, order_by, query, select,
                           distinct=None, dataset=None):
            limit = parse_int(limit, 0)
            query = self.__parse_query(query)
            select = self.__parse_select(select)

            return QueryArgs(query=query,
                             select=select,
                             distinct=distinct,
                             limit=limit,
                             order_by=order_by,
                             dataset=dataset)
