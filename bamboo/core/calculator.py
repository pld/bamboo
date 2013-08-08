from collections import defaultdict

from celery.task import task
from pandas import concat, DataFrame

from bamboo.core.aggregator import Aggregator
from bamboo.core.frame import BambooFrame, NonUniqueJoinError
from bamboo.core.parser import ParseError, Parser
from bamboo.lib.mongo import MONGO_ID, MONGO_ID_ENCODED
from bamboo.lib.query_args import QueryArgs
from bamboo.lib.schema_builder import make_unique
from bamboo.lib.utils import combine_dicts, flatten, to_list


def build_columns(dataset, dframe, functions, name, no_index):
    columns = []

    for function in functions:
        column = dframe.apply(function, axis=1,
                              args=(dataset,))
        column.name = make_unique(name, [c.name for c in columns])

        if no_index:
            column = column.reset_index(drop=True)

        columns.append(column)

    return columns


def calculation_data(dataset):
    """Create a list of aggregate calculation information.

    Builds a list of calculation information from the current datasets
    aggregated datasets and aggregate calculations.
    """
    calcs_to_data = defaultdict(list)

    calculations = dataset.calculations(only_aggs=True)
    names_to_formulas = {c.name: c.formula for c in calculations}
    names = set(names_to_formulas.keys())

    for group, dataset in dataset.aggregated_datasets:
        labels_to_slugs = dataset.schema.labels_to_slugs
        calculations_for_dataset = list(set(
            labels_to_slugs.keys()).intersection(names))

        for calc in calculations_for_dataset:
            calcs_to_data[calc].append((
                names_to_formulas[calc],
                labels_to_slugs[calc],
                group,
                dataset
            ))

    return flatten(calcs_to_data.values())


def parse_aggregation(dataset, parser, formula, name, groups, dframe=None):
    # TODO this should work with index eventually
    columns = parse_columns(
        dataset, parser, formula, name, dframe, no_index=True)

    # get dframe with only the necessary columns
    select = combine_dicts({group: 1 for group in groups},
                           {col: 1 for col in parser.dependent_columns})

    # ensure at least one column (MONGO_ID) for the count aggregation
    query_args = QueryArgs(select=select or {MONGO_ID: 1})
    dframe = dataset.dframe(query_args=query_args,
                            keep_mongo_keys=not select)

    return Aggregator(dataset, dframe, groups,
                      parser.aggregation, name, columns)


def parse_columns(dataset, parser, formula, name, dframe=None, no_index=False):
    """Parse a formula and return columns resulting from its functions.

    Parse a formula into a list of functions then apply those functions to
    the Data Frame and return the resulting columns.

    :param formula: The formula to parse.
    :param name: Name of the formula.
    :param dframe: A DataFrame to apply functions to.
    :param no_index: Drop the index on result columns.
    """
    functions = parser.parse_formula(formula, dataset)

    # make select from dependent_columns
    if dframe is None:
        select = {col: 1 for col in parser.dependent_columns or [MONGO_ID]}

        dframe = dataset.dframe(
            query_args=QueryArgs(select=select),
            keep_mongo_keys=True).set_index(MONGO_ID_ENCODED)

        if not parser.dependent_columns:
            # constant column, use dummy
            dframe['dummy'] = 0

    return build_columns(dataset, dframe, functions, name, no_index)


class Calculator(object):
    """Perform and store calculations and recalculations on update."""

    def __init__(self, dataset):
        self.dataset = dataset.reload()
        self.parser = Parser()

    @property
    def dependent_columns(self):
        return self.parser.dependent_columns

    def validate(self, formula, groups):
        """Validate `formula` and `groups` for calculator's dataset.

        Validate the formula and group string by attempting to get a row from
        the dframe for the dataset and then running parser validation on this
        row. Additionally, ensure that the groups in the group string are
        columns in the dataset.

        :param formula: The formula to validate.
        :param groups: A list of columns to group by.

        :returns: The aggregation (or None) for the formula.
        """
        aggregation = self.parser.validate_formula(formula, self.dataset)

        for group in groups:
            if not group in self.dataset.schema.keys():
                raise ParseError(
                    'Group %s not in dataset columns.' % group)

        return aggregation

    def calculate_columns(self, calculations):
        """Calculate and store new columns for `calculations`.

        The new columns are join t othe Calculation dframe and replace the
        dataset's observations.

        .. note::

            This can result in race-conditions when:

            - deleting ``controllers.Datasets.DELETE``
            - updating ``controllers.Datasets.POST([dataset_id])``

            Therefore, perform these actions asychronously.

        :param calculations: A list of calculations.
        """
        new_cols = None

        for c in calculations:

            if c.aggregation:
                aggregator = parse_aggregation(
                    self.dataset, self.parser, c.formula, c.name,
                    c.groups_as_list)
                aggregator.save()
            else:
                columns = parse_columns(
                    self.dataset, self.parser, c.formula, c.name)
                if new_cols is None:
                    new_cols = DataFrame(columns[0])
                else:
                    new_cols = new_cols.join(columns[0])

                self.dataset.update_observations(new_cols)

        # propagate calculation to any merged child datasets
        for merged_dataset in self.dataset.merged_datasets:
            merged_calculator = Calculator(merged_dataset)
            merged_calculator.propagate_column(self.dataset)

    @task(default_retry_delay=5, ignore_result=True)
    def calculate_updates(self, new_data, new_dframe_raw=None,
                          parent_dataset_id=None, update_id=None):
        """Update dataset with `new_data`.

        This can result in race-conditions when:

        - deleting ``controllers.Datasets.DELETE``
        - updating ``controllers.Datasets.POST([dataset_id])``

        Therefore, perform these actions asychronously.

        :param new_data: Data to update this dataset with.
        :param new_dframe_raw: DataFrame to update this dataset with.
        :param parent_dataset_id: If passed add ID as parent ID to column,
            default is None.
        """
        self.__ensure_ready(update_id)

        labels_to_slugs = self.dataset.schema.labels_to_slugs

        if new_dframe_raw is None:
            new_dframe_raw = self.dframe_from_update(new_data, labels_to_slugs)

        self.check_update_is_valid(new_dframe_raw)

        new_dframe = new_dframe_raw.recognize_dates_from_schema(
            self.dataset.schema)

        new_dframe = self.__add_calculations(new_dframe, labels_to_slugs)

        # set parent id if provided
        if parent_dataset_id:
            new_dframe = new_dframe.add_parent_column(parent_dataset_id)

        self.dataset.append_observations(new_dframe)
        self.dataset.clear_summary_stats()

        self.__update_aggregate_datasets(new_dframe)
        self.__update_merged_datasets(new_data, labels_to_slugs)
        self.__update_joined_datasets(new_dframe_raw)

        self.dataset.update_complete(update_id)

    def check_update_is_valid(self, new_dframe_raw):
        """Check if the update is valid.

        Check whether this is a right-hand side of any joins
        and deny the update if the update would produce an invalid
        join as a result.

        :raises: `NonUniqueJoinError` if update is illegal given joins of
            dataset.
        """
        for on in self.dataset.on_columns_for_rhs_of_joins:
            if on in new_dframe_raw.columns and on in self.dataset.columns:
                query_args = QueryArgs(select={on: 1})
                dframe = self.dataset.dframe(query_args=query_args)
                merged_join_column = concat([new_dframe_raw[on], dframe[on]])

                if len(merged_join_column) != merged_join_column.nunique():
                    raise NonUniqueJoinError(
                        'Cannot update. This is the right hand join and the'
                        'column "%s" will become non-unique.' % on)

    def dframe_from_update(self, new_data, labels_to_slugs):
        """Make a single-row dataframe for the additional data to add.

        :param new_data: Data to add to dframe.
        :type new_data: List.
        :param labels_to_slugs: Map of labels to slugs.
        """
        filtered_data = []
        columns = self.dataset.columns
        num_columns = len(columns)
        num_rows = self.dataset.num_rows
        dframe_empty = not num_columns

        if dframe_empty:
            columns = self.dataset.schema.keys()

        for row in new_data:
            filtered_row = dict()
            for col, val in row.iteritems():
                # special case for reserved keys (e.g. _id)
                if col == MONGO_ID:
                    if (not num_columns or col in columns) and\
                            col not in filtered_row.keys():
                        filtered_row[col] = val
                else:
                    # if col is a label take slug, if it's a slug take col
                    slug = labels_to_slugs.get(
                        col, col if col in labels_to_slugs.values() else None)

                    # if slug is valid or there is an empty dframe
                    if (slug or col in labels_to_slugs.keys()) and (
                            dframe_empty or slug in columns):
                        filtered_row[slug] = self.dataset.schema.convert_type(
                            slug, val)

            filtered_data.append(filtered_row)

        index = range(num_rows, num_rows + len(filtered_data))

        return BambooFrame(filtered_data, index=index)

    @task(default_retry_delay=5, ignore_result=True)
    def propagate(self):
        """Propagate changes in a modified dataset."""
        pass

    def propagate_column(self, parent_dataset):
        """Propagate columns in `parent_dataset` to this dataset.

        When a new calculation is added to a dataset this will propagate the
        new column to all child (merged) datasets.

        :param parent_dataset: The dataset to propagate to `self.dataset`.
        """
        # delete the rows in this dataset from the parent
        self.dataset.remove_parent_observations(parent_dataset.dataset_id)

        # get this dataset without the out-of-date parent rows
        dframe = self.dataset.dframe(keep_parent_ids=True)

        # create new dframe from the upated parent and add parent id
        parent_dframe = parent_dataset.dframe().add_parent_column(
            parent_dataset.dataset_id)

        # merge this new dframe with the existing dframe
        updated_dframe = concat([dframe, parent_dframe])

        # save new dframe (updates schema)
        self.dataset.replace_observations(updated_dframe)
        self.dataset.clear_summary_stats()

        # recur
        for merged_dataset in self.dataset.merged_datasets:
            merged_calculator = Calculator(merged_dataset)
            merged_calculator.propagate_column(self.dataset)

    def __add_calculations(self, new_dframe, labels_to_slugs):
        for calculation in self.dataset.calculations(include_aggs=False):
            function = self.parser.parse_formula(
                calculation.formula, self.dataset)
            new_column = new_dframe.apply(function[0], axis=1,
                                          args=(self.dataset, ))
            potential_name = calculation.name

            if potential_name not in self.dataset.dframe().columns:
                if potential_name in labels_to_slugs:
                    new_column.name = labels_to_slugs[potential_name]
            else:
                new_column.name = potential_name

            new_dframe = new_dframe.join(new_column)

        return new_dframe

    def __ensure_ready(self, update_id):
        # dataset must not be pending
        if not self.dataset.is_ready or (
                update_id and self.dataset.has_pending_updates(update_id)):
            self.dataset.reload()
            raise self.calculate_updates.retry()

    def __propagate_join(self, joined_dataset, merged_dframe):
        joined_calculator = Calculator(joined_dataset)
        joined_calculator.calculate_updates(
            joined_calculator, merged_dframe.to_jsondict(),
            parent_dataset_id=self.dataset.dataset_id)

    def __remapped_data(self, mapping, slugified_data):
        column_map = mapping.get(self.dataset.dataset_id) if mapping else None

        if column_map:
            slugified_data = [{column_map.get(k, k): v for k, v in row.items()}
                              for row in slugified_data]

        return slugified_data

    def __slugify_data(self, new_data, labels_to_slugs):
        slugified_data = []
        new_data = to_list(new_data)

        for row in new_data:
            for key, value in row.iteritems():
                if labels_to_slugs.get(key) and key != MONGO_ID:
                    del row[key]
                    row[labels_to_slugs[key]] = value

            slugified_data.append(row)

        return slugified_data

    def __update_aggregate_datasets(self, new_dframe):
        calcs_to_data = calculation_data(self.dataset)

        for formula, slug, groups, dataset in calcs_to_data:
            self.__update_aggregate_dataset(formula, new_dframe, slug, groups,
                                            dataset)

    def __update_aggregate_dataset(self, formula, new_dframe, name, groups,
                                   agg_dataset):
        """Update the aggregated dataset built for `self` with `calculation`.

        Proceed with the following steps:

            - delete the rows in this dataset from the parent
            - recalculate aggregated dataframe from aggregation
            - update aggregated dataset with new dataframe and add parent id
            - recur on all merged datasets descending from the aggregated
              dataset

        :param formula: The formula to execute.
        :param new_dframe: The DataFrame to aggregate on.
        :param name: The name of the aggregation.
        :param groups: A column or columns to group on.
        :type group: String, list of strings, or None.
        :param agg_dataset: The DataSet to store the aggregation in.
        """
        # parse aggregation and build column arguments
        aggregator = parse_aggregation(
            self.dataset, self.parser, formula, name, groups, new_dframe)
        new_agg_dframe = aggregator.update(agg_dataset, self, formula)

        # jsondict from new dframe
        new_data = new_agg_dframe.to_jsondict()

        for merged_dataset in agg_dataset.merged_datasets:
            # remove rows in child from this merged dataset
            merged_dataset.remove_parent_observations(
                agg_dataset.dataset_id)
            merged_calculator = Calculator(merged_dataset)

            # calculate updates for the child
            merged_calculator.calculate_updates(
                merged_calculator, new_data,
                parent_dataset_id=agg_dataset.dataset_id)

    def __update_joined_datasets(self, new_dframe_raw):
        # update any joined datasets
        for direction, other_dataset, on, joined_dataset in\
                self.dataset.joined_datasets:
            if direction == 'left':
                # only proceed if on in new dframe
                if on in new_dframe_raw.columns:
                    other_dframe = other_dataset.dframe(padded=True)

                    # only proceed if new on value is in on column in lhs
                    if len(set(new_dframe_raw[on]).intersection(
                            set(other_dframe[on]))):
                        merged_dframe = other_dframe.join_dataset(
                            self.dataset, on)
                        joined_dataset.replace_observations(merged_dframe)

                        # TODO is it OK not to propagate the join here?
                        #self.__propagate_join(joined_dataset, merged_dframe)
            else:
                merged_dframe = new_dframe_raw

                # if on in new data join with existing data
                if on in merged_dframe:
                    merged_dframe = new_dframe_raw.join_dataset(
                        other_dataset, on)

                self.__propagate_join(joined_dataset, merged_dframe)

    def __update_merged_datasets(self, new_data, labels_to_slugs):
        # store slugs as labels for child datasets
        slugified_data = self.__slugify_data(new_data, labels_to_slugs)

        # update the merged datasets with new_dframe
        for mapping, merged_dataset in self.dataset.merged_datasets_with_map:
            merged_calculator = Calculator(merged_dataset)
            slugified_data = self.__remapped_data(mapping, slugified_data)

            merged_calculator.calculate_updates(
                merged_calculator,
                slugified_data,
                parent_dataset_id=self.dataset.dataset_id)

    def __getstate__(self):
        """Get state for pickle."""
        return [self.dataset, self.parser]

    def __setstate__(self, state):
        self.dataset, self.parser = state
