from collections import defaultdict

from celery.task import task
from pandas import concat, DataFrame

from bamboo.core.aggregator import Aggregator
from bamboo.core.frame import BambooFrame, NonUniqueJoinError
from bamboo.core.parser import ParseError, Parser
from bamboo.lib.mongo import MONGO_RESERVED_KEY_ID, MONGO_RESERVED_KEYS
from bamboo.lib.query_args import QueryArgs
from bamboo.lib.schema_builder import make_unique
from bamboo.lib.utils import combine_dicts, to_list


class Calculator(object):
    """Perform and store calculations and recalculations on update."""

    dframe = None

    def __init__(self, dataset):
        self.dataset = dataset.reload()
        self.parser = Parser(self.dataset)

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
        aggregation = self.parser.validate_formula(formula)

        for group in groups:
            if not group in self.parser.context.dataset.schema.keys():
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
                aggregator = self.parse_aggregation(
                    c.formula, c.name, c.groups_as_list)
                aggregator.save()
            else:
                columns = self.parse_columns(c.formula, c.name,
                                             length=self.dataset.num_columns)
                if new_cols is None:
                    new_cols = DataFrame(columns[0])
                else:
                    new_cols = new_cols.join(columns[0])

                self.dataset.update_observations(new_cols)

        # propagate calculation to any merged child datasets
        for merged_dataset in self.dataset.merged_datasets:
            merged_calculator = Calculator(merged_dataset)
            merged_calculator.propagate_column(self.dataset)

    def dependent_columns(self):
        return self.parser.context.dependent_columns

    def propagate_column(self, parent_dataset):
        """Propagate columns in `parent_dataset` to this dataset.

        This is used when there has been a new calculation added to
        a dataset and that new column needs to be propagated to all
        child (merged) datasets.

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

        self._check_update_is_valid(new_dframe_raw)

        new_dframe = new_dframe_raw.recognize_dates_from_schema(
            self.dataset.schema)

        new_dframe, aggregations = self.__add_calcs_and_find_aggregations(
            new_dframe, labels_to_slugs)

        # set parent id if provided
        if parent_dataset_id:
            new_dframe = new_dframe.add_parent_column(parent_dataset_id)

        # TODO use update to add rows
        # (don't read the whole thing then overwrite)
        existing_dframe = self.dataset.dframe(keep_parent_ids=True)

        # merge the two dframes
        updated_dframe = concat([existing_dframe, new_dframe])

        # update (overwrite) the dataset with the new merged dframe
        self.dataset.replace_observations(
            updated_dframe, set_num_columns=False)
        self.dataset.clear_summary_stats()

        self.__update_aggregate_datasets(aggregations, new_dframe)
        self.__update_merged_datasets(new_data, labels_to_slugs)
        self.__update_joined_datasets(new_dframe_raw)

        self.dataset.update_complete(update_id)

    def parse_aggregation(self, formula, name, groups, dframe=None):
        # TODO this should work with index eventually
        columns = self.parse_columns(formula, name, dframe, no_index=True)
        functions, dependent_columns = self.parser.parse_formula(formula)

        # get dframe with only necessary columns
        # or dummy column (needed for count aggregation)
        select = combine_dicts({group: 1 for group in groups},
                               {col: 1 for col in dependent_columns})
        if select:
            query_args = QueryArgs(select=select)
            dframe = self.dataset.dframe(query_args=query_args)
        else:
            query_args = QueryArgs(select={'_id': 1})
            dframe = self.dataset.dframe(query_args=query_args,
                                         keep_mongo_keys=True)

        return Aggregator(self.dataset, dframe, groups,
                          self.parser.aggregation, name, columns)

    def parse_columns(self, formula, name, dframe=None, length=None,
                      no_index=False):
        """Parse formula into function and variables."""
        functions, dependent_columns = self.parser.parse_formula(formula)

        # make select from dependent_columns
        if dframe is None:
            select = {col: 1 for col in dependent_columns} if\
                dependent_columns else {'_id': 1}

            dframe = self.dataset.dframe(
                query_args=QueryArgs(select=select),
                keep_mongo_keys=True).set_index(MONGO_RESERVED_KEY_ID)

            if not dependent_columns:
                # constant column, use dummy
                dframe['dummy'] = 0

        columns = []

        for function in functions:
            column = dframe.apply(function, axis=1,
                                  args=(self.parser.context,))
            column.name = make_unique(name, [c.name for c in columns])
            if no_index:
                column = column.reset_index(drop=True)
            columns.append(column)

        return columns

    def _check_update_is_valid(self, new_dframe_raw):
        """Check if the update is valid.

        Check whether this is a right-hand side of any joins
        and deny the update if the update would produce an invalid
        join as a result.

        :raises: `NonUniqueJoinError` if update is illegal given joins of
            dataset.
        """
        # TODO clean this up
        if any([direction == 'left' for direction, _, on, __ in
                self.dataset.joined_datasets]):
            # TODO do not call dframe in here
            if on in new_dframe_raw.columns and\
                    on in self.dataset.dframe().columns:
                merged_join_column = concat(
                    [new_dframe_raw[on], self.dataset.dframe()[on]])
                if len(merged_join_column) != merged_join_column.nunique():
                    raise NonUniqueJoinError(
                        'Cannot update. This is the right hand join and the'
                        'column "%s" will become non-unique.' % on)

    def __ensure_ready(self, update_id):
        # dataset must not be pending
        if not self.dataset.is_ready or (
                update_id and self.dataset.has_pending_updates(update_id)):
            self.dataset.reload()
            raise self.calculate_updates.retry()

    def __add_calcs_and_find_aggregations(self, new_dframe, labels_to_slugs):
        aggregations = []
        calculations = self.dataset.calculations()

        for calculation in calculations:
            if calculation.aggregation is not None:
                aggregations.append(calculation)
            else:
                function, dependent_columns = self.parser.parse_formula(
                    calculation.formula)
                new_column = new_dframe.apply(function[0], axis=1,
                                              args=(self.parser.context, ))
                potential_name = calculation.name

                if potential_name not in self.dataset.dframe().columns:
                    if potential_name in labels_to_slugs:
                        new_column.name = labels_to_slugs[potential_name]
                else:
                    new_column.name = potential_name

                new_dframe = new_dframe.join(new_column)

        return new_dframe, aggregations

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

    def __update_joined_datasets(self, new_dframe_raw):
        # update any joined datasets
        for direction, other_dataset, on, joined_dataset in\
                self.dataset.joined_datasets:
            if direction == 'left':
                if on in new_dframe_raw.columns:
                    # only proceed if on in new dframe
                    other_dframe = other_dataset.dframe(padded=True)

                    if len(set(new_dframe_raw[on]).intersection(
                            set(other_dframe[on]))):
                        # only proceed if new on value is in on column in lhs
                        merged_dframe = other_dframe.join_dataset(
                            self.dataset, on)
                        joined_dataset.replace_observations(merged_dframe)
            else:
                merged_dframe = new_dframe_raw

                if on in merged_dframe:
                    merged_dframe = new_dframe_raw.join_dataset(
                        other_dataset, on)

                joined_calculator = Calculator(joined_dataset)
                joined_calculator.calculate_updates(
                    joined_calculator, merged_dframe.to_jsondict(),
                    parent_dataset_id=self.dataset.dataset_id)

    def dframe_from_update(self, new_data, labels_to_slugs):
        """Make a single-row dataframe for the additional data to add.

        :param new_data: Data to add to dframe.
        :type new_data: List.
        :param labels_to_slugs: Map of labels to slugs.
        """
        #if not isinstance(new_data, list):
        #    new_data = [new_data]

        filtered_data = []
        # TODO don't call dframe to get columns
        dframe = self.dataset.dframe()
        columns = dframe.columns
        num_columns = len(columns)
        num_rows = len(dframe)
        dframe_empty = not num_columns

        if dframe_empty:
            columns = self.dataset.schema.keys()

        for row in new_data:
            filtered_row = dict()
            for col, val in row.iteritems():
                # special case for reserved keys (e.g. _id)
                if col in MONGO_RESERVED_KEYS:
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

    def __update_aggregate_datasets(self, calculations, new_dframe):
        calcs_to_data = self.__create_calculations_to_groups_and_datasets(
            calculations)

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
        aggregator = self.parse_aggregation(formula, name, groups, new_dframe)
        new_agg_dframe = aggregator.update(agg_dataset, self, formula)

        # jsondict from new dframe
        new_data = new_agg_dframe.to_jsondict()

        for merged_dataset in agg_dataset.merged_datasets:
            # remove rows in child from this merged dataset
            merged_dataset.remove_parent_observations(
                agg_dataset.dataset_id)

            # calculate updates on the child
            merged_calculator = Calculator(merged_dataset)
            merged_calculator.calculate_updates(
                merged_calculator, new_data,
                parent_dataset_id=agg_dataset.dataset_id)

    def __create_calculations_to_groups_and_datasets(self, calculations):
        """Create list of groups and calculations."""
        calcs_to_data = defaultdict(list)

        names_to_formulas = {
            calc.name: calc.formula for calc in calculations
        }
        calculations = set([calculation.name for calculation in calculations])

        for group, dataset in self.dataset.aggregated_datasets:
            labels_to_slugs = dataset.schema.labels_to_slugs
            calculations_for_dataset = list(set(
                labels_to_slugs.keys()).intersection(calculations))

            for calc in calculations_for_dataset:
                calcs_to_data[calc].append((
                    names_to_formulas[calc],
                    labels_to_slugs[calc],
                    group,
                    dataset
                ))

        return [
            item for sublist in calcs_to_data.values() for item in sublist
        ]

    def __slugify_data(self, new_data, labels_to_slugs):
        slugified_data = []
        new_data = to_list(new_data)

        for row in new_data:
            for key, value in row.iteritems():
                if labels_to_slugs.get(key) and key not in MONGO_RESERVED_KEYS:
                    del row[key]
                    row[labels_to_slugs[key]] = value

            slugified_data.append(row)

        return slugified_data

    def __remapped_data(self, mapping, slugified_data):
        column_map = mapping.get(self.dataset.dataset_id) if mapping else None

        if column_map:
            slugified_data = [{column_map.get(k, k): v for k, v in row.items()}
                              for row in slugified_data]

        return slugified_data

    def __getstate__(self):
        """Get state for pickle."""
        return [self.dataset, self.parser]

    def __setstate__(self, state):
        self.dataset, self.parser = state
