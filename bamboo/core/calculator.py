from collections import defaultdict

from celery.task import task
from pandas import concat

from bamboo.core.aggregator import Aggregator
from bamboo.core.frame import BambooFrame, NonUniqueJoinError
from bamboo.core.parser import ParseError, Parser
from bamboo.lib.datetools import recognize_dates_from_schema
from bamboo.lib.mongo import MONGO_RESERVED_KEYS
from bamboo.lib.utils import call_async, split_groups


class Calculator(object):
    """Perform and store calculations and recalculations on update."""

    calcs_to_data = None
    dframe = None

    def __init__(self, dataset):
        self.dataset = dataset
        self.parser = Parser(dataset)

    def ensure_dframe(self):
        """Ensure *dframe* for the calculator's dataset is defined."""
        if not self.dframe:
            self.dframe = self.dataset.dframe()

    def validate(self, formula, group_str):
        """Validate *formula* and *group_str* for calculator's dataset.

        Validate the formula and group string by attempting to get a row from
        the dframe for the dataset and then running parser validation on this
        row. Additionally, ensure that the groups in the group string are
        columns in the dataset.

        Args:

        - formula: The formula to validate.
        - group_str: A string of a group or comma separated groups.

        Returns:
            The aggregation (or None) for the formula.
        """
        dframe = self.dataset.dframe(limit=1)
        row = dframe.irow(0) if len(dframe) else {}

        aggregation = self.parser.validate_formula(formula, row)

        if group_str:
            groups = split_groups(group_str)
            for group in groups:
                if not group in dframe.columns:
                    raise ParseError(
                        'Group %s not in dataset columns.' % group)
        return aggregation

    def calculate_column(self, formula, name, group_str=None):
        """Calculate a new column based on *formula* store as *name*.

        The new column is joined to *dframe* and stored in *self.dataset*.
        The *group_str* is only applicable to aggregations and groups for
        aggregations.

        This can result in race-conditions when:

        - deleting ``controllers.Datasets.DELETE``
        - updating ``controllers.Datasets.POST([dataset_id])``

        Therefore, perform these actions asychronously.

        Args:

        - formula: The formula parsed by *self.parser* and applied to
          *self.dframe*.
        - name: The name of the new column or aggregate column.
        - group_str: A string or columns to group on for aggregate
          calculations.

        """
        self.ensure_dframe()

        aggregation, new_columns = self.make_columns(formula, name)

        if aggregation:
            agg = Aggregator(self.dataset, self.dframe,
                             group_str, aggregation, name)
            agg.save(new_columns)
        else:
            self.dataset.replace_observations(self.dframe.join(new_columns[0]))

        # propagate calculation to any merged child datasets
        for merged_dataset in self.dataset.merged_datasets:
            merged_calculator = Calculator(merged_dataset)
            merged_calculator.propagate_column(self.dataset)

    def propagate_column(self, parent_dataset):
        """Propagate columns in *parent_dataset* to this dataset.

        This is used when there has been a new calculation added to
        a dataset and that new column needs to be propagated to all
        child (merged) datasets.

        Args:

        - parent_dataset: The dataset to propagate to *self.dataset*.

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

    @task
    def calculate_updates(self, new_data, parent_dataset_id=None):
        """Update dataset with *new_data*.

        This can result in race-conditions when:

        - deleting ``controllers.Datasets.DELETE``
        - updating ``controllers.Datasets.POST([dataset_id])``

        Therefore, perform these actions asychronously.

        Args:

        - new_data: Data to update this dataset with.
        - parent_dataset_id: If passed add ID as parent ID to column, default
          is None.

        """
        self.ensure_dframe()

        calculations = self.dataset.calculations()
        labels_to_slugs = self.dataset.build_labels_to_slugs()
        new_dframe_raw = self._dframe_from_update(new_data, labels_to_slugs)

        # check whether this is a right-hand side of any joins
        # and deny the update if the update would produce an invalid
        # join as a result
        if any([direction == 'left' for direction, _, on, __ in
                self.dataset.joined_datasets]):
            if on in new_dframe_raw.columns:
                merged_join_column = concat(
                    [new_dframe_raw[on], self.dframe[on]])
                if len(merged_join_column) != merged_join_column.nunique():
                    raise NonUniqueJoinError(
                        'Cannot update. This is the right hand join and the'
                        'column "%s" will become non-unique.' % on)

        new_dframe = recognize_dates_from_schema(
            self.dataset.schema, new_dframe_raw)

        aggregate_calculations = []

        for calculation in calculations:
            _, function =\
                self.parser.parse_formula(calculation.formula)
            new_column = new_dframe.apply(function[0], axis=1,
                                          args=(self.parser.context, ))
            potential_name = calculation.name
            if potential_name not in self.dframe.columns:
                if potential_name not in labels_to_slugs:
                    # it is an aggregate calculation, update after
                    aggregate_calculations.append(calculation)
                    continue
                else:
                    new_column.name = labels_to_slugs[potential_name]
            else:
                new_column.name = potential_name
            new_dframe = new_dframe.join(new_column)

        # set parent id if provided
        if parent_dataset_id:
            new_dframe = new_dframe.add_parent_column(parent_dataset_id)

        existing_dframe = self.dataset.dframe(keep_parent_ids=True)

        # merge the two
        updated_dframe = concat([existing_dframe, new_dframe])

        # update (overwrite) the dataset with the new merged version
        self.dframe = self.dataset.replace_observations(updated_dframe)
        self.dataset.clear_summary_stats()

        self._update_aggregate_datasets(aggregate_calculations, new_dframe)

        # store slugs as labels for child datasets
        slugified_data = []
        if not isinstance(new_data, list):
            new_data = [new_data]
        for row in new_data:
            for key, value in row.iteritems():
                if labels_to_slugs.get(key) and key not in MONGO_RESERVED_KEYS:
                    del row[key]
                    row[labels_to_slugs[key]] = value
            slugified_data.append(row)

        # update the merged datasets with new_dframe
        for merged_dataset in self.dataset.merged_datasets:
            merged_calculator = Calculator(merged_dataset)
            call_async(merged_calculator.calculate_updates, merged_calculator,
                       slugified_data, self.dataset.dataset_id)

        # update any joined datasets
        for direction, other_dataset, on, joined_dataset in\
                self.dataset.joined_datasets:
            if direction == 'left':
                merged_dframe = other_dataset.dframe().join_dataset(
                    self.dataset, on)
                joined_dataset.replace_observations(merged_dframe)
            else:
                merged_dframe = new_dframe_raw
                if on in merged_dframe:
                    merged_dframe = new_dframe_raw.join_dataset(
                        other_dataset, on)
                joined_calculator = Calculator(joined_dataset)
                call_async(joined_calculator.calculate_updates,
                           joined_calculator, merged_dframe.to_jsondict(),
                           self.dataset.dataset_id)

    def make_columns(self, formula, name, dframe=None):
        """Parse formula into function and variables."""
        if dframe is None:
            dframe = self.dframe

        aggregation, functions = self.parser.parse_formula(formula)

        new_columns = []
        for function in functions:
            new_column = dframe.apply(
                function, axis=1, args=(self.parser.context, ))
            new_column.name = name
            new_columns.append(new_column)

        return aggregation, new_columns

    def _dframe_from_update(self, new_data, labels_to_slugs):
        """Make a single-row dataframe for the additional data to add."""
        if not isinstance(new_data, list):
            new_data = [new_data]

        filtered_data = []
        for row in new_data:
            filtered_row = dict()
            for col, val in row.iteritems():
                if labels_to_slugs.get(col, None) in self.dframe.columns:
                    filtered_row[labels_to_slugs[col]] = val
                # special case for reserved keys (e.g. _id)
                if col in MONGO_RESERVED_KEYS and\
                    col in self.dframe.columns and\
                        col not in filtered_row.keys():
                    filtered_row[col] = val
            filtered_data.append(filtered_row)
        return BambooFrame(filtered_data)

    def _update_aggregate_datasets(self, calculations, new_dframe):
        if not self.calcs_to_data:
            self._create_calculations_to_groups_and_datasets(calculations)

        for formula, slug, group, dataset in self.calcs_to_data:
            self._update_aggregate_dataset(formula, new_dframe, slug, group,
                                           dataset)

    def _update_aggregate_dataset(self, formula, new_dframe, name, group,
                                  agg_dataset):
        """Update the aggregated dataset built for *self* with *calculation*.

        Proceed with the following steps:

            - delete the rows in this dataset from the parent
            - recalculate aggregated dataframe from aggregation
            - update aggregated dataset with new dataframe and add parent id
            - recur on all merged datasets descending from the aggregated
              dataset

        """
        aggregation, new_columns = self.make_columns(
            formula, name, new_dframe)

        agg = Aggregator(self.dataset, self.dframe,
                         group, aggregation, name)
        new_agg_dframe = agg.update(agg_dataset, self, formula, new_columns)

        # jsondict from new dframe
        new_data = new_agg_dframe.to_jsondict()

        for merged_dataset in agg_dataset.merged_datasets:
            # remove rows in child from this merged dataset
            merged_dataset.remove_parent_observations(
                agg_dataset.dataset_id)

            # calculate updates on the child
            merged_calculator = Calculator(merged_dataset)
            call_async(merged_calculator.calculate_updates, merged_calculator,
                       new_data, agg_dataset.dataset_id)

    def _create_calculations_to_groups_and_datasets(self, calculations):
        """Create list of groups and calculations."""
        self.calcs_to_data = defaultdict(list)
        names_to_formulas = dict([(calc.name, calc.formula) for calc in
                                  calculations])
        calculations = set([calc.name for calc in calculations])
        for group, dataset in self.dataset.aggregated_datasets.items():
            labels_to_slugs = dataset.build_labels_to_slugs()
            for calc in list(
                    set(labels_to_slugs.keys()).intersection(calculations)):
                self.calcs_to_data[calc].append(
                    (names_to_formulas[calc],
                     labels_to_slugs[calc], group, dataset))
        self.calcs_to_data = [
            item for sublist in self.calcs_to_data.values() for item in sublist
        ]

    def __getstate__(self):
        """Get state for pickle."""
        return [self.dataset, self.parser]

    def __setstate__(self, state):
        self.dataset, self.parser = state
