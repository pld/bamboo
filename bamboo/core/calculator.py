from collections import defaultdict

from celery.task import task
from pandas import concat, DataFrame, Series

from bamboo.core.aggregator import Aggregator
from bamboo.core.parser import ParseError, Parser
from bamboo.lib.constants import PARENT_DATASET_ID
from bamboo.lib.datetools import recognize_dates, recognize_dates_from_schema
from bamboo.lib.mongo import MONGO_RESERVED_KEYS
from bamboo.lib.utils import call_async, df_to_jsondict, split_groups


class Calculator(object):

    labels_to_slugs_and_groups = None

    def __init__(self, dataset):
        self.dataset = dataset
        self.dframe = dataset.dframe()
        self.parser = Parser(dataset)

    def validate(self, formula, group_str):
        # attempt to get a row from the dataframe
        try:
            row = self.dframe.irow(0)
        except IndexError, err:
            row = {}

        self.parser.validate_formula(formula, row)

        if group_str:
            groups = split_groups(group_str)
            for group in groups:
                if not group in self.dframe.columns:
                    raise ParseError(
                        'Group %s not in dataset columns.' % group)

    @task
    def calculate_column(self, formula, name, group_str=None):
        """
        Calculate a new column based on *formula* store as *name*.
        The *fomula* is parsed by *self.parser* and applied to *self.dframe*.
        The new column is joined to *dframe* and stored in *self.dataset*.
        The *group_str* is only applicable to aggregations and groups for
        aggregations.

        This can result in race-conditions when:

        - deleting ``controllers.Datasets.DELETE``
        - updating ``controllers.Datasets.POST([dataset_id])``

        Therefore, perform these actions asychronously.
        """

        aggregation, new_columns = self._make_columns(formula, name)

        if aggregation:
            agg = Aggregator(self.dataset, self.dframe, new_columns,
                             group_str, aggregation, name)
            agg.save_aggregation()
        else:
            self.dataset.replace_observations(self.dframe.join(new_columns[0]))

        # propagate calculation to any merged child datasets
        for merged_dataset in self.dataset.merged_datasets:
            merged_calculator = Calculator(merged_dataset)
            merged_calculator._propagate_column(self.dataset)

    def _propagate_column(self, parent_dataset):
        """
        This is called when there has been a new calculation added to
        a dataset and that new column needs to be propagated to all
        child (merged) datasets.
        """
        # delete the rows in this dataset from the parent
        self.dataset.remove_parent_observations(parent_dataset.dataset_id)
        # get this dataset without the out-of-date parent rows
        dframe = self.dataset.dframe()
        dframe = self._add_parent_ids(dframe)

        # create new dframe from the upated parent
        parent_dframe = parent_dataset.dframe()

        # add parent ids to parent dframe
        parent_dframe = self._add_parent_column(
            parent_dataset.dataset_id,
            parent_dframe)

        # merge this new dframe with the existing dframe
        updated_dframe = concat([dframe, parent_dframe])

        # save new dframe (updates schema)
        self.dataset.replace_observations(updated_dframe)
        self.dataset.clear_summary_stats()

        # recur
        for merged_dataset in self.dataset.merged_datasets:
            merged_calculator = Calculator(merged_dataset)
            merged_calculator._propagate_column(self.dataset)

    @task
    def calculate_updates(self, new_data):
        """
        Update dataset with *new_data*.

        This can result in race-conditions when:

        - deleting ``controllers.Datasets.DELETE``
        - updating ``controllers.Datasets.POST([dataset_id])``

        Therefore, perform these actions asychronously.
        """
        calculations = self.dataset.calculations()
        labels_to_slugs = self.dataset.build_labels_to_slugs()
        new_dframe = self._dframe_from_update(new_data, labels_to_slugs)

        aggregate_calculations = []

        for calculation in calculations:
            aggregation, function = \
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

        # get parent id from existing dataset
        parent_dataset_id = self.dataset.observations()[0].get(
            PARENT_DATASET_ID)
        if parent_dataset_id:
            new_dframe = self._add_parent_column(
                parent_dataset_id, new_dframe)

        existing_dframe = self._add_parent_ids(self.dframe)

        # merge the two
        updated_dframe = concat([existing_dframe, new_dframe])

        # update (overwrite) the dataset with the new merged version
        self.dframe = self.dataset.replace_observations(updated_dframe)
        self.dataset.clear_summary_stats()

        self._update_aggregate_datasets(aggregate_calculations)

        # update the merged datasets with new_dframe
        for merged_dataset in self.dataset.merged_datasets:
            merged_calculator = Calculator(merged_dataset)
            call_async(merged_calculator.calculate_updates,
                       merged_dataset, merged_calculator, new_data)

    def _add_parent_ids(self, existing_dframe):
        old_dframe = self.dataset.dframe(with_reserved_keys=True)
        if PARENT_DATASET_ID in old_dframe.columns.tolist():
            parent_column = old_dframe[PARENT_DATASET_ID]
            existing_dframe = existing_dframe.join(parent_column)
        return existing_dframe

    def _make_columns(self, formula, name):
        # parse formula into function and variables
        aggregation, functions = self.parser.parse_formula(formula)

        new_columns = []
        for function in functions:
            new_column = self.dframe.apply(
                function, axis=1, args=(self.parser.context, ))
            new_columns.append(new_column)

        new_columns[0].name = name

        return aggregation, new_columns

    def _dframe_from_update(self, new_data, labels_to_slugs):
        """
        Make a single-row dataframe for the additional data to add
        """
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
        return recognize_dates_from_schema(self.dataset,
                                           DataFrame(filtered_data))

    def _update_aggregate_datasets(self, aggregate_calculations):
        for calculation in aggregate_calculations:
            self._update_aggregate_dataset(calculation)

    def _update_aggregate_dataset(self, calculation):
        if not self.labels_to_slugs_and_groups:
            self._create_labels_to_slugs_and_groups()
        data = self.labels_to_slugs_and_groups.get(calculation.name)
        name, group, agg_dataset = data

        # recalculate aggregated dataframe from aggregation
        agg_dframe = agg_dataset.dframe()
        aggregation, new_columns = self._make_columns(
            calculation.formula, name)
        agg = Aggregator(agg_dataset, agg_dframe, new_columns,
                         group, aggregation, name)
        new_agg_dframe = agg.eval_dframe()

        # update aggregated dataset with new dataframe
        new_agg_dframe = agg_dataset.replace_observations(new_agg_dframe)

        # add parent id column to new dframe
        new_agg_dframe = self._add_parent_column(
            agg_dataset.dataset_id, new_agg_dframe)

        # jsondict from new dframe
        new_data = df_to_jsondict(new_agg_dframe)

        for merged_dataset in agg_dataset.merged_datasets:
            # remove rows in child from this merged dataset
            merged_dataset.remove_parent_observations(
                agg_dataset.dataset_id)

            # calculate updates on the child
            merged_calculator = Calculator(merged_dataset)
            call_async(merged_calculator.calculate_updates,
                       merged_dataset, merged_calculator, new_data)

    def _create_labels_to_slugs_and_groups(self):
        """
        Extract info from linked datasets
        """
        self.labels_to_slugs_and_groups = dict()
        for group, dataset in self.dataset.linked_datasets.items():
            for label, slug in dataset.build_labels_to_slugs().items():
                self.labels_to_slugs_and_groups[label] = (
                    slug, group, dataset)

    def _add_parent_column(self, parent_dataset_id, child_dframe):
        column = Series([parent_dataset_id] * len(child_dframe))
        column.name = PARENT_DATASET_ID
        return child_dframe.join(column)
