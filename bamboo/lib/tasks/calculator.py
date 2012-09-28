from collections import defaultdict

from celery.task import task
from pandas import concat, DataFrame, Series

from lib.aggregator import Aggregator
from lib.constants import MONGO_RESERVED_KEYS
from lib.exceptions import ParseError
from lib.parser import Parser
from lib.utils import recognize_dates, recognize_dates_from_schema,\
    split_groups
from models.observation import Observation
from models.dataset import Dataset


class Calculator(object):

    labels_to_slugs_and_groups = None

    def __init__(self, dataset):
        self.dataset = dataset
        self.dframe = Observation.find(dataset, as_df=True)
        self.parser = Parser(dataset.record)

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
            new_dframe = agg.new_dframe
        else:
            new_dframe = Observation.update(
                self.dframe.join(new_columns[0]), self.dataset)

        return new_dframe

    @task
    def calculate_updates(self, new_data, calculations, FORMULA):
        """
        Update dataset with *new_data*.

        This can result in race-conditions when:

        - deleting ``controllers.Datasets.DELETE``
        - updating ``controllers.Datasets.POST([dataset_id])``

        Therefore, perform these actions asychronously.
        """
        labels_to_slugs = self.dataset.build_labels_to_slugs()
        new_dframe = self._dframe_from_update(new_data, labels_to_slugs)

        for calculation in calculations:
            aggregation, function = \
                self.parser.parse_formula(calculation.record[FORMULA])
            new_column = new_dframe.apply(function[0], axis=1,
                                          args=(self.parser.context, ))
            potential_name = calculation.name
            if potential_name not in self.dframe.columns:
                if potential_name not in labels_to_slugs:
                    # it is a linked calculation
                    self._update_linked_dataset(potential_name)
                    continue
                else:
                    new_column.name = labels_to_slugs[potential_name]
            else:
                new_column.name = potential_name
            new_dframe = new_dframe.join(new_column)

        # merge the two
        updated_dframe = concat([self.dframe, new_dframe])

        # update (overwrite) the dataset with the new merged version
        updated_dframe = Observation.update(updated_dframe, self.dataset)

        self.dataset.clear_summary_stats()

        # update the merged datasets with new_dframe
        for dataset_id in self.dataset.merged_datasets:
            merged_dataset = Dataset.find_one(dataset_id)
            merged_calculator = Calculator(merged_dataset)
            print 'recurring on merge'
            merged_calculator.calculate_updates(
                merged_calculator, new_data, calculations, FORMULA)
            print 'done recurring'

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
        filtered_data = dict()
        for col, val in new_data.iteritems():
            if labels_to_slugs.get(col, None) in self.dframe.columns:
                filtered_data[labels_to_slugs[col]] = val
            # special case for reserved keys (e.g. _id)
            if col in MONGO_RESERVED_KEYS and\
                col in self.dframe.columns and\
                    col not in filtered_data.keys():
                filtered_data[col] = val
        return recognize_dates_from_schema(self.dataset,
                                           DataFrame([filtered_data]))

    def _update_linked_dataset(self, formula):
        if not self.labels_to_slugs_and_groups:
            self._create_labels_to_slugs_and_groups()
        data = self.labels_to_slugs_and_groups.get(formula)
        if not data:
            return
        print 'lsg %s' % self.labels_to_slugs_and_groups
        print 'formula: %s' % formula
        print 'linked_datasets: %s' % self.dataset.linked_datasets
        print 'columns: %s' % self.dframe.columns
        print 'num rows: %s' % len(self.dframe)
        slug, group, linked_dataset = data


        # remove all exisiting rows
        Observation.update(DataFrame(), linked_dataset)

        # recalculate
        linked_dframe = linked_dataset.observations(as_df=True)
        aggregation, new_columns = self._make_columns(formula, slug)
        agg = Aggregator(linked_dataset, linked_dframe, new_columns,
                         group, aggregation, slug)
        new_dframe = agg.eval_dframe()

        Observation.update(new_dframe, linked_dataset)

    def _create_labels_to_slugs_and_groups(self):
        """
        Extract info from linked datasets
        """
        self.labels_to_slugs_and_groups = dict()
        for group, dataset_id in self.dataset.linked_datasets.items():
            linked_dataset = Dataset.find_one(dataset_id)
            for label, slug in linked_dataset.build_labels_to_slugs().items():
                self.labels_to_slugs_and_groups[label] = (slug, group, linked_dataset)
#            linked_dataset.delete(linked_dataset)

#        # remove linked datasets
#        self.dataset.clear_linked_datasets()
