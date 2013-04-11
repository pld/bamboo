import traceback

from celery.task import Task, task

from bamboo.core.calculator import Calculator
from bamboo.core.frame import DATASET_ID
from bamboo.lib.async import call_async
from bamboo.lib.exceptions import ArgumentError
from bamboo.lib.query_args import QueryArgs
from bamboo.lib.schema_builder import make_unique
from bamboo.lib.utils import to_list
from bamboo.models.abstract_model import AbstractModel


class UniqueCalculationError(Exception):

    def __init__(self, name, current_names):
        current_names_str = '", "'.join(current_names)
        message = ('The calculation name "%s" is not unique for this dataset.'
                   ' Please choose a name that does not exists.  The current '
                   'names are: "%s"' % (name, current_names_str))

        Exception.__init__(self, message)


class DependencyError(Exception):
    pass


@task(ignore_result=True)
def delete_task(calculation, dataset):
    """Background task to delete `calculation` and columns in its dataset.

    :param calculation: Calculation to delete.
    :param dataset: Dataset for this calculation.
    """
    slug = dataset.schema.labels_to_slugs.get(calculation.name)

    if slug:
        dframe = dataset.dframe(keep_parent_ids=True)
        del dframe[slug]
        dataset.replace_observations(dframe, overwrite=True)
        dataset.clear_summary_stats(column=slug)

    calculation.remove_dependencies()

    super(calculation.__class__, calculation).delete({
        DATASET_ID: calculation.dataset_id,
        calculation.NAME: calculation.name
    })


class CalculateTask(Task):
    def after_return(self, status, retval, task_id, args, kwargs, einfo=None):
        if status == 'FAILURE':
            calculations = args[0]

            for calculation in calculations:
                calculation.failed(traceback.format_exc())


@task(base=CalculateTask, default_retry_delay=5, max_retries=10,
      ignore_result=True)
def calculate_task(calculations, dataset):
    """Background task to run a calculation.

    Set calculation to failed and raise if an exception occurs.

    :param calculation: Calculation to run.
    :param dataset: Dataset to run calculation on.
    """
    # block until other calculations for this dataset are finished
    calculations[0].restart_if_has_pending(dataset, calculations[1:])

    calculator = Calculator(dataset)
    calculator.calculate_columns(calculations)

    for calculation in calculations:
        calculation.add_dependencies(dataset, calculator.dependent_columns())

        if calculation.aggregation is not None:
            aggregated_id = dataset.aggregated_datasets_dict[calculation.group]
            calculation.set_aggregation_id(aggregated_id)

        calculation.ready()


class Calculation(AbstractModel):

    __collectionname__ = 'calculations'

    AGGREGATION = 'aggregation'
    AGGREGATION_ID = 'aggregation_id'
    DEPENDENCIES = 'dependencies'
    DEPENDENT_CALCULATIONS = 'dependent_calculations'
    FORMULA = 'formula'
    GROUP = 'group'
    NAME = 'name'

    @property
    def aggregation(self):
        return self.record[self.AGGREGATION]

    @property
    def aggregation_id(self):
        return self.record.get(self.AGGREGATION_ID)

    @property
    def dataset_id(self):
        return self.record[DATASET_ID]

    @property
    def formula(self):
        return self.record[self.FORMULA]

    @property
    def group(self):
        return self.record[self.GROUP]

    @property
    def groups_as_list(self):
        return self.split_groups(self.group)

    @property
    def name(self):
        return self.record[self.NAME]

    @property
    def dependencies(self):
        return self.record.get(self.DEPENDENCIES, [])

    @property
    def dependent_calculations(self):
        return self.record.get(self.DEPENDENT_CALCULATIONS, [])

    def add_dependency(self, name):
        self.__add_and_update_set(self.DEPENDENCIES, self.dependencies, name)

    def add_dependent_calculation(self, name):
        self.__add_and_update_set(self.DEPENDENT_CALCULATIONS,
                                  self.dependent_calculations, name)

    def remove_dependent_calculation(self, name):
        new_dependent_calcs = self.dependent_calculations
        new_dependent_calcs.remove(name)
        self.update({self.DEPENDENT_CALCULATIONS: new_dependent_calcs})

    def remove_dependencies(self):
        for name in self.dependencies:
            calculation = self.find_one(self.dataset_id, name)
            calculation.remove_dependent_calculation(self.name)

    def set_aggregation_id(self, _id):
        self.update({self.AGGREGATION_ID: _id})

    def delete(self, dataset):
        """Delete this calculation.

        First ensure that there are no other calculations which depend on this
        one. If not, start a background task to delete the calculation.

        :param dataset: Dataset for this calculation.

        :raises: `DependencyError` if dependent calculations exist.
        :raises: `ArgumentError` if group is not in DataSet or calculation does
            not exist for DataSet.
        """
        if len(self.dependent_calculations):
            raise DependencyError(
                'Cannot delete, the calculations %s depend on this calculation'
                % self.dependent_calculations)

        if not self.group is None:
            # it is an aggregate calculation
            dataset = dataset.aggregated_dataset(self.group)

            if not dataset:
                raise ArgumentError(
                    'Aggregation with group "%s" does not exist for '
                    'dataset' % self.group)

        call_async(delete_task, self, dataset)

    def save(self, dataset, formula, name, group_str=None):
        """Parse, save, and calculate a formula.

        Validate `formula` and `group_str` for the given `dataset`. If the
        formula and group are valid for the dataset, then save a new
        calculation for them under `name`. Finally, create a background task
        to compute the calculation.

        Calculations are initially saved in a **pending** state, after the
        calculation has finished processing it will be in a **ready** state.

        :param dataset: The DataSet to save.
        :param formula: The formula to save.
        :param name: The name of the formula.
        :param group_str: Columns to group on.
        :type group_str: String, list or strings, or None.

        :raises: `ParseError` if an invalid formula was supplied.
        """
        calculator = Calculator(dataset)

        # ensure that the formula is parsable
        groups = self.split_groups(group_str) if group_str else []
        aggregation = calculator.validate(formula, groups)

        if aggregation:
            # set group if aggregation and group unset
            group_str = group_str or ''

            # check that name is unique for aggregation
            aggregated_dataset = dataset.aggregated_dataset(groups)

            if aggregated_dataset:
                name = self.__check_name_and_make_unique(name,
                                                         aggregated_dataset)

        else:
            # set group if aggregation and group unset
            name = self.__check_name_and_make_unique(name, dataset)

        record = {
            DATASET_ID: dataset.dataset_id,
            self.AGGREGATION: aggregation,
            self.FORMULA: formula,
            self.GROUP: group_str,
            self.NAME: name,
            self.STATE: self.STATE_PENDING,
        }
        super(self.__class__, self).save(record)

        return self

    @classmethod
    def create(cls, dataset, formula, name, group=None):
        calculation = super(cls, cls).create(dataset, formula, name, group)
        call_async(calculate_task, [calculation], dataset.clear_cache())
        return calculation

    @classmethod
    def create_from_list_or_dict(cls, dataset, calculations):
        calculations = to_list(calculations)

        if not len(calculations) or not isinstance(calculations, list):
            raise ArgumentError('Improper format for JSON calculations.')

        parsed_calculations = []

        # Pull out args to check JSON format
        try:
            for c in calculations:
                groups = c.get("groups")

                if not isinstance(groups, list):
                    groups = [groups]

                for group in groups:
                    parsed_calculations.append([
                        c[cls.FORMULA],
                        c[cls.NAME], group])
        except KeyError as e:
            raise ArgumentError('Required key %s not found in JSON' % e)

        calculations = [cls().save(dataset, formula, name, group)
                        for formula, name, group in parsed_calculations]
        call_async(calculate_task, calculations, dataset.clear_cache())

    @classmethod
    def find_one(cls, dataset_id, name, group=None):
        query = {
            DATASET_ID: dataset_id,
            cls.NAME: name,
        }

        if group:
            query[cls.GROUP] = group

        return super(cls, cls).find_one(query)

    @classmethod
    def find(cls, dataset):
        query_args = QueryArgs(query={DATASET_ID: dataset.dataset_id},
                               order_by='name')
        return super(cls, cls).find(query_args)

    def restart_if_has_pending(self, dataset, current_calcs=[]):
        current_names = sorted([self.name] + [c.name for c in current_calcs])
        unfinished = [c for c in dataset.calculations() if c.is_pending]
        unfinished_names = [c.name for c in unfinished[:len(current_names)]]

        if len(unfinished) and current_names != sorted(unfinished_names):
            raise calculate_task.retry()

    def add_dependencies(self, dataset, dependent_columns):
        """Store calculation dependencies."""
        calculations = dataset.calculations()
        names_to_calcs = {calc.name: calc for calc in calculations}

        for column_name in dependent_columns:
            calc = names_to_calcs.get(column_name)
            if calc:
                self.add_dependency(calc.name)
                calc.add_dependent_calculation(self.name)

    def __check_name_and_make_unique(self, name, dataset):
        """Check that the name is valid and make unique if valid.

        :param name: The name to make unique.
        :param dataset: The dataset to make unique for.
        :raises: `UniqueCalculationError` if not unique.
        :returns: A unique name.
        """
        current_names = dataset.labels

        if name in current_names:
            raise UniqueCalculationError(name, current_names)

        return make_unique(name, dataset.schema.keys())

    def __add_and_update_set(self, link_key, existing, new):
        new_list = list(set(existing + [new]))

        if new_list != existing:
            self.update({link_key: new_list})
