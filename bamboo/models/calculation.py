from celery.task import task

from bamboo.core.calculator import Calculator
from bamboo.core.frame import DATASET_ID
from bamboo.lib.async import call_async
from bamboo.lib.exceptions import ArgumentError
from bamboo.lib.schema_builder import make_unique
from bamboo.models.abstract_model import AbstractModel


class DependencyError(Exception):
    pass


@task
def delete_task(calculation, dataset, slug):
    """Background task to delete `calculation` and columns in its dataset.

    Args:

    :param calculation: Calculation to delete.
    :param dataset: Dataset for this calculation.

    """
    dframe = dataset.dframe(keep_parent_ids=True)
    del dframe[slug]
    dataset.replace_observations(dframe, overwrite=True)
    calculation.remove_dependencies()

    super(calculation.__class__, calculation).delete({
        DATASET_ID: calculation.dataset_id,
        calculation.NAME: calculation.name
    })


@task(max_retries=10)
def calculate_task(calculation, dataset):
    """Background task to run a calculation.

    :param calculation: Calculation to run.
    :param dataset: Dataset to run calculation on.
    """
    # block until other calculations for this dataset are finished
    calculation.restart_if_has_pending(dataset)
    dataset.clear_summary_stats()

    calculator = Calculator(dataset)
    calculator.calculate_column(calculation.formula, calculation.name,
                                calculation.group)
    calculation.add_dependencies(dataset, calculator.dependent_columns())
    calculation.ready()


class Calculation(AbstractModel):

    __collectionname__ = 'calculations'

    AGGREGATION = 'aggregation'
    DEPENDENCIES = 'dependencies'
    DEPENDENT_CALCULATIONS = 'dependent_calculations'
    FORMULA = 'formula'
    GROUP = 'group'
    NAME = 'name'

    @property
    def aggregation(self):
        return self.record[self.AGGREGATION]

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
    def name(self):
        return self.record[self.NAME]

    @property
    def dependencies(self):
        return self.record.get(self.DEPENDENCIES, [])

    @property
    def dependent_calculations(self):
        return self.record.get(self.DEPENDENT_CALCULATIONS, [])

    def add_dependency(self, name):
        self._add_and_update_set(self.DEPENDENCIES, self.dependencies, name)

    def add_dependent_calculation(self, name):
        self._add_and_update_set(self.DEPENDENT_CALCULATIONS,
                                 self.dependent_calculations, name)

    def _add_and_update_set(self, link_key, existing, new):
        new_list = list(set(existing + [new]))

        if new_list != existing:
            self.update({link_key: new_list})

    def remove_dependent_calculation(self, name):
        new_dependent_calcs = self.dependent_calculations
        new_dependent_calcs.remove(name)
        self.update({self.DEPENDENT_CALCULATIONS: new_dependent_calcs})

    def remove_dependencies(self):
        for name in self.dependencies:
            calculation = self.find_one(self.dataset_id, name)
            calculation.remove_dependent_calculation(self.name)

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
            if not self.group in dataset.aggregated_datasets:
                raise ArgumentError(
                    'Aggregation with group "%s" does not exists yet for '
                    'dataset' % self.group)
            dataset = dataset.aggregated_datasets[self.group]

        slug = dataset.schema.labels_to_slugs.get(self.name)

        if slug is None:
            raise ArgumentError(
                'Calculation "%s" does not exists for dataset' % self.name)

        call_async(delete_task, self, dataset, slug)

    def save(self, dataset, formula, name, group=None):
        """Parse, save, and calculate a formula.

        Validate `formula` and `group` for the given `dataset`. If the formula
        and group are valid for the dataset, then save a new calculation for
        them under `name`. Finally, create a background task to compute the
        calculation.

        Calculations are initially saved in a **pending** state, after the
        calculation has finished processing it will be in a **ready** state.

        :param dataset: The DataSet to save.
        :param formula: The formula to save.
        :param name: The name of the formula.
        :param group: The group of the formula.
        :type group: String, list or strings, or None.

        :raises: `ParseError` if an invalid formula was supplied.
        """
        calculator = Calculator(dataset)

        # ensure that the formula is parsable
        aggregation = calculator.validate(formula, group)

        if aggregation:
            # set group if aggregation and group unset
            if not group:
                group = ''
        else:
            # ensure the name is unique
            name = make_unique(name, dataset.labels + dataset.schema.keys())

        record = {
            DATASET_ID: dataset.dataset_id,
            self.AGGREGATION: aggregation,
            self.FORMULA: formula,
            self.GROUP: group,
            self.NAME: name,
            self.STATE: self.STATE_PENDING,
        }
        super(self.__class__, self).save(record)

        call_async(calculate_task, self, dataset)

        return record

    @classmethod
    def create(cls, dataset, formula, name, group=None):
        calculation = cls()
        return calculation.save(dataset, formula, name, group)

    @classmethod
    def create_from_list_or_dict(cls, dataset, calculations):
        if isinstance(calculations, dict):
            calculations = [calculations]

        if not len(calculations) or not isinstance(calculations, list):
            raise ArgumentError(
                'Improper format for JSON calculations.')
        try:
            for calc in calculations:
                cls.create(dataset, calc[cls.FORMULA], calc[cls.NAME],
                           calc.get(cls.GROUP))
        except KeyError as e:
            raise ArgumentError('Required key %s not found in JSON' % e)

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
        return super(cls, cls).find({DATASET_ID: dataset.dataset_id},
                                    order_by='name')

    def restart_if_has_pending(self, dataset):
        unfinished_calcs = [
            calc for calc in dataset.calculations() if not calc.is_ready]

        if len(unfinished_calcs) and self.name != unfinished_calcs[0].name:
            raise calculate_task.retry(countdown=5)

    def add_dependencies(self, dataset, dependent_columns):
        """Store calculation dependencies."""
        calculations = dataset.calculations()
        names_to_calcs = {calc.name: calc for calc in calculations}

        for column_name in dependent_columns:
            calc = names_to_calcs.get(column_name)
            if calc:
                self.add_dependency(calc.name)
                calc.add_dependent_calculation(self.name)
