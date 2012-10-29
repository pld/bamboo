from celery.task import task

from bamboo.core.calculator import Calculator
from bamboo.core.frame import DATASET_ID
from bamboo.core.parser import Parser
from bamboo.lib.utils import call_async
from bamboo.models.abstract_model import AbstractModel


@task
def delete_task(calculation, dataset):
    """Background task to delete *calculation* and its columns in *dataset*.

    Args:
        calculation: Calculation to delete.
        dataset: Dataset to delete columns from calculation in.
    """
    if not calculation.group is None:
        # it is an aggregate calculation
        dataset = dataset.aggregated_datasets[calculation.group]

    dframe = dataset.dframe(keep_parent_ids=True)
    slug = dataset.build_labels_to_slugs()[calculation.name]
    del dframe[slug]
    dataset.replace_observations(dframe)
    super(calculation.__class__, calculation).delete({
        DATASET_ID: calculation.dataset_id,
        calculation.NAME: calculation.name
    })


@task
def calculate_task(calculation, dataset, calculator):
    """Background task to run a calculation.

    Args:
        calculation: Calculation to run.
        dataset: Dataset to run calculation on.
        calculator: Calculator model instantiated for this dataset.
    """
    dataset.clear_summary_stats()
    calculator.calculate_column(calculation.formula, calculation.name,
                                calculation.group)
    calculation.ready()


class Calculation(AbstractModel):

    __collectionname__ = 'calculations'
    parser = Parser()

    FORMULA = 'formula'
    GROUP = 'group'
    NAME = 'name'
    QUERY = 'query'

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

    def delete(self, dataset):
        call_async(delete_task, self, dataset)

    def save(self, dataset, formula, name, group=None):
        """Parse, save, and calculate a formula.

        Validate *formula* and *group* for the given *dataset*. If the formula
        and group are valid for the dataset, then save a new calculation for
        them under *name*. Finally, create a background task to compute the
        calculation.

        Calculations are initially saved in a **pending** state, after the
        calculation has finished processing it will be in a **ready** state.

        Raises:
            ParseError: An invalid formula was supplied.
        """
        calculator = Calculator(dataset)

        # ensure that the formula is parsable
        aggregation = calculator.validate(formula, group)

        # set group if aggregation and group unset
        if aggregation and not group:
            group = ''

        record = {
            DATASET_ID: dataset.dataset_id,
            self.FORMULA: formula,
            self.GROUP: group,
            self.NAME: name,
            self.STATE: self.STATE_PENDING,
        }
        super(self.__class__, self).save(record)

        call_async(calculate_task, self, dataset, calculator)
        return record

    @classmethod
    def create(cls, dataset, formula, name, group=None):
        calculation = cls()
        return calculation.save(dataset, formula, name, group)

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
        return super(cls, cls).find({DATASET_ID: dataset.dataset_id})
