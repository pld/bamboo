from celery.task import task

from bamboo.core.calculator import Calculator
from bamboo.core.frame import DATASET_ID
from bamboo.core.parser import Parser, ParserContext
from bamboo.lib.utils import call_async
from bamboo.models.abstract_model import AbstractModel


@task
def delete_task(calculation, dataset):
    dframe = dataset.dframe()
    slug = dataset.build_labels_to_slugs()[calculation.name]
    del dframe[slug]
    dataset.replace_observations(dframe)
    super(calculation.__class__, calculation).delete({
        DATASET_ID: calculation.dataset_id,
        calculation.NAME: calculation.name
    })


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
    def name(self):
        return self.record[self.NAME]

    @property
    def formula(self):
        return self.record[self.FORMULA]

    def delete(self, dataset):
        call_async(delete_task, self, dataset)

    def save(self, dataset, formula, name, group=None):
        """
        Attempt to parse formula, then save formula, and add a task to
        calculate formula.

        Raises a ParseError if an invalid formula is supplied.
        """
        calculator = Calculator(dataset)

        # ensure that the formula is parsable
        calculator.validate(formula, group)

        record = {
            DATASET_ID: dataset.dataset_id,
            self.FORMULA: formula,
            self.GROUP: group,
            self.NAME: name,
        }
        super(self.__class__, self).save(record)
        dataset.clear_summary_stats()

        # call async calculate
        call_async(calculator.calculate_column,
                   calculator, formula, name, group)
        return record

    @classmethod
    def create(cls, dataset, formula, name, group=None):
        calculation = cls()
        return calculation.save(dataset, formula, name, group)

    @classmethod
    def find_one(cls, dataset_id, name):
        return super(cls, cls).find_one({
            DATASET_ID: dataset_id,
            cls.NAME: name,
        })

    @classmethod
    def find(cls, dataset):
        return super(cls, cls).find({DATASET_ID: dataset.dataset_id})
