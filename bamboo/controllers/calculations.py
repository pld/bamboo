import simplejson as json

from bamboo.controllers.abstract_controller import AbstractController
from bamboo.core.parser import ParseError
from bamboo.lib.exceptions import ArgumentError
from bamboo.models.calculation import Calculation, DependencyError


class Calculations(AbstractController):
    """The Calculations Controller provides access to calculations.

    Calculations are formulas and names that (for now) must be linked to a
    specific dataset via that dataset's ID. All actions in the Calculations
    Controller can optionally take a `callback` parameter.  If passed the
    returned result will be wrapped this the parameter value.  E.g., if
    ``callback=parseResults`` the returned value will be
    ``parseResults([some-JSON])``, where ``some-JSON`` is the function return
    value.
    """

    def delete(self, dataset_id, name, group=None):
        """Delete the calculation with `name` from the dataset.

        Delete the calculation with column `name` from the dataset specified by
        the hash `dataset_id` from mongo. If it is an aggregate calculation a
        `group` must also be passed to determine the correct aggregate
        calculation to delete. This will also remove the column `name` from the
        dataframe for the dataset or the aggregate dataset.

        :param dataset_id: The dataset ID for which to delete the calculation.
        :param name: The name of the calculation to delete.
        :param group: The group of the calculation to delete, if an
            aggregation.

        :returns: JSON with success if delete or an error string if the
            calculation could not be found.
        """
        def action(dataset):
            calculation = Calculation.find_one(dataset.dataset_id, name, group)
            if calculation:
                calculation.delete(dataset)
                message = 'deleted calculation: \'%s\' for dataset: %s' % (
                    name, dataset.dataset_id)
                return {self.SUCCESS: message}

        return self._safe_get_and_call(
            dataset_id, action, exceptions=(DependencyError,),
            error = 'name and dataset_id combination not found')

    def create(self, dataset_id, formula=None, name=None, json_file=None,
               group=None):
        """Add a calculation to a dataset with the given fomula, etc.

        Create a new calculation for `dataset_id` named `name` that calculates
        the `formula`.  Variables in formula can only refer to columns in the
        dataset.

        :param dataset_id: The dataset ID to add the calculation to.
        :param formula: The formula for the calculation which must match the
            parser language.
        :param name: The name to assign the new column for this formula.
        :param data: A dict or list of dicts mapping calculation names and
            formulas.
        :param group: A column to group by for aggregations, must be a
            dimension.

        :returns: A success string is the calculation is create. An error
            string if the dataset could not be found, the formula could not be
            parsed, or the group was invalid.
        """
        def action(dataset):
            if json_file:
                calculations = json.loads(json_file.file.read())
                Calculation.create_from_list_or_dict(dataset, calculations)
                success_message = ('created calculations from JSON for dataset'
                                   ':%s' % dataset_id)
            elif formula is None or name is None:
                raise ArgumentError('Must provide both formula and name argume'
                                    'nts, or json_file argument')
            else:
                Calculation.create(dataset, formula, name, group)
                success_message = 'created calculation: %s for dataset: %s' % (
                    name, dataset_id)

            return {self.SUCCESS: success_message}

        return self._safe_get_and_call(dataset_id, action,
                                       exceptions=(ParseError,),
                                       success_status_code=201)

    def show(self, dataset_id, callback=False):
        """Retrieve the calculations for `dataset_id`.

        :param dataset_id: The dataset to show calculations for.
        :param callback: A JSONP callback function string.

        :returns: A list of calculation records.  Each calculation record
            shows the calculations name, formula, group (if it exists), and
            state.
        """
        def action(dataset):
            result = Calculation.find(dataset)
            return [x.clean_record for x in result]

        return self._safe_get_and_call(dataset_id, action, callback=callback)
