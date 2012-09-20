import json

import cherrypy

from controllers.abstract_controller import AbstractController
from lib.constants import ALL, ERROR, ID
from lib.exceptions import JSONError
from lib.mongo import mongo_to_json
from lib.io import create_dataset_from_url, create_dataset_from_csv
from lib.utils import dump_or_error
from models.calculation import Calculation
from models.dataset import Dataset
from models.observation import Observation


class Datasets(AbstractController):
    'Datasets controller'

    SELECT_ALL_FOR_SUMMARY = 'all'

    # modes for dataset GET
    MODE_INFO = 'info'
    MODE_RELATED = 'related'
    MODE_SUMMARY = 'summary'

    def DELETE(self, dataset_id):
        """
        Delete the dataset with hash *dataset_id* from mongo
        """
        dataset = Dataset.find_one(dataset_id)
        result = None

        if dataset.record:
            task = dataset.delete.delay(dataset)
            result = {self.SUCCESS: 'deleted dataset: %s' % dataset_id}
        return dump_or_error(result, 'id not found')

    def GET(self, dataset_id, mode=False, query=None, select=None,
            group=ALL):
        """
        Based on *mode* perform different operations on the dataset specified
        by *dataset_id*.

        - *info*: return the meta-data and schema of the dataset.
        - *related*: return the dataset_ids of linked datasets for the
        dataset.
        - *summary*: return summary statistics for the dataset.
          - The *select* argument is required, it can be 'all' or a MongoDB
            JSON query
          - If *group* is passed group the summary.
          - If *query* is passed restrict summary to rows matching query.
        - no mode passed: Return the raw data for the dataset.
          - Restrict to *query* and *select* if passed.

        Returns an error message if dataset_id does not exists, mode does not
        exist, or the JSON for query or select is improperly formatted.
        Otherwise, returns the result from above dependent on mode.
        """
        dataset = Dataset.find_one(dataset_id)
        result = None
        error = 'id not found'

        try:
            if dataset.record:
                if mode == self.MODE_INFO:
                    result = dataset.schema()
                elif mode == self.MODE_RELATED:
                    result = dataset.linked_datasets
                elif mode == self.MODE_SUMMARY:
                    # for summary require a select
                    if select is None:
                        error = 'no select'
                    else:
                        if select == self.SELECT_ALL_FOR_SUMMARY:
                            select = None
                        result = dataset.summarize(
                            dataset, query, select, group)
                elif mode is False:
                    return mongo_to_json(dataset.observations(query, select))
                else:
                    error = 'unsupported API call'
        except JSONError, e:
            error = e.__str__()

        return dump_or_error(result, error)

    def POST(self, dataset_id=None, url=None, csv_file=None):
        """
        If *url* is provided read data from URL *url*.
        If *csv_file* is provided read data from *csv_file*.
        If neither are provided return an error message.  Also return an error
        message if an improperly formatted value raises a ValueError, e.g. an
        improperly formatted CSV file.

        The follow words are reserved and will lead to unexpected behavior if
        used as column names:

            - sum
            - date
            - years
            - and
            - or
            - not
            - in
            - default

        """
        # if we have a dataset_id then try to update
        if dataset_id:
            dataset = Dataset.find_one(dataset_id)
            if dataset:
                Calculation.update(
                    dataset,
                    data=json.loads(cherrypy.request.body.read()))
                # return some success value
                return json.dumps({ID: dataset_id})
            else:
                return json.dumps({ERROR:
                                   'dataset for this id does not exist'})
        else:
            # no dataset_id, try to load from file handle
            result = None
            error = 'url or csv_file required'

            try:
                if url:
                    result = create_dataset_from_url(url)

                if csv_file:
                    result = create_dataset_from_csv(csv_file)
            except ValueError as e:
                error = e.__str__()

        return dump_or_error(result, error)
