import json

import cherrypy

from lib.constants import ALL, ERROR, ID, MODE_RELATED,\
    MODE_SUMMARY, MODE_INFO, SCHEMA, SUCCESS
from lib.exceptions import JSONError
from lib.mongo import mongo_to_json
from lib.io import create_dataset_from_url, create_dataset_from_csv
from lib.tasks.summarize import summarize
from lib.utils import dump_or_error
from models.calculation import Calculation
from models.dataset import Dataset
from models.observation import Observation


class Datasets(object):
    'Datasets controller'

    exposed = True

    def DELETE(self, dataset_id):
        """
        Delete the dataset with hash *dataset_id* from mongo
        """
        dataset = Dataset.find_one(dataset_id)
        result = None

        if dataset.record:
            task = dataset.delete.delay(dataset)
            result = {SUCCESS: 'deleted dataset: %s' % dataset_id}
        return dump_or_error(result, 'id not found')

    def GET(self, dataset_id, mode=False, query='{}', select=None,
            group=ALL):
        """
        Return data set for hash *dataset_id*.
        Execute query *query* in mongo if passed.
        If summary is passed return summary statistics for data set.
        If group is passed group the summary, if summary is false group is
        ignored.
        """
        dataset = Dataset.find_one(dataset_id)
        result = None
        error = 'id not found'

        try:
            if dataset.record:
                if mode == MODE_INFO:
                    result = dataset.schema()
                elif mode == MODE_RELATED:
                    result = dataset.linked_datasets
                elif mode == MODE_SUMMARY:
                    result = summarize(dataset, query, select, group)
                elif mode is False:
                    return mongo_to_json(dataset.observations(query, select))
                else:
                    error = 'unsupported API call'
        except JSONError, e:
            result = {ERROR: e.__str__()}

        return dump_or_error(result, error)

    def POST(self, dataset_id=None, url=None, csv_file=None):
        """
        If *url* is provided read data from URL *url*.
        If *csv_file* is provided read data from *csv_file*.
        If neither are provided return an error message.  Also return an error
        message if an improperly formatted value raises a ValueError, e.g. an
        improperly formatted CSV file.
        """
        # if we have a dataset_id then try to update
        if dataset_id:
            dataset = Dataset.find_one(dataset_id)
            if dataset.record:
                Calculation.update(
                    dataset,
                    data=json.loads(cherrypy.request.body.read()))
                # return some success value
                return json.dumps({ID: dataset_id})
            else:
                return json.dumps({ERROR:
                                   'dataset for this id does not exist'})

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
