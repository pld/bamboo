from lib.constants import ALL, ERROR, MODE_SUMMARY, MODE_INFO, SUCCESS
from lib.exceptions import JSONError
from lib.mongo import mongo_to_json
from lib.io import create_dataset_from_url, create_dataset_from_csv
from lib.tasks.summarize import summarize
from lib.utils import dump_or_error
from models.dataset import Dataset
from models.observation import Observation


class Datasets(object):
    'Datasets controller'

    exposed = True

    def DELETE(self, dataset_id):
        """
        Delete observations (i.e. the dataset) with hash *dataset_id* from mongo
        """
        dataset = Dataset.find_one(dataset_id)
        result = None

        if dataset:
            Dataset.delete(dataset_id)
            Observation.delete(dataset)
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

        try:
            if dataset:
                if mode == MODE_INFO:
                    result = Dataset.schema(dataset)
                elif mode == MODE_SUMMARY:
                    result = summarize(dataset, query, select, group)
                else:
                    return mongo_to_json(Observation.find(dataset, query,
                                select))
        except JSONError, e:
            result = {ERROR: e.__str__()}

        return dump_or_error(result, 'id not found')

    def POST(self, url=None, csv_file=None):
        """
        If *url* is provided read data from URL *url*.
        If *csv_file* is provided read data from *csv_file*.
        Otherwise return an error message.
        """
        result = None

        if url:
            result = create_dataset_from_url(url)

        if csv_file:
            result = create_dataset_from_csv(csv_file)

        return dump_or_error(result, 'url or csv_file required')
