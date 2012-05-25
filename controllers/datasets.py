import json

from lib.exceptions import JSONError
from lib.mongo import mongo_to_json
from lib.io import create_dataset_from_url, open_data_file
from lib.summary import summarize
from models.dataset import Dataset
from models.observation import Observation


class Datasets(object):
    'Datasets controller'

    exposed = True

    def DELETE(self, dataset_id):
        """
        Delete observations (i.e. the dataset) with hash 'dataset_id' from mongo
        """
        dataset = Dataset.find_one(dataset_id)
        if dataset:
            Dataset.delete(dataset_id)
            Observation.delete(dataset)
            return {'success': 'deleted dataset: %s' % dataset_id}
        return {'error': 'id not found'}

    def GET(self, dataset_id, summary=False, query='{}', select=None,
            group=None):
        """
        Return data set for hash 'dataset_id' in format 'format'.
        Execute query 'query' in mongo if passed.
        If summary is passed return summary statistics for data set.
        If group is passed group the summary, if summary is false group is
        ignored.
        """
        dataset = Dataset.find_one(dataset_id)
        try:
            if dataset:
                if summary:
                    return json.dumps(summarize(dataset, query, select, group))
                return mongo_to_json(Observation.find(dataset, query, select))
        except JSONError, e:
            return {'error': e.__str__()}

        return {'error': 'id not found'}

    def POST(self, url=None):
        """
        Read data from URL 'url'.
        If URL is not provided and data is provided, read posted data 'data'.
        """
        return json.dumps(create_dataset_from_url(url))
