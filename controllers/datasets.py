import json

import cherrypy
from pandas import concat, DataFrame

from lib.constants import ALL, ERROR, ID, MODE_SUMMARY, MODE_INFO, \
     SCHEMA, SUCCESS
from lib.exceptions import JSONError
from lib.mongo import mongo_to_json
from lib.io import create_dataset_from_url, create_dataset_from_csv
from lib.parser import Parser
from lib.tasks.summarize import summarize
from lib.utils import dump_or_error, build_labels_to_slugs
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
        error = 'id not found'

        try:
            if dataset:
                if mode == MODE_INFO:
                    result = Dataset.schema(dataset)
                elif mode == MODE_SUMMARY:
                    result = summarize(dataset, query, select, group)
                elif mode == False:
                    return mongo_to_json(Observation.find(dataset, query,
                                                          select))
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
            # TODO: move this into it's own function
            dataset = Dataset.find_one(dataset_id)
            if dataset:
                # get the dataframe for this dataset
                existing_dframe = Observation.find(dataset, as_df=True)
                # make a dataframe for the additional data to add
                new_data = json.loads(cherrypy.request.body.read())
                filtered_data = [dict([(k, v) for k, v in new_data.iteritems()
                    if k in existing_dframe.columns])]
                new_dframe = DataFrame(filtered_data)
                # calculate columns (and update aggregated datasets?)
                calculations = Calculation.find(dataset)
                parser = Parser()
                labels_to_slugs = build_labels_to_slugs(dataset)
                for calculation in calculations:
                    aggregation, function = \
                        parser.parse_formula(calculation[Calculation.FORMULA])
                    new_column = new_dframe.apply(function, axis=1,
                        args=(parser, ))
                    potential_name = calculation[Calculation.NAME]
                    if potential_name not in existing_dframe.columns:
                        new_column.name = labels_to_slugs[potential_name]
                    else:
                        new_column.name = potential_name
                    new_dframe = new_dframe.join(new_column)
                # merge the two
                updated_dframe = concat([existing_dframe, new_dframe])
                # update (overwrite) the dataset with the new merged version
                updated_dframe = Observation.update(updated_dframe, dataset)
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
