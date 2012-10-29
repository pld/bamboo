import cherrypy

from bamboo.lib.jsontools import JSONError
from bamboo.lib.mongo import dump_mongo_json
from bamboo.models.dataset import Dataset


class ArgumentError(Exception):
    pass


class AbstractController(object):
    """Abstract controller class for web facing controllers.

    Attributes:
        ERROR: constant string for error messages.
        SUCCESS: constant string for success messages.
    """

    exposed = True

    ERROR = 'error'
    SUCCESS = 'success'

    def _add_cors_headers(self):
        cherrypy.response.headers['Access-Control-Allow-Origin'] = '*'
        cherrypy.response.headers['Access-Control-Allow-Methods'] =\
            'GET, POST, PUT, DELETE, OPTIONS'
        cherrypy.response.headers['Access-Control-Allow-Headers'] =\
            'Content-Type, Accept'

    def options(self, dataset_id=None):
        self._add_cors_headers()
        cherrypy.response.headers['Content-Length'] = 0
        cherrypy.response.status = 204
        return ''

    def dump_or_error(self, obj, error_message, callback=False):
        """Dump JSON or return error message, potentially with callback.

        If *obj* is None *error_message* is returned.  If *callback* exists,
        the returned string is wrapped in the callback for JSONP.

        Args:
            obj: data to dump as JSON using BSON encoder.
            error_message: error message to return is object is None.
            callback: callback string to wrap obj in for JSONP.

        Returns:
            A JSON string wrapped with callback if callback is not False.
        """
        if obj is None:
            obj = {self.ERROR: error_message}
        json = dump_mongo_json(obj)
        self._add_cors_headers()
        return '%s(%s)' % (callback, json) if callback else json

    def _safe_get_and_call(self, dataset_id, action, **kwargs):
        """Find dataset and call action with it and kwargs.

        Finds the dataset by *dataset_id* then calls function *action* and
        catches any passed in exceptions as well as a set of standard
        exceptions. Passes the result, error and callback to dump_or_error and
        returns the resulting string.

        Args:
            dataset_id: The dataset ID to fetch.
            action: A function to call within a try block that takes a dataset
                any kwargs.
            callback: A JSONP callback that is passed through to dump_or_error.
            exceptions: A set exceptions to additionally catch.
            kwargs: A set of keyword arguments that are passed to the action.

        Returns:
            A string that is the result of calling action or an error caught
            when calling action.
        """
        # kwargs that will not be passed through
        callback = kwargs.pop('callback', None)
        exceptions = (ArgumentError, JSONError, ValueError) +\
            kwargs.pop('exceptions', ())

        dataset = Dataset.find_one(dataset_id)
        error = 'id not found'
        result = None

        try:
            if dataset.record:
                result = action(dataset, **kwargs)
        except exceptions as e:
            error = e.__str__()

        return self.dump_or_error(result, error, callback)
