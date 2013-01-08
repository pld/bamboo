import cherrypy

from bamboo.lib.exceptions import ArgumentError
from bamboo.lib.jsontools import JSONError
from bamboo.lib.mongo import dump_mongo_json
from bamboo.models.dataset import Dataset


class AbstractController(object):
    """Abstract controller class for web facing controllers.

    Attributes:

    - ERROR: constant string for error messages.
    - SUCCESS: constant string for success messages.

    """

    exposed = True

    CSV = 'csv'
    JSON = 'json'

    ERROR = 'error'
    SUCCESS = 'success'

    DEFAULT_ERROR_MESSAGE = 'id not found'
    DEFAULT_SUCCESS_STATUS_CODE = 200
    ERROR_STATUS_CODE = 400

    def _add_cors_headers(self):
        cherrypy.response.headers['Access-Control-Allow-Origin'] = '*'
        cherrypy.response.headers['Access-Control-Allow-Methods'] =\
            'GET, POST, PUT, DELETE, OPTIONS'
        cherrypy.response.headers['Access-Control-Allow-Headers'] =\
            'Content-Type, Accept'

    def options(self, dataset_id=None):
        """Set Cross Origin Resource Sharing (CORS) headers.

        Set the CORS headers required for AJAX non-GET requests.

        :param dataset_id: Ignored argument so signature maps requests from
            clients.

        :returns: An empty string with the proper response headers for CORS.
        """
        self._add_cors_headers()
        cherrypy.response.headers['Content-Length'] = 0
        cherrypy.response.status = 204

        return ''

    def dump_or_error(self, obj, error_message, callback=False):
        """Dump JSON or return error message, potentially with callback.

        If `obj` is None `error_message` is returned and the HTTP status code
        is set to 400. Otherwise the HTTP status code is set to
        `success_status_code`. If `callback` exists, the returned string is
        wrapped in the callback for JSONP.

        :param obj: Data to dump as JSON using BSON encoder.
        :param error_message: Error message to return is object is None.
        :param callback: Callback string to wrap obj in for JSONP.

        :returns: A JSON string wrapped with callback if callback is not False.
        """
        if obj is None:
            obj = {self.ERROR: error_message}

        result = obj if isinstance(obj, str) else dump_mongo_json(obj)
        self._add_cors_headers()

        return '%s(%s)' % (callback, result) if callback else result

    def set_response_params(self, obj,
                            success_status_code=DEFAULT_SUCCESS_STATUS_CODE,
                            content_type=JSON):
        """Set response parameters.

        :param obj: The object to set the response for.
        :param content_type: The content type.
        :param success_status_code: The HTTP status code to return, default is
            DEFAULT_SUCCESS_STATUS_CODE.
        """
        cherrypy.response.headers['Content-Type'] = 'application/%s' % (
            content_type)
        cherrypy.response.status = success_status_code if obj is not None else\
            self.ERROR_STATUS_CODE

    def _safe_get_and_call(self, dataset_id, action, callback=None,
                           exceptions=(),
                           success_status_code=DEFAULT_SUCCESS_STATUS_CODE,
                           error=DEFAULT_ERROR_MESSAGE,
                           content_type=JSON):
        """Find dataset and call action with it and kwargs.

        Finds the dataset by `dataset_id` then calls function `action` and
        catches any passed in exceptions as well as a set of standard
        exceptions. Passes the result, error and callback to dump_or_error and
        returns the resulting string.

        :param dataset_id: The dataset ID to fetch.
        :param action: A function to call within a try block that takes a
            dataset any kwargs.
        :param callback: A JSONP callback that is passed through to
            dump_or_error.
        :param exceptions: A set of exceptions to additionally catch.
        :param success_status_code: The HTTP status code to return, default is
            DEFAULT_SUCCESS_STATUS_CODE.
        :param error: Default error string.
        :param kwargs: A set of keyword arguments that are passed to the
            action.

        :returns: A string that is the result of calling action or an error
            caught when calling action.
        """
        exceptions += (ArgumentError, JSONError, ValueError)

        dataset = Dataset.find_one(dataset_id) if dataset_id else None
        result = None

        try:
            if dataset is None or dataset.record:
                result = action(dataset)
        except exceptions as err:
            error = err.__str__()

        self.set_response_params(result, success_status_code, content_type)
        return self.dump_or_error(result, error, callback)
