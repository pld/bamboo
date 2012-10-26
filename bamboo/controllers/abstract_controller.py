import cherrypy

from bamboo.lib.mongo import dump_mongo_json


class ArgumentError(Exception):
    pass


class AbstractController(object):
    """Abstract controller class for web facing controllers."""
    exposed = True

    # constants for Controllers
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

        :param obj: data to dump as JSON using BSON encoder
        :type obj: dict, list, or string
        :param error_message: error message to return is object is None
        :type error_message: string
        :param callback: callback string to wrap obj in for JSONP
        :type callback: string
        """
        if obj is None:
            obj = {self.ERROR: error_message}
        json = dump_mongo_json(obj)
        self._add_cors_headers()
        return '%s(%s)' % (callback, json) if callback else json
