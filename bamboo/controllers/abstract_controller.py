import cherrypy

from bamboo.lib.mongo import dump_mongo_json


class ArgumentError(Exception):
    pass


class AbstractController(object):
    """
    Abstract controller class for web facing controllers.
    """
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

    def dump_or_error(self, data, error_message, callback=False):
        if data is None:
            data = {self.ERROR: error_message}
        json = dump_mongo_json(data)
        self._add_cors_headers()
        return '%s(%s)' % (callback, json) if callback else json
