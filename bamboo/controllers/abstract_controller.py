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

    def dump_or_error(self, data, error_message, callback=False):
        if data is None:
            data = {self.ERROR: error_message}
        json = dump_mongo_json(data)
        return '%s(%s)' % (callback, json) if callback else json
