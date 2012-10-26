from bamboo.lib.mongo import dump_mongo_json


class ArgumentError(Exception):
    pass


class AbstractController(object):
    """Abstract controller class for web facing controllers."""
    exposed = True

    # constants for Controllers
    ERROR = 'error'
    SUCCESS = 'success'

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
        return '%s(%s)' % (callback, json) if callback else json
