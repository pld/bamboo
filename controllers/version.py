import json

from lib.constants import VERSION_DESCRIPTION, VERSION_NUMBER


class Version(object):

    exposed = True

    def GET(self):
        return json.dumps({
            'version': VERSION_NUMBER,
            'description': VERSION_DESCRIPTION
        })
