import json

from controllers.abstract_controller import AbstractController


class Version(AbstractController):

    # versioning
    VERSION_NUMBER = '0.1'
    VERSION_DESCRIPTION = 'alpha'

    def GET(self):
        return json.dumps({
            'version': self.VERSION_NUMBER,
            'description': self.VERSION_DESCRIPTION
        })
