import json

from bamboo.controllers.abstract_controller import AbstractController


class Version(AbstractController):

    # versioning
    VERSION_NUMBER = '0.4.2'
    VERSION_DESCRIPTION = 'alpha'

    def index(self):
        return json.dumps({
            'version': self.VERSION_NUMBER,
            'description': self.VERSION_DESCRIPTION
        })
