import simplejson as json

from bamboo.controllers.abstract_controller import AbstractController


class Version(AbstractController):

    # versioning
    VERSION_NUMBER = '0.5.4'
    VERSION_DESCRIPTION = 'alpha'

    def index(self):
        """Return JSON of version and version description"""
        return json.dumps({
            'version': self.VERSION_NUMBER,
            'description': self.VERSION_DESCRIPTION
        })
