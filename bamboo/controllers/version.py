import simplejson as json

from bamboo.controllers.abstract_controller import AbstractController
from bamboo.version import get_version


class Version(AbstractController):

    def index(self):
        """Return JSON of version and version description"""
        return json.dumps(get_version())
