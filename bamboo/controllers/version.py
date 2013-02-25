from bamboo.controllers.abstract_controller import AbstractController
from bamboo.lib.version import get_version


class Version(AbstractController):

    def index(self):
        """Return JSON of version and version description"""
        return self._dump_or_error(get_version())
