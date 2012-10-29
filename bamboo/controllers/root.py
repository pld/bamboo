from cherrypy import HTTPRedirect


class Root(object):

    def index(self):
        """Redirect to documentation index."""
        raise HTTPRedirect('/docs/index.html')
