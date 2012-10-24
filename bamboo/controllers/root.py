from cherrypy import HTTPRedirect


class Root(object):

    def index(self):
        raise HTTPRedirect('/docs/index.html')
