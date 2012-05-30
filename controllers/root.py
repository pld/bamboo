from cherrypy import HTTPRedirect


class Root(object):
    pass
    exposed = True

    def GET(self):
        raise HTTPRedirect('/index.html')
