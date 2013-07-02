import cherrypy

from bamboo.lib.mail import send_mail


ERROR_RESPONSE_BODY = """
    <html><body><p>Sorry, an error occured. We are working to resolve it.
    </p><p>For more help please email the <a
    href= 'https://groups.google.com/forum/#!forum/bamboo-dev'>bamboo-dev
    list</a></p></body></html>"""


def handle_error():
    cherrypy.response.status = 500
    cherrypy.response.body = [ERROR_RESPONSE_BODY]
    send_mail('smtp.googlemail.com', 'bamboo.errors', 'test-password',
              'bamboo-errors@googlegroups.com',
              'bamboo.errors@gmail.com',
              '[ERROR] 500 Error in Bamboo',
              cherrypy._cperror.format_exc())


class Root(object):
    _cp_config = {'request.error_response': handle_error}

    def index(self):
        """Redirect to documentation index."""
        raise cherrypy.HTTPRedirect('/docs/index.html')
