import smtplib

import cherrypy


ERROR_RESPONSE_BODY = """
    <html><body><p>Sorry, an error occured. We are working to resolve it.
    </p><p>For more help please email the <a
    href= 'https://groups.google.com/forum/#!forum/bamboo-dev'>bamboo-dev
    list</a></p></body></html>"""


def send_mail(smtp_server, mailbox_name, mailbox_password, recipient, sender,
              subject, body):
    server = smtplib.SMTP(smtp_server)
    server.login(mailbox_name, mailbox_password)

    msg = ('To: %s\r\nFrom: %s\r\nSubject: %s\r\nContent-type:'
           'text/plain\r\n\r\n%s' % (recipient, sender, subject, body))

    server.sendmail(sender, recipient, msg)
    server.quit()


def handle_error():
    cherrypy.response.status = 500
    cherrypy.response.body = [ERROR_RESPONSE_BODY]
    send_mail('imap.googlemail.com', 'bamboo.errors', 'test-password',
              'bamboo.errors@gmail.com',
              'bamboo-errors@googlegroups.com',
              '[ERROR] 500 Error in Bamboo',
              cherrypy._cperror.format_exc())


class Root(object):
    _cp_config = {'request.error_response': handle_error}

    def index(self):
        """Redirect to documentation index."""
        raise cherrypy.HTTPRedirect('/docs/index.html')
