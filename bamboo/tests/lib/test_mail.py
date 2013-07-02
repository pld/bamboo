from mock import patch

from bamboo.lib.mail import send_mail
from bamboo.tests.test_base import TestBase


class TestRoot(TestBase):

    @patch('smtplib.SMTP')
    def test_handle_error(self, send_mail):
        send_mail('server', 'mailbox', 'password', 'rec', 'sender', 'body')
