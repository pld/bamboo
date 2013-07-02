import smtplib


def __format_message(recipient, sender, subject, body):
    return ('To: %s\r\nFrom: %s\r\nSubject: %s\r\nContent-type:'
            'text/plain\r\n\r\n%s' % (recipient, sender, subject, body))


def send_mail(smtp_server, mailbox_name, mailbox_password, recipient, sender,
              subject, body):
    msg = __format_message(recipient, sender, subject, body)

    server = smtplib.SMTP(smtp_server, 587)
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login(mailbox_name, mailbox_password)
    server.sendmail(sender, recipient, msg)
    server.close()
