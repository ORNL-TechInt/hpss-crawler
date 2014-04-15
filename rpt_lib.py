import email.mime.text
import pdb
import smtplib
import util

def sendmail(sender='',
             to='',
             subject='',
             body=''):

    pdb.set_trace()
    
    if to.strip() == '':
        raise StandardError("Can't send mail without at least one recipient")
    if '@' not in to:
        raise StandardError("'%s' does not look like an e-mail address" % to)
        
    if sender.strip() == '':
        raise StandardError("Can't send mail without a sender")

    if subject.strip() == '':
        subject = "<empty subject>"

    if body.strip() == '':
        body = "<empty body>"

    hostname = util.hostname()
    payload = email.mime.text.MIMEText(body)
    payload['Subject'] = subject
    payload['From'] = sender
    payload['To'] = to
    s = smtplib.SMTP('localhost')
    s.sendmail(sender, to, payload.as_string())
    s.quit()
    
    
