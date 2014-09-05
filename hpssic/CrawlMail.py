import CrawlConfig
import email.mime.text
import smtplib
import socket
import util


# -----------------------------------------------------------------------------
def send(to='', subj='', msg='', cfg=None):
    """
    Send e-mail as indicated
    """
    # Prepare a message object based on *msg*
    if msg:
        payload = email.mime.text.MIMEText(msg)
    else:
        payload = email.mime.text.MIMEText("Empty message")

    # Set the recipient address(es) based on *to*
    if cfg is None:
        cfg = CrawlConfig.get_config()

    if to:
        (section, option) = to.split('.')
    else:
        (section, option) = ('crawler', 'notify-e-mail')
    addrs = cfg.get(section, option)
    addrlist = [x.strip() for x in addrs.split(',')]
    payload['To'] = addrs

    # Set the subject based on *subj*
    if subj:
        payload['Subject'] = subj
    else:
        payload['Subject'] = 'HPSS Integrity Crawler ALERT'

    # Set the from address
    sender = 'HIC@%s' % util.hostname(long=True)
    payload['From'] = sender

    # Send the message
    s = smtplib.SMTP('localhost')
    s.sendmail(sender, addrlist, payload.as_string())
    s.quit()

    # Log it
    CrawlConfig.log("sent mail to %s", addrs)
