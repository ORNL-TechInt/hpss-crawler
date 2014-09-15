import CrawlConfig
import email.mime.text
import messages as MSG
import smtplib
import socket
import util


# -----------------------------------------------------------------------------
def send(to='', subj='', msg='', sender='', cfg=None):
    """
    Send e-mail as indicated
    """
    if type(to) != str:
        raise util.HpssicError(MSG.invalid_recip_list)

    # Prepare a message object based on *msg*
    if msg:
        payload = email.mime.text.MIMEText(msg)
    else:
        payload = email.mime.text.MIMEText("Empty message")

    # Set the recipient address(es) based on *to*
    default_recip = 'tbarron@ornl.gov'
    if to == '':
        if cfg is None:
            raise util.HpssicError(MSG.no_recip_list)
        else:
            (section, option) = ('crawler', 'notify-e-mail')
            addrs = cfg.get(section, option)
    elif ',' in to or '@' in to:
        addrs = to
    elif '.' in to:
        if cfg is None:
            addrs = default_recip
        else:
            (section, option) = to.split('.')
            addrs = cfg.get(section, option)

    addrlist = [x.strip() for x in addrs.split(',')]
    payload['To'] = addrs

    # Set the subject based on *subj*
    if subj:
        payload['Subject'] = subj
    else:
        payload['Subject'] = 'HPSS Integrity Crawler ALERT'

    # Set the from address
    default_sender = 'hpssic@%s' % util.hostname(long=True)
    if sender == '':
        if cfg is not None:
            sender = cfg.get_d('rpt', 'sender', default_sender)
        else:
            sender = default_sender
    payload['From'] = sender

    # Send the message
    s = smtplib.SMTP('localhost')
    s.sendmail(sender, addrlist, payload.as_string())
    s.quit()

    # Log it
    CrawlConfig.log("sent mail to %s", addrs)
