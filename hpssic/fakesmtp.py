"""
monkey-patch smtplib so we don't send actual emails

This technique was taken from this blog post:

http://www.psychicorigami.com/2007/09/20/monkey-patching-pythons-smtp-lib-for-
unit-testing/

"""
smtp = None
inbox = []


# -----------------------------------------------------------------------------
class Message(object):
    """
    These can be put in the inbox
    """
    # -------------------------------------------------------------------------
    def __init__(self, from_address, to_address, fullmessage):
        """
        Initialize the Message
        """
        self.from_address = from_address
        self.to_address = to_address
        self.fullmessage = fullmessage


# -----------------------------------------------------------------------------
class DummySMTP(object):
    """
    This is our mock SMTP class. Rather than actually sending e-mail, it will
    put a Message object into an in-memory 'inbox' for subsequent examination.
    """
    # -------------------------------------------------------------------------
    def __init__(self, address):
        """
        Set up the fake SMTP thing
        """
        self.address = address
        global smtp
        smtp = self

    # -------------------------------------------------------------------------
    def login(self, username, password):
        """
        Accept a username and password
        """
        self.username = username
        self.password = password

    # -------------------------------------------------------------------------
    def sendmail(self, from_address, to_address, fullmessage):
        """
        Instantiate a Message object and add it to the inbox
        """
        global inbox
        inbox.append(Message(from_address, to_address, fullmessage))
        return []

    # -------------------------------------------------------------------------
    def quit(self):
        """
        Mark the SMTP object as having been quit. Nothing is really disallowed,
        though.
        """
        self.has_quit = True

# -----------------------------------------------------------------------------
# this is the actual monkey patch (simply replacing one class with another)
import smtplib
smtplib.SMTP = DummySMTP
# Now any calls to smtplib.SMTP will actually get this DummySMTP class instead
