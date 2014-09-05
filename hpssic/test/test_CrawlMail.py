from hpssic import fakesmtp
from nose.plugins.skip import SkipTest
from hpssic import testhelp as th


# -----------------------------------------------------------------------------
class CrawlMailTest(th.HelpedTestCase):
    # -------------------------------------------------------------------------
    def test_to_csv(self):
        """
        The *to* arg to CrawlMail.send() is a comma separated list of
        addresses. Should work.
        """
        raise SkipTest

    # -------------------------------------------------------------------------
    def test_to_sectopt(self):
        """
        The *to* arg to CrawlMail.send() is a section.option ref into a config
        object. Should work.
        """
        raise SkipTest

    # -------------------------------------------------------------------------
    def test_to_notstr(self):
        """
        The *to* arg to CrawlMail.send() is not a string. Should throw an
        exception.
        """
        raise SkipTest

    # -------------------------------------------------------------------------
    def test_to_empty(self):
        """
        The *to* arg to CrawlMail.send() is empty. Should throw an exception.
        """
        raise SkipTest

    # -------------------------------------------------------------------------
    def test_to_none(self):
        """
        The *to* arg to CrawlMail.send() is None. Should throw an exception.
        """
        raise SkipTest

    # -------------------------------------------------------------------------
    def test_to_unspec(self):
        """
        The *to* arg to CrawlMail.send() is unspecified. Should throw an
        exception.
        """
        raise SkipTest

    # -------------------------------------------------------------------------
    def test_subj_something(self):
        """
        The *subj* arg to CrawlMail.send() is set. The generated message should
        have the correct subject.
        """
        raise SkipTest

    # -------------------------------------------------------------------------
    def test_subj_empty(self):
        """
        The *subj* arg to CrawlMail.send() is empty. The generated message
        should have the default subject 'HPSS Integrity Crawler ALERT'
        """
        raise SkipTest

    # -------------------------------------------------------------------------
    def test_subj_none(self):
        """
        The *subj* arg to CrawlMail.send() is None. The generated message
        should have the default subject 'HPSS Integrity Crawler ALERT'
        """
        raise SkipTest

    # -------------------------------------------------------------------------
    def test_subj_unspec(self):
        """
        The *subj* arg to CrawlMail.send() is unspecified. The generated
        message should have the default subject 'HPSS Integrity Crawler ALERT'
        """
        raise SkipTest

    # -------------------------------------------------------------------------
    def test_subj_notstr(self):
        """
        The *subj* arg to CrawlMail.send() is not a string. Should throw
        exception.
        """
        raise SkipTest

    # -------------------------------------------------------------------------
    def test_msg_something(self):
        """
        The *msg* arg to CrawlMail.send() is set. The generated message should
        have the correct message body.
        """
        raise SkipTest

    # -------------------------------------------------------------------------
    def test_msg_empty(self):
        """
        The *msg* arg to CrawlMail.send() is empty. Should throw exception.
        """
        raise SkipTest

    # -------------------------------------------------------------------------
    def test_msg_none(self):
        """
        The *msg* arg to CrawlMail.send() is None. Should throw exception.
        """
        raise SkipTest

    # -------------------------------------------------------------------------
    def test_msg_unspec(self):
        """
        The *msg* arg to CrawlMail.send() is unspecified. Show throw exception.
        """
        raise SkipTest

    # -------------------------------------------------------------------------
    def test_msg_notstr(self):
        """
        The *msg* arg to CrawlMail.send() is not a string. Should throw
        exception.
        """
        raise SkipTest

    # -------------------------------------------------------------------------
    def test_sender_something(self):
        """
        The *sender* arg to CrawlMail.send() is set. The generated message
        should have the correct sender.
        """
        raise SkipTest

    # -------------------------------------------------------------------------
    def test_sender_cfg(self):
        """
        The *sender* arg to CrawlMail.send() is empty. Sender should be pulled
        from the configuration [rpt.sender]
        """
        raise SkipTest

    # -------------------------------------------------------------------------
    def test_sender_empty(self):
        """
        The *sender* arg to CrawlMail.send() is empty. The generated message
        should have the default sender.
        """
        raise SkipTest

    # -------------------------------------------------------------------------
    def test_sender_none(self):
        """
        The *sender* arg to CrawlMail.send() is None. The generated message
        should use the default sender.
        """
        raise SkipTest

    # -------------------------------------------------------------------------
    def test_sender_unspec(self):
        """
        The *sender* arg to CrawlMail.send() is unspecified. The generated
        message should use the default sender.
        """
        raise SkipTest

    # -------------------------------------------------------------------------
    def test_sender_notstr(self):
        """
        The *sender* arg to CrawlMail.send() is not a string. Should throw
        exception.
        """
        raise SkipTest
