from hpssic import CrawlConfig
from hpssic import CrawlMail
from hpssic import fakesmtp
from hpssic import messages as MSG
import os
import pdb
import pytest
from hpssic import testhelp as th
from hpssic import util as U


# -----------------------------------------------------------------------------
class CrawlMailTest(th.HelpedTestCase):
    # -------------------------------------------------------------------------
    def setUp(self):
        """
        Clear the inbox before each test
        """
        fakesmtp.inbox = []

    # -------------------------------------------------------------------------
    def test_to_csv(self):
        """
        The *to* arg to CrawlMail.send() is a comma separated list of
        addresses. Should work.
        """
        sender = 'from@here.now'
        tolist = ['a@b.c', 'd@e.f']
        subject = 'Topic'
        body = 'Message body'
        CrawlMail.send(sender=sender,
                       to=','.join(tolist),
                       subj=subject,
                       msg=body)
        m = fakesmtp.inbox[0]
        self.expected(sender, m.from_address)
        self.expected(tolist, m.to_address)
        self.expected_in(subject, m.fullmessage)
        self.expected_in(body, m.fullmessage)

    # -------------------------------------------------------------------------
    def test_to_sectopt(self):
        """
        The *to* arg to CrawlMail.send() is a section.option ref into a config
        object. Should work.
        """
        exp = "jock@there.net,sally@elsewhere.org"
        cdict = {'crawler': {'notify-email': "somebody@somewhere.com"},
                 'alerts': {'recipients': exp},
                 }
        cfg = CrawlConfig.CrawlConfig.dictor(cdict)

        sender = 'from@here.now'
        to = 'alerts.recipients'
        subject = 'Topic'
        body = 'Message body'
        CrawlMail.send(sender=sender,
                       to=to,
                       subj=subject,
                       msg=body,
                       cfg=cfg)
        m = fakesmtp.inbox[0]
        self.expected(sender, m.from_address)
        self.expected(exp.split(','), m.to_address)
        self.expected_in(subject, m.fullmessage)
        self.expected_in(body, m.fullmessage)

    # -------------------------------------------------------------------------
    def test_to_notstr(self):
        """
        The *to* arg to CrawlMail.send() is not a string. Should throw an
        exception.
        """
        sender = 'from@here.now'
        subject = 'Topic'
        body = 'Message body'
        self.assertRaisesMsg(U.HpssicError,
                             MSG.invalid_recip_list,
                             CrawlMail.send,
                             sender=sender,
                             to=17,
                             subj=subject,
                             msg=body)
        self.expected(0, len(fakesmtp.inbox))
        # pytest.skip('construction')

    # -------------------------------------------------------------------------
    def test_to_empty(self):
        """
        The *to* arg to CrawlMail.send() is empty. Should throw an exception.
        """
        sender = 'from@here.now'
        tolist = []
        subject = 'Topic'
        body = 'Message body'
        self.assertRaisesMsg(U.HpssicError,
                             MSG.no_recip_list,
                             CrawlMail.send,
                             sender=sender,
                             to=','.join(tolist),
                             subj=subject,
                             msg=body)
        self.expected(0, len(fakesmtp.inbox))

    # -------------------------------------------------------------------------
    def test_to_none(self):
        """
        The *to* arg to CrawlMail.send() is None. Should throw an exception.
        """
        pytest.skip('construction')

    # -------------------------------------------------------------------------
    def test_to_unspec(self):
        """
        The *to* arg to CrawlMail.send() is unspecified. Should throw an
        exception.
        """
        pytest.skip('construction')

    # -------------------------------------------------------------------------
    def test_subj_something(self):
        """
        The *subj* arg to CrawlMail.send() is set. The generated message should
        have the correct subject.
        """
        pytest.skip('construction')

    # -------------------------------------------------------------------------
    def test_subj_empty(self):
        """
        The *subj* arg to CrawlMail.send() is empty. The generated message
        should have the default subject 'HPSS Integrity Crawler ALERT'
        """
        pytest.skip('construction')

    # -------------------------------------------------------------------------
    def test_subj_none(self):
        """
        The *subj* arg to CrawlMail.send() is None. The generated message
        should have the default subject 'HPSS Integrity Crawler ALERT'
        """
        pytest.skip('construction')

    # -------------------------------------------------------------------------
    def test_subj_unspec(self):
        """
        The *subj* arg to CrawlMail.send() is unspecified. The generated
        message should have the default subject 'HPSS Integrity Crawler ALERT'
        """
        pytest.skip('construction')

    # -------------------------------------------------------------------------
    def test_subj_notstr(self):
        """
        The *subj* arg to CrawlMail.send() is not a string. Should throw
        exception.
        """
        pytest.skip('construction')

    # -------------------------------------------------------------------------
    def test_msg_something(self):
        """
        The *msg* arg to CrawlMail.send() is set. The generated message should
        have the correct message body.
        """
        pytest.skip('construction')

    # -------------------------------------------------------------------------
    def test_msg_empty(self):
        """
        The *msg* arg to CrawlMail.send() is empty. Should throw exception.
        """
        pytest.skip('construction')

    # -------------------------------------------------------------------------
    def test_msg_none(self):
        """
        The *msg* arg to CrawlMail.send() is None. Should throw exception.
        """
        pytest.skip('construction')

    # -------------------------------------------------------------------------
    def test_msg_unspec(self):
        """
        The *msg* arg to CrawlMail.send() is unspecified. Show throw exception.
        """
        pytest.skip('construction')

    # -------------------------------------------------------------------------
    def test_msg_notstr(self):
        """
        The *msg* arg to CrawlMail.send() is not a string. Should throw
        exception.
        """
        pytest.skip('construction')

    # -------------------------------------------------------------------------
    def test_sender_something(self):
        """
        The *sender* arg to CrawlMail.send() is set. The generated message
        should have the correct sender.
        """
        pytest.skip('construction')

    # -------------------------------------------------------------------------
    def test_sender_cfg(self):
        """
        The *sender* arg to CrawlMail.send() is empty. Sender should be pulled
        from the configuration [rpt.sender]
        """
        pytest.skip('construction')

    # -------------------------------------------------------------------------
    def test_sender_empty(self):
        """
        The *sender* arg to CrawlMail.send() is empty. The generated message
        should have the default sender.
        """
        pytest.skip('construction')

    # -------------------------------------------------------------------------
    def test_sender_none(self):
        """
        The *sender* arg to CrawlMail.send() is None. The generated message
        should use the default sender.
        """
        pytest.skip('construction')

    # -------------------------------------------------------------------------
    def test_sender_unspec(self):
        """
        The *sender* arg to CrawlMail.send() is unspecified. The generated
        message should use the default sender.
        """
        pytest.skip('construction')

    # -------------------------------------------------------------------------
    def test_sender_notstr(self):
        """
        The *sender* arg to CrawlMail.send() is not a string. Should throw
        exception.
        """
        pytest.skip('construction')
