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
    def test_to_sectopt_nocfg(self):
        """
        The *to* arg to CrawlMail.send() is a section.option ref into a config
        object. Should work.
        """
        exp = "tbarron@ornl.gov"
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
                       cfg=None)
        m = fakesmtp.inbox[0]
        self.expected(sender, m.from_address)
        self.expected([exp], m.to_address)
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

    # -------------------------------------------------------------------------
    def test_to_empty(self):
        """
        The *to* arg to CrawlMail.send() is empty. Should throw an exception.
        """
        sender = 'from@here.now'
        subject = 'Topic'
        body = 'Message body'
        self.assertRaisesMsg(U.HpssicError,
                             MSG.no_recip_list,
                             CrawlMail.send,
                             sender=sender,
                             to='',
                             subj=subject,
                             msg=body)
        self.expected(0, len(fakesmtp.inbox))

    # -------------------------------------------------------------------------
    def test_to_empty_cfg(self):
        """
        The *to* arg to CrawlMail.send() is empty. A config is provided. The to
        address list should be taken from the config.
        """
        exp = "somebody@somewhere.com,someoneelse@someplaceelse.org"
        cdict = {'crawler': {'notify-e-mail': exp},
                 'alerts': {'recipients':
                            "jock@there.net,sally@elsewhere.org"},
                 }
        cfg = CrawlConfig.CrawlConfig.dictor(cdict)

        sender = 'from@here.now'
        to = 'alerts.recipients'
        subject = 'Topic'
        body = 'Message body'
        CrawlMail.send(sender=sender,
                       to='',
                       subj=subject,
                       msg=body,
                       cfg=cfg)
        m = fakesmtp.inbox[0]
        self.expected(sender, m.from_address)
        self.expected(exp.split(','), m.to_address)
        self.expected_in(subject, m.fullmessage)
        self.expected_in(body, m.fullmessage)

    # -------------------------------------------------------------------------
    def test_to_none(self):
        """
        The *to* arg to CrawlMail.send() is None. Should throw an exception.
        """
        sender = 'from@here.now'
        subject = 'Topic'
        body = 'Message body'
        self.assertRaisesMsg(U.HpssicError,
                             MSG.invalid_recip_list,
                             CrawlMail.send,
                             sender=sender,
                             to=None,
                             subj=subject,
                             msg=body)
        self.expected(0, len(fakesmtp.inbox))

    # -------------------------------------------------------------------------
    def test_to_unspec(self):
        """
        The *to* arg to CrawlMail.send() is unspecified. Should throw an
        exception.
        """
        sender = 'from@here.now'
        subject = 'Topic'
        body = 'Message body'
        self.assertRaisesMsg(U.HpssicError,
                             MSG.no_recip_list,
                             CrawlMail.send,
                             sender=sender,
                             subj=subject,
                             msg=body)
        self.expected(0, len(fakesmtp.inbox))

    # -------------------------------------------------------------------------
    def test_subj_something(self):
        """
        The *subj* arg to CrawlMail.send() is set. The generated message should
        have the correct subject.
        """
        sender = 'from@here.now'
        tolist = ['a@b.c', 'd@e.f']
        subject = 'This is the topic we expect'
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
    def test_subj_empty(self):
        """
        The *subj* arg to CrawlMail.send() is empty. The generated message
        should have the default subject 'HPSS Integrity Crawler ALERT'
        """
        sender = 'from@here.now'
        tolist = ['a@b.c', 'd@e.f']
        subject = ''
        default_subj = 'HPSS Integrity Crawler ALERT'
        body = 'Message body'
        CrawlMail.send(sender=sender,
                       to=','.join(tolist),
                       subj=subject,
                       msg=body)
        m = fakesmtp.inbox[0]
        self.expected(sender, m.from_address)
        self.expected(tolist, m.to_address)
        self.expected_in(MSG.default_mail_subject, m.fullmessage)
        self.expected_in(body, m.fullmessage)
        # pytest.skip('construction')

    # -------------------------------------------------------------------------
    def test_subj_none(self):
        """
        The *subj* arg to CrawlMail.send() is None. The generated message
        should have the default subject 'HPSS Integrity Crawler ALERT'
        """
        sender = 'from@here.now'
        tolist = ['a@b.c', 'd@e.f']
        default_subj = 'HPSS Integrity Crawler ALERT'
        body = 'Message body'
        CrawlMail.send(sender=sender,
                       to=','.join(tolist),
                       subj=None,
                       msg=body)
        m = fakesmtp.inbox[0]
        self.expected(sender, m.from_address)
        self.expected(tolist, m.to_address)
        self.expected_in(MSG.default_mail_subject, m.fullmessage)
        self.expected_in(body, m.fullmessage)

    # -------------------------------------------------------------------------
    def test_subj_unspec(self):
        """
        The *subj* arg to CrawlMail.send() is unspecified. The generated
        message should have the default subject 'HPSS Integrity Crawler ALERT'
        """
        sender = 'from@here.now'
        tolist = ['a@b.c', 'd@e.f']
        default_subj = 'HPSS Integrity Crawler ALERT'
        body = 'Message body'
        CrawlMail.send(sender=sender,
                       to=','.join(tolist),
                       msg=body)
        m = fakesmtp.inbox[0]
        self.expected(sender, m.from_address)
        self.expected(tolist, m.to_address)
        self.expected_in(MSG.default_mail_subject, m.fullmessage)
        self.expected_in(body, m.fullmessage)

    # -------------------------------------------------------------------------
    def test_subj_notstr(self):
        """
        The *subj* arg to CrawlMail.send() is not a string. Should throw
        exception.
        """
        sender = 'from@here.now'
        tolist = ['a@b.c', 'd@e.f']
        subject = {}
        body = 'Message body'
        self.assertRaisesMsg(U.HpssicError,
                             MSG.invalid_subject_S % str(subject),
                             CrawlMail.send,
                             sender=sender,
                             to=','.join(tolist),
                             subj=subject,
                             msg=body)
        self.expected(0, len(fakesmtp.inbox))

    # -------------------------------------------------------------------------
    def test_msg_something(self):
        """
        The *msg* arg to CrawlMail.send() is set. The generated message should
        have the correct message body.
        """
        sender = 'from@here.now'
        tolist = ['a@b.c', 'd@e.f']
        subject = 'Topic'
        body = """
        This is a very elaborate message body containing very specific
        test information.
        """
        CrawlMail.send(sender=sender,
                       to=','.join(tolist),
                       subj=subject,
                       msg=body)
        m = fakesmtp.inbox[0]
        self.expected(sender, m.from_address)
        self.expected(tolist, m.to_address)
        self.expected_in(subject, m.fullmessage)
        self.expected_in(body, m.fullmessage)
        # pytest.skip('construction')

    # -------------------------------------------------------------------------
    def test_msg_empty(self):
        """
        The *msg* arg to CrawlMail.send() is empty. Send it anyway.
        """
        sender = 'from@here.now'
        tolist = ['a@b.c', 'd@e.f']
        subject = 'Topic'
        body = ''
        CrawlMail.send(sender=sender,
                       to=','.join(tolist),
                       subj=subject,
                       msg=body)
        m = fakesmtp.inbox[0]
        self.expected(sender, m.from_address)
        self.expected(tolist, m.to_address)
        self.expected_in(subject, m.fullmessage)
        self.expected_in(MSG.empty_message, m.fullmessage)

    # -------------------------------------------------------------------------
    def test_msg_none(self):
        """
        The *msg* arg to CrawlMail.send() is None. Should throw exception.
        """
        sender = 'from@here.now'
        subject = 'Topic'
        self.assertRaisesMsg(U.HpssicError,
                             MSG.invalid_msg_body,
                             CrawlMail.send,
                             sender=sender,
                             subj=subject,
                             msg=None)
        self.expected(0, len(fakesmtp.inbox))

    # -------------------------------------------------------------------------
    def test_msg_unspec(self):
        """
        The *msg* arg to CrawlMail.send() is unspecified. Show throw exception.
        """
        sender = 'from@here.now'
        tolist = ['a@b.c', 'd@e.f']
        subject = 'Topic'
        body = ''
        CrawlMail.send(sender=sender,
                       to=','.join(tolist),
                       subj=subject)
        m = fakesmtp.inbox[0]
        self.expected(sender, m.from_address)
        self.expected(tolist, m.to_address)
        self.expected_in(subject, m.fullmessage)
        self.expected_in(MSG.empty_message, m.fullmessage)

    # -------------------------------------------------------------------------
    def test_msg_notstr(self):
        """
        The *msg* arg to CrawlMail.send() is not a string. Should throw
        exception.
        """
        sender = 'from@here.now'
        subject = 'Topic'
        body = {}
        self.assertRaisesMsg(U.HpssicError,
                             MSG.invalid_msg_body,
                             CrawlMail.send,
                             to='someone@someplace.net',
                             sender=sender,
                             subj=subject,
                             msg={})
        self.expected(0, len(fakesmtp.inbox))

    # -------------------------------------------------------------------------
    def test_sender_something(self):
        """
        The *sender* arg to CrawlMail.send() is set. The generated message
        should have the correct sender.
        """
        sender = 'unique@here.now'
        tolist = ['a@b.c', 'd@e.f']
        subject = 'Topic'
        body = 'Not an empty message'
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
    def test_sender_invalid(self):
        """
        The *sender* arg to CrawlMail.send() is set but is not a valid e-mail
        address. Should throw exception.
        """
        sender = 'invalid sender'
        subject = 'Topic'
        body = "This is a valid message body"
        self.assertRaisesMsg(U.HpssicError,
                             MSG.invalid_sender_S % sender,
                             CrawlMail.send,
                             to='someone@someplace.net',
                             sender=sender,
                             subj=subject,
                             msg=body)
        self.expected(0, len(fakesmtp.inbox))

    # -------------------------------------------------------------------------
    def test_sender_cfg(self):
        """
        The *sender* arg to CrawlMail.send() is empty. Sender should be pulled
        from the configuration [crawler.from_address]
        """
        exp = "hpssic@not-default.net"
        cdict = {'crawler': {'notify-email': "somebody@somewhere.com",
                             'from_address': exp},
                 'alerts': {'recipients':
                            "jock@there.net,sally@elsewhere.org"},
                 }
        cfg = CrawlConfig.CrawlConfig.dictor(cdict)

        to = "jock@there.net,sally@elsewhere.org"
        subject = 'Topic'
        body = 'Message body'
        CrawlMail.send(sender='',
                       to=to,
                       subj=subject,
                       msg=body,
                       cfg=cfg)
        m = fakesmtp.inbox[0]
        self.expected(exp, m.from_address)
        self.expected(to.split(','), m.to_address)
        self.expected_in(subject, m.fullmessage)
        self.expected_in(body, m.fullmessage)

    # -------------------------------------------------------------------------
    def test_sender_empty(self):
        """
        The *sender* arg to CrawlMail.send() is empty. The generated message
        should have the default sender.
        """
        exp = 'hpssic@' + U.hostname(long=True)
        tolist = ['a@b.c', 'd@e.f']
        subject = 'This is the topic we expect'
        body = 'Message body'
        CrawlMail.send(sender='',
                       to=','.join(tolist),
                       subj=subject,
                       msg=body)
        m = fakesmtp.inbox[0]
        self.expected(exp, m.from_address)
        self.expected(tolist, m.to_address)
        self.expected_in(subject, m.fullmessage)
        self.expected_in(body, m.fullmessage)

    # -------------------------------------------------------------------------
    def test_sender_none(self):
        """
        The *sender* arg to CrawlMail.send() is None. The generated message
        should use the default sender.
        """
        exp = 'hpssic@' + U.hostname(long=True)
        tolist = ['a@b.c', 'd@e.f']
        subject = 'This is the topic we expect'
        body = 'Message body'
        CrawlMail.send(sender=None,
                       to=','.join(tolist),
                       subj=subject,
                       msg=body)
        m = fakesmtp.inbox[0]
        self.expected(exp, m.from_address)
        self.expected(tolist, m.to_address)
        self.expected_in(subject, m.fullmessage)
        self.expected_in(body, m.fullmessage)

    # -------------------------------------------------------------------------
    def test_sender_unspec(self):
        """
        The *sender* arg to CrawlMail.send() is unspecified. The generated
        message should use the default sender.
        """
        exp = 'hpssic@' + U.hostname(long=True)
        tolist = ['a@b.c', 'd@e.f']
        subject = 'This is the topic we expect'
        body = 'Message body'
        CrawlMail.send(to=','.join(tolist),
                       subj=subject,
                       msg=body)
        m = fakesmtp.inbox[0]
        self.expected(exp, m.from_address)
        self.expected(tolist, m.to_address)
        self.expected_in(subject, m.fullmessage)
        self.expected_in(body, m.fullmessage)

    # -------------------------------------------------------------------------
    def test_sender_notstr(self):
        """
        The *sender* arg to CrawlMail.send() is not a string. Should throw
        exception.
        """
        sender = 234.9234
        tolist = ['a@b.c', 'd@e.f']
        subject = 'Topic'
        body = 'Message body'
        self.assertRaisesMsg(U.HpssicError,
                             MSG.invalid_sender_S % str(sender),
                             CrawlMail.send,
                             sender=sender,
                             to=','.join(tolist),
                             subj=subject,
                             msg=body)
        self.expected(0, len(fakesmtp.inbox))
