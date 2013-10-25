#!/usr/bin/env python

import Alert
import fakesmtp
import testhelp
import toolframe
import util

# -----------------------------------------------------------------------------
def setUpModule():
    pass

# -----------------------------------------------------------------------------
def tearDownModule():
    pass

# -----------------------------------------------------------------------------
class AlertTest(testhelp.HelpedTestCase):
    # -------------------------------------------------------------------------
    def test_init(self):
        """
        Get an Alert object and make sure it has the correct attributes
        """
        x = Alert.Alert('this is the message',
                        caller=util.my_name(),
                        dispatch=False)
        self.expected('this is the message', x.msg)
        self.expected(util.my_name(), x.caller)
        self.assertIn('dispatch', dir(x))

    # -------------------------------------------------------------------------
    def test_alert_log(self):
        raise testhelp.UnderConstructionError()
    
    # -------------------------------------------------------------------------
    def test_next(self):
        self.fail("Alert needs more tests")
        
if __name__ == '__main__':
    toolframe.ez_launch(test='AlertTest',
                        logfile='crawl_test.log')
