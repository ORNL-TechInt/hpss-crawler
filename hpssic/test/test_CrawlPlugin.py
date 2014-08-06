#!/usr/bin/env python
"""
Plugin class for HPSS integrity crawler

This module contains the CrawlPlugin and CrawlPluginTest classes.
"""
import copy
from hpssic import CrawlConfig
from hpssic import CrawlPlugin
import os
import pdb
import re
import sys
from hpssic import testhelp
import time
from hpssic import toolframe
import traceback as tb
from hpssic import util


M = sys.modules['__main__']
if 'py.test' in M.__file__:
    import pytest
    attr = pytest.mark.attr
else:
    from nose.plugins.attrib import attr


# -----------------------------------------------------------------------------
def setUpModule():
    """
    Set up for testing
    """
    testhelp.module_test_setup(CrawlPluginTest.testdir)


# -----------------------------------------------------------------------------
def tearDownModule():
    """
    Clean up after testing
    """
    testhelp.module_test_teardown(CrawlPluginTest.testdir)


# -----------------------------------------------------------------------------
class CrawlPluginTest(testhelp.HelpedTestCase):
    """
    This class contains the tests for the CrawlPlugin class.
    """
    testdir = testhelp.testdata(__name__)
    plugdir = '%s/test_plugins' % testdir

    # -------------------------------------------------------------------------
    def test_fire(self):
        """
        The plugin should fire and self.last_fired should be set
        """
        pname = util.my_name()
        self.make_plugin(pname)
        c = self.make_cfg(pname)
        p = CrawlPlugin.CrawlPlugin(pname, c)

        p.fire()
        filename = "%s/%s" % (self.plugdir, pname)
        self.assertEqual(os.path.exists(filename), True,
                         "File '%s' should exist but does not" % (filename))
        self.expected('my name is %s\n' % (pname), util.contents(filename))

    # -------------------------------------------------------------------------
    def test_init_fire_false(self):
        """
        If option 'fire' is False in config, the object's firable attribute
        should be False and attempting to fire the plugin should do nothing
        """
        pname = util.my_name()
        self.make_plugin(pname)
        c = self.make_cfg(pname, fire=False)
        p = CrawlPlugin.CrawlPlugin(pname, c)

        self.assertEqual(p.firable, False,
                         "p.firable should be False, is True")
        p.fire()
        filename = "%s/%s" % (self.plugdir, pname)
        self.assertEqual(os.path.exists(filename), False,
                         "'%s' should not exist, but does" % (filename))

    # -------------------------------------------------------------------------
    def test_init_fire_true(self):
        """
        If option 'fire' is True in config, the plugin's firable attribute
        should be True and attempting to fire the plugin should work
        """
        pname = util.my_name()
        self.make_plugin(pname)
        c = self.make_cfg(pname)
        p = CrawlPlugin.CrawlPlugin(pname, c)

        self.assertEqual(p.firable, True,
                         "p.firable should be True, is False")
        p.fire()
        filename = "%s/%s" % (self.plugdir, pname)
        self.assertEqual(os.path.exists(filename), True,
                         "'%s' should exist but does not" % (filename))

    # -------------------------------------------------------------------------
    def test_init_fire_unset(self):
        """
        If option 'fire' is not set in config, the firable attribute should be
        true in plugin object
        """
        pname = util.my_name()
        self.make_plugin(pname)
        c = self.make_cfg(pname, fire=None)
        p = CrawlPlugin.CrawlPlugin(pname, c)

        # make sure 'fire' was not set in the config
        try:
            x = c.get(pname, 'fire')
            self.fail("Expected NoOptionError but didn't get it")
        except CrawlConfig.NoOptionError:
            pass

        # if 'fire' is not set in the config, it should default to True in the
        # plugin
        self.assertEqual(p.firable, True,
                         "p.firable should be True but is not")

    # -------------------------------------------------------------------------
    def test_init_freq_set(self):
        """
        If frequency is set in config, plugin should match
        """
        pname = util.my_name()
        self.make_plugin(pname)
        c = self.make_cfg(pname, fire=False)
        p = CrawlPlugin.CrawlPlugin(pname, c)

        self.expected(False, p.firable)
        self.expected(19, p.frequency)

    # -------------------------------------------------------------------------
    def test_init_freq_unset(self):
        """
        If frequency not set in config, should be 3600 (1 hour) in plugin
        """
        pname = util.my_name()
        self.make_plugin(pname)
        c = self.make_cfg(pname, freq=None)
        p = CrawlPlugin.CrawlPlugin(pname, c)

        # make sure 'frequency' was not set in the config
        try:
            x = c.get(pname, 'frequency')
            self.fail("Expected NoOptionError but didn't get it")
        except CrawlConfig.NoOptionError:
            pass

        # if 'freq' is not set in the config, it should default to 3600 in the
        # plugin
        self.expected(3600, p.frequency)

    # -------------------------------------------------------------------------
    def test_init_plugdir_inspath(self):
        """
        If plugin_dir set in config and in sys.path, sys.path should not change
        """
        if self.plugdir not in sys.path:
            sys.path.append(self.plugdir)
        pre = copy.copy(sys.path)
        pname = util.my_name()
        self.make_plugin(pname)
        c = self.make_cfg(pname, fire=False)

        p = CrawlPlugin.CrawlPlugin(pname, c)
        self.expected(pre, sys.path)

    # -------------------------------------------------------------------------
    def test_init_plugdir_ninspath(self):
        """
        If plugin_dir set in config and not in sys.path, should be added to
        sys.path
        """
        if self.plugdir in sys.path:
            sys.path.remove(self.plugdir)
        pre = copy.copy(sys.path)
        pname = util.my_name()
        self.make_plugin(pname)
        c = self.make_cfg(pname, fire=False)
        p = CrawlPlugin.CrawlPlugin(pname, c)
        self.assertNotEqual(pre, sys.path,
                            "pre and sys.path should not be equal, but are")
        self.assertTrue(self.plugdir in sys.path,
                        "sys.path should contain '%s' but does not" %
                        self.plugdir)

    # -------------------------------------------------------------------------
    def test_init_plugdir_unset(self):
        """
        If plugin_dir not set in config, attempting to create a plugin object
        should throw an exception
        """
        if self.plugdir in sys.path:
            sys.path.remove(self.plugdir)
        pre = copy.copy(sys.path)
        pname = util.my_name()
        self.make_plugin(pname)
        c = self.make_cfg(pname, plugdir=None, fire=False, freq=None)
        try:
            p = CrawlPlugin.CrawlPlugin(pname, c)
            self.fail("Expected NoOptionError but didn't get it")
        except CrawlConfig.NoOptionError:
            pass

    # -------------------------------------------------------------------------
    def test_init_plugin_inmod(self):
        """
        If plugin does exist and is in module list and its config file changes,
        it should be reloaded.

        We stick a 'z' on the front of the method name when setting pname so
        that py.test won't think it's a test object and munge it in a way that
        prevents it being reloaded.
        """
        # set up dir, plugin name, create plugin
        if self.plugdir not in sys.path:
            sys.path.append(self.plugdir)
        pname = 'z' + util.my_name()
        self.make_plugin(pname)

        # get it into the module list, remove the .pyc file
        __import__(pname)
        self.assertTrue(pname in sys.modules.keys(),
                        "%s not found in %s" % (pname, sys.modules.keys()))

        # update the plugin so we can tell whether it gets reloaded
        f = open('%s/%s.py' % (self.plugdir, pname), 'a')
        f.write("\n")
        f.write("def added():\n")
        f.write("    pass\n")
        f.close()

        # set up the config
        c = self.make_cfg(pname, fire=None)

        # initializing a plugin object should reload the plugin
        try:
            p = CrawlPlugin.CrawlPlugin(pname, c)
            self.assertTrue('added' in dir(sys.modules[pname]),
                            "expected 'added' in %s" % dir(sys.modules[pname]))
        except ImportError:
            self.fail("Expected import to succeed but it did not.")

    # -------------------------------------------------------------------------
    def test_init_plugin_ninmod(self):
        """
        If plugin does exists and not in module list, should be imported
        """
        # set up dir, plugin name, create plugin
        if self.plugdir not in sys.path:
            sys.path.append(self.plugdir)
        pname = util.my_name()
        self.make_plugin(pname)

        # set up the config
        c = self.make_cfg(pname)

        # initializing a plugin object should import the plugin
        try:
            p = CrawlPlugin.CrawlPlugin(pname, c)
        except ImportError:
            self.fail("Expected import to succeed but it did not.")

    # -------------------------------------------------------------------------
    def test_init_plugin_nosuch(self):
        """
        If plugin does not exist, should get ImportError
        """
        # set up dir, plugin name, create plugin
        if self.plugdir not in sys.path:
            sys.path.append(self.plugdir)
        pname = util.my_name()

        # don't call make_plugin -- leave the following line commented
        # self.make_plugin(pname)

        # set up the config
        c = self.make_cfg(pname)

        # initializing a plugin object should import the plugin
        try:
            p = CrawlPlugin.CrawlPlugin(pname, c)
            self.fail("Expected import to fail but it did not.")
        except ImportError:
            pass

    # -------------------------------------------------------------------------
    def test_reload_fire(self):
        """
        Changing a plugin's configuration and reloading it should update the
        firable attribute.
        """
        # set up the plugin
        if self.plugdir not in sys.path:
            sys.path.append(self.plugdir)
        pre = sys.path
        pname = util.my_name()
        self.make_plugin(pname)

        # create the config
        c = self.make_cfg(pname, fire=False)

        # instantiate the plugin object
        p = CrawlPlugin.CrawlPlugin(pname, c)
        self.assertEqual(p.firable, False,
                         "Expected p.firable() to be false")

        # change the config
        c.set(pname, 'fire', 'true')

        # re-init the plugin object
        p.reload(c)
        self.assertEqual(p.firable, True,
                         "Expected p.firables() to be true")

    # -------------------------------------------------------------------------
    def test_reload_freq(self):
        """
        Changing a plugin's configuration and reloading it should update its
        frequency attribute.
        """
        # set up the plugin
        if self.plugdir not in sys.path:
            sys.path.append(self.plugdir)
        pre = sys.path
        pname = util.my_name()
        self.make_plugin(pname)

        # create the config
        c = self.make_cfg(pname, freq='72')

        # instantiate the plugin object
        p = CrawlPlugin.CrawlPlugin(pname, c)
        self.expected(72, p.frequency)

        # change the config
        c.set(pname, 'frequency', '19')

        # re-init the plugin object
        p.reload(c)
        self.expected(19, p.frequency)

    # -------------------------------------------------------------------------
    def test_reload_plugdir(self):
        """
        Updating plugdir in the configuration and reloading should update the
        object's plugdir attribute.

        If the plugdir changes, do we unload all the plugins currently loaded
        from the old dir? No, just the one being reloaded.
        """
        # set up the plugin
        if self.plugdir not in sys.path:
            sys.path.insert(0, self.plugdir)
        pre = sys.path
        pname = util.my_name()
        self.make_plugin(pname)

        # create the config
        c = self.make_cfg(pname)

        # instantiate the plugin object
        p = CrawlPlugin.CrawlPlugin(pname, c)
        self.expected(self.plugdir, p.plugin_dir)
        rgx = '%s/%s.pyc?' % (util.realpath(self.plugdir), pname)
        self.assertTrue(re.findall(rgx,
                                   util.realpath(sys.modules[pname].__file__)),
                        "expected \n    %s\nto match \n    %s\n" %
                        (rgx, sys.modules[pname].__file__))

        # alternate plugin in alternate directory
        apdir = self.plugdir + "_alt"
        if apdir not in sys.path:
            sys.path.insert(0, apdir)
        self.make_plugin(pname, pdir=apdir)

        # change the config
        c.set('crawler', 'plugin-dir', apdir)

        # re-init the plugin object
        p.reload(c)
        self.expected(apdir, p.plugin_dir)
        rgx = '%s/%s.pyc?' % (apdir, pname)
        self.assertTrue(re.findall(rgx, sys.modules[pname].__file__),
                        "expected '%s' to match sys.modules[%s].__file__" %
                        (rgx, pname))

    # -------------------------------------------------------------------------
    def test_time_to_fire_false(self):
        """
        If time.time() - last_fired <= freq, time_to_fire() should return False
        """
        # set up the plugin
        if self.plugdir not in sys.path:
            sys.path.insert(0, self.plugdir)
        pre = sys.path
        pname = util.my_name()
        self.make_plugin(pname)

        # create the config
        c = self.make_cfg(pname)

        # instantiate the plugin object
        p = CrawlPlugin.CrawlPlugin(pname, c)

        # time_to_fire() should return False
        p.last_fired = time.time()
        self.assertFalse(p.time_to_fire(),
                         "p.time_to_fire() returned True when it" +
                         " should have been False")

    # -------------------------------------------------------------------------
    def test_time_to_fire_true(self):
        """
        If freq < time.time() - last_fired, time_to_fire() should return True
        """
        # set up the plugin
        if self.plugdir not in sys.path:
            sys.path.insert(0, self.plugdir)
        pre = sys.path
        pname = util.my_name()
        self.make_plugin(pname)

        # create the config
        c = self.make_cfg(pname)

        # instantiate the plugin object
        p = CrawlPlugin.CrawlPlugin(pname, c)

        # time_to_fire() should return True
        p.last_fired = time.time() - int(p.frequency) - 1
        self.assertTrue(p.time_to_fire(),
                        "p.time_to_fire() returned False when it" +
                        " should have been True")

    # -------------------------------------------------------------------------
    def make_cfg(self, pname, plugdir='', fire=True, freq='19'):
        """
        pname: name of the plugin for this test
        plugdir: <value> => plugin directory to go in config
                 None => leave unset in the config
        fire: True => set to 'true' in the config
              False => set to 'false' in the config
              None => leave unset in the config
        freq: <value> => set 'frequency' to <value>
              None => leave unset in the config
        """
        rval = CrawlConfig.CrawlConfig()
        rval.add_section(pname)
        rval.add_section('crawler')
        rval.set('crawler', 'logpath', "%s/test.log" % self.testdir)
        if plugdir is not None:
            if plugdir == '':
                rval.set('crawler', 'plugin-dir', self.plugdir)
            else:
                rval.set('crawler', 'plugin-dir', plugdir)
        if freq is not None:
            rval.set(pname, 'frequency', freq)
        if fire is not None:
            if fire:
                rval.set(pname, 'fire', 'true')
            else:
                rval.set(pname, 'fire', 'false')
        rval.set(pname, 'module', pname)
        return rval

    # -------------------------------------------------------------------------
    def make_plugin(self, pname, pdir=None):
        """
        Create a plugin for testing
        """
        if None == pdir:
            pdir = self.plugdir
        if not os.path.isdir(pdir):
            os.mkdir(pdir)
        if not pname.endswith('.py'):
            fname = pname + '.py'
        else:
            fname = pname
        f = open('%s/%s' % (pdir, fname), 'w')
        f.write("#!/bin/env python\n")
        f.write("def main(cfg):\n")
        f.write("    f = open('%s/%s', 'w')\n" % (pdir, pname))
        f.write("    f.write('my name is %s\\n')\n" % (pname))
        f.write("    f.close()\n")
        f.close()
