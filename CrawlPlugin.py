#!/usr/bin/env python
"""
Plugin class for HPSS integrity crawler

This module contains the CrawlPlugin and CrawlPluginTest classes.
"""
import copy
import CrawlConfig
import os
import re
import shutil
import sys
import testhelp
import time
import toolframe
import traceback as tb
import unittest
import util

# -----------------------------------------------------------------------------
class CrawlPlugin(object):
    """
    An object of this class represents a crawler plugin which is to be run
    periodically. The plugin's attributes are defined in a configuration file
    and this class loads them from there.
    """
    # -------------------------------------------------------------------------
    def __init__(self, name=None, cfg=None, logger=None):
        """
        Configuration data is read and copied into the object by method
        init_cfg_data(), called by both the constructor and reload().
        init_cfg_data() reverses the order of cfg and name in its argument list
        from the constructor so name can have a default and reload() doesn't
        have to pass it.

        last_fired is initialized by the constructor but not by reload(). So if
        the plugin is updated by a reconfigure, it won't lose its last fire
        time but will stay on the same schedule.
        """
        assert(name != None)
        assert(cfg != None)
        self.cfg = cfg
        self.log = logger
        self.clog("%s: Initializing plugin data" % name)
        self.init_cfg_data(name, cfg)
        self.last_fired = time.time() - self.frequency - 1
        super(CrawlPlugin, self).__init__()

    # -------------------------------------------------------------------------
    def clog(self, *args):
        """
        Conditional log. Only try to log if the logger is defined
        """
        if self.log:
            self.log.info(*args)
            
    # -------------------------------------------------------------------------
    def fire(self):
        """
        Run the plugin.
        """
        if self.firable:
            self.clog("%s: firing" % self.name)
            sys.modules[self.name].main(self.cfg)
            self.last_fired = time.time()
        elif self.cfg.getboolean('crawler', 'verbose'):
            self.clog("%s: not firable" % self.name)
            self.last_fired = time.time()

    # -------------------------------------------------------------------------
    def init_cfg_data(self, name='', cfg=None):
        """
        Read data we care about from the configuration.
        """
        if name != '':
            self.name = name
        if cfg != None:
            self.cfg = cfg
        try:
            x = cfg.get(self.name, 'fire')
            if x.lower() in cfg._boolean_states.keys():
                self.firable = cfg._boolean_states[x]
            else:
                self.firable = False
        except CrawlConfig.NoOptionError:
            self.firable = True
        self.frequency = cfg.get_time(self.name, 'frequency', 3600)

        try:
            old_pdir = self.plugin_dir
        except AttributeError:
            old_pdir = None
        pdir = cfg.get('crawler', 'plugin-dir')
        if pdir not in sys.path:
            sys.path.insert(0, pdir)

        if self.name in sys.modules.keys():
            filename = re.sub("\.pyc?", ".pyc",
                              sys.modules[self.name].__file__)
            util.conditional_rm(filename)

            del sys.modules[self.name]
            
        __import__(self.name)

        self.plugin_dir = cfg.get('crawler', 'plugin-dir')

    # -------------------------------------------------------------------------
    def reload(self, cfg):
        """
        Re-initialize this object from the configuration.
        """
        self.init_cfg_data(cfg=cfg)
        
    # -------------------------------------------------------------------------
    def time_to_fire(self):
        """
        Return True or False, indicating whether it's time for this plugin to
        fire.
        """
        return(self.frequency < (time.time() - self.last_fired))

# -----------------------------------------------------------------------------
def setUpModule():
    """
    Set up for testing
    """
    testhelp.module_test_setup([CrawlPluginTest.plugdir,
                                CrawlPluginTest.plugdir + '_alt'])
    
# -----------------------------------------------------------------------------
def tearDownModule():
    """
    Clean up after testing
    """
    testhelp.module_test_teardown([CrawlPluginTest.plugdir,
                                   CrawlPluginTest.plugdir + '_alt'])

# -----------------------------------------------------------------------------
class CrawlPluginTest(testhelp.HelpedTestCase):
    """
    This class contains the tests for the CrawlPlugin class.
    """
    plugdir = 'test_plugins'
    
    # -------------------------------------------------------------------------
    def test_fire(self):
        """
        The plugin should fire and self.last_fired should be set
        """
        pname = util.my_name()
        self.make_plugin(pname)
        c = CrawlConfig.CrawlConfig()
        c.add_section(pname)
        c.add_section('crawler')
        c.set('crawler', 'plugin-dir', self.plugdir)
        c.set(pname, 'fire', 'true')
        p = CrawlPlugin(pname, c)
            
        p.fire()
        filename = "./%s/%s" % (self.plugdir, pname)
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
        c = CrawlConfig.CrawlConfig()
        c.add_section(pname)
        c.add_section('crawler')
        c.set(pname, 'frequency', '19')
        c.set('crawler', 'plugin-dir', self.plugdir)
        c.set(pname, 'fire', 'false')
        p = CrawlPlugin(pname, c)
            
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
        c = CrawlConfig.CrawlConfig()
        c.add_section(pname)
        c.add_section('crawler')
        c.set(pname, 'frequency', '19')
        c.set('crawler', 'plugin-dir', self.plugdir)
        c.set(pname, 'fire', 'true')
        p = CrawlPlugin(pname, c)
            
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
        c = CrawlConfig.CrawlConfig()
        c.add_section(pname)
        c.add_section('crawler')
        c.set(pname, 'frequency', '19')
        c.set('crawler', 'plugin-dir', self.plugdir)
        p = CrawlPlugin(pname, c)
            
        self.assertEqual(p.firable, True,
                         "p.firable should be True but is not")
        
    # -------------------------------------------------------------------------
    def test_init_freq_set(self):
        """
        If frequency is set in config, plugin should match
        """
        pname = util.my_name()
        self.make_plugin(pname)
        c = CrawlConfig.CrawlConfig()
        c.add_section(pname)
        c.add_section('crawler')
        c.set(pname, 'frequency', '19')
        c.set('crawler', 'plugin-dir', self.plugdir)
        c.set(pname, 'fire', 'false')
        p = CrawlPlugin(pname, c)

        self.expected(19, p.frequency)
        
    # -------------------------------------------------------------------------
    def test_init_freq_unset(self):
        """
        If frequency not set in config, should be 3600 (1 hour) in plugin
        """
        pname = util.my_name()
        self.make_plugin(pname)
        c = CrawlConfig.CrawlConfig()
        c.add_section(pname)
        c.add_section('crawler')
        c.set('crawler', 'plugin-dir', self.plugdir)
        c.set(pname, 'fire', 'false')
        p = CrawlPlugin(pname, c)
            
        self.expected(3600, p.frequency)

    # -------------------------------------------------------------------------
    def test_init_plugdir_inspath(self):
        """
        If plugin_dir set in config and in sys.path, sys.path should not change
        """
        if self.plugdir not in sys.path:
            sys.path.append(self.plugdir)
        pre = sys.path
        pname = util.my_name()
        self.make_plugin(pname)
        c = CrawlConfig.CrawlConfig()
        c.add_section(pname)
        c.add_section('crawler')
        c.set('crawler', 'plugin-dir', self.plugdir)
        c.set(pname, 'fire', 'false')
        p = CrawlPlugin(pname, c)
        self.expected(pre, sys.path)

    # -------------------------------------------------------------------------
    def test_init_plugdir_ninspath(self):
        """
        If plugin_dir set in config and not in sys.path, should be added to sys.path
        """
        if self.plugdir in sys.path:
            sys.path.remove(self.plugdir)
        pre = copy.copy(sys.path)
        pname = util.my_name()
        self.make_plugin(pname)
        c = CrawlConfig.CrawlConfig()
        c.add_section(pname)
        c.add_section('crawler')
        c.set('crawler', 'plugin-dir', self.plugdir)
        c.set(pname, 'fire', 'false')
        p = CrawlPlugin(pname, c)
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
        c = CrawlConfig.CrawlConfig()
        c.add_section(pname)
        c.add_section('crawler')
        c.set(pname, 'fire', 'false')
        try:
            p = CrawlPlugin(pname, c)
            self.fail("Expected exception but didn't get one")
        except CrawlConfig.NoOptionError:
            pass
        except Exception, e:
            self.fail("Got unexpected exception: %s" % tb.format_exc())

    # -------------------------------------------------------------------------
    def test_init_plugin_inmod(self):
        """
        If plugin does exist and is in module list and its config file changes,
        it should be reloaded
        """
        # set up dir, plugin name, create plugin
        if self.plugdir not in sys.path:
            sys.path.append(self.plugdir)
        pname = util.my_name()
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
        c = CrawlConfig.CrawlConfig()
        c.add_section(pname)
        c.add_section('crawler')
        c.set(pname, 'frequency', '19')
        c.set('crawler', 'plugin-dir', self.plugdir)

        # initializing a plugin object should reload the plugin
        try:
            p = CrawlPlugin(pname, c)
            self.assertTrue('added' in dir(sys.modules[pname]),
                            "expected 'added' in " +
                            "dir(sys.modules[%s]) not found" % (pname))
        except ImportError:
            self.fail("Expected import to succeed but it did not.")
        except Exception, e:
            self.fail("Got unexpected exception: %s" % tb.format_exc())

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
        c = CrawlConfig.CrawlConfig()
        c.add_section(pname)
        c.add_section('crawler')
        c.set(pname, 'frequency', '19')
        c.set('crawler', 'plugin-dir', 'plugins')

        # initializing a plugin object should import the plugin
        try:
            p = CrawlPlugin(pname, c)
        except ImportError:
            self.fail("Expected import to succeed but it did not.")
        # except Exception, e:
        #     self.fail("Got unexpected exception: %s" % tb.format_exc())

    # -------------------------------------------------------------------------
    def test_init_plugin_nosuch(self):
        """
        If plugin does not exist, should get ImportError
        """
        # set up dir, plugin name, create plugin
        if self.plugdir not in sys.path:
            sys.path.append(self.plugdir)
        pname = util.my_name()
        # self.make_plugin(pname)

        # set up the config
        c = CrawlConfig.CrawlConfig()
        c.add_section(pname)
        c.add_section('crawler')
        c.set(pname, 'frequency', '19')
        c.set('crawler', 'plugin-dir', 'plugins')

        # initializing a plugin object should import the plugin
        try:
            p = CrawlPlugin(pname, c)
            self.fail("Expected import to fail but it did not.")
        except ImportError:
            pass
        # except Exception, e:
        #     self.fail("Got unexpected exception: %s" % tb.format_exc())

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
        c = CrawlConfig.CrawlConfig()
        c.add_section(pname)
        c.add_section('crawler')
        c.set('crawler', 'plugin-dir', self.plugdir)
        c.set(pname, 'fire', 'false')

        # instantiate the plugin object
        p = CrawlPlugin(pname, c)
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
        c = CrawlConfig.CrawlConfig()
        c.add_section(pname)
        c.add_section('crawler')
        c.set('crawler', 'plugin-dir', self.plugdir)
        c.set(pname, 'frequency', '72')

        # instantiate the plugin object
        p = CrawlPlugin(pname, c)
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
        c = CrawlConfig.CrawlConfig()
        c.add_section(pname)
        c.add_section('crawler')
        c.set('crawler', 'plugin-dir', self.plugdir)
        c.set(pname, 'frequency', '72')

        # instantiate the plugin object
        p = CrawlPlugin(pname, c)
        self.expected(self.plugdir, p.plugin_dir)
        rgx = '%s/%s.pyc?' % (self.plugdir, pname)
        self.assertTrue(re.findall(rgx, 
                                   sys.modules[pname].__file__),
                        "expected '%s' to match '%s'" %
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
        c = CrawlConfig.CrawlConfig()
        c.add_section(pname)
        c.add_section('crawler')
        c.set('crawler', 'plugin-dir', self.plugdir)
        c.set(pname, 'frequency', '72')
        
        # instantiate the plugin object
        p = CrawlPlugin(pname, c)
        
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
        c = CrawlConfig.CrawlConfig()
        c.add_section(pname)
        c.add_section('crawler')
        c.set('crawler', 'plugin-dir', self.plugdir)
        c.set(pname, 'frequency', '72')

        # instantiate the plugin object
        p = CrawlPlugin(pname, c)

        # time_to_fire() should return True
        p.last_fired = time.time() - 73
        self.assertTrue(p.time_to_fire(),
                        "p.time_to_fire() returned False when it" +
                        " should have been True")

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
        
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    toolframe.ez_launch(test='CrawlPluginTest',
                        logfile='crawl_test.log')
