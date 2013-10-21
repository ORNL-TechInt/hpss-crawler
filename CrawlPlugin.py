#!/usr/bin/env python
"""
CrawlPlugin.py - Plugin class for HPSS integrity crawler

  * Configuration data is read and copied into the object by method
    init_cfg_data(), called by both the constructor and reload().
    init_cfg_data() reversed the order of cfg and name in its argument list from
    the constructor so name can have a default and reload() doesn't have to pass
    it.

  * last_fired is initialized by the constructor but not by reload(). So if the
    plugin is updated by a reconfigure, it won't lose its last fire time but
    will stay on the same schedule.

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
    # -------------------------------------------------------------------------
    def __init__(self, name=None, cfg=None, logger=None):
        """
        Initialize this object.
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
        self.plugin_dir = cfg.get('crawler', 'plugin-dir')
        if self.plugin_dir not in sys.path:
            sys.path.append(self.plugin_dir)
        if self.name not in sys.modules.keys():
            __import__(self.name)
        else:
            filename = re.sub("\.pyc?", ".pyc",
                              sys.modules[self.name].__file__)
            if os.path.exists(filename):
                os.unlink(filename)
            reload(sys.modules[self.name])

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
def CrawlPlugin_setup():
    pass

# -----------------------------------------------------------------------------
def CrawlPlugin_teardown():
    if os.path.exists('test_plugins'):
        shutil.rmtree('test_plugins')

    if os.path.exists('test_plugins_alt'):
        shutil.rmtree('test_plugins_alt')

# -----------------------------------------------------------------------------
class CrawlPluginTest(testhelp.HelpedTestCase):
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
        If option 'fire' is False in config, should be False in plugin
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
        if option 'fire' is True in config, should be True in plugin
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
        If option 'fire' is not set in config, should be true in plugin
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
        if frequency is set in config, plugin should match
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
        if frequency not set in config, should be 3600 in plugin
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
        if plugin_dir set in config and in sys.path, sys.path should not change
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
        if plugin_dir set in config and not in sys.path, should be added to sys.path
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
        self.assertIn(self.plugdir, sys.path,
                      "sys.path should contain '%s' but does not" %
                      self.plugdir)

    # -------------------------------------------------------------------------
    def test_init_plugdir_unset(self):
        """
        if plugin_dir not set in config, should throw exception
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
        if plugin does exist and is in module list, should be reloaded
        """
        # set up dir, plugin name, create plugin
        if self.plugdir not in sys.path:
            sys.path.append(self.plugdir)
        pname = util.my_name()
        self.make_plugin(pname)

        # get it into the module list, remove the .pyc file
        __import__(pname)
        self.assertIn(pname, sys.modules.keys())

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
        c.set('crawler', 'plugin-dir', 'plugins')

        # initializing a plugin object should reload the plugin
        try:
            p = CrawlPlugin(pname, c)
            self.assertIn('added', dir(sys.modules[pname]))
        except ImportError:
            self.fail("Expected import to succeed but it did not.")
        except Exception, e:
            self.fail("Got unexpected exception: %s" % tb.format_exc())

    # -------------------------------------------------------------------------
    def test_init_plugin_ninmod(self):
        """
        if plugin does exists and not in module list, should be imported
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
        except Exception, e:
            self.fail("Got unexpected exception: %s" % tb.format_exc())

    # -------------------------------------------------------------------------
    def test_init_plugin_nosuch(self):
        """
        if plugin does not exist, should get ImportError
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
        except Exception, e:
            self.fail("Got unexpected exception: %s" % tb.format_exc())

    # -------------------------------------------------------------------------
    def test_reload_fire(self):
        """
        Update fire
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
        Update frequency
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
        Update plugdir

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
        self.assertRegexpMatches('%s/%s.pyc?' % (self.plugdir, pname),
                                 sys.modules[pname].__file__)
        
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
        self.assertRegexpMatches('%s/%s.pyc?' % (apdir, pname),
                                 sys.modules[pname].__file__)
        
        # raise testhelp.UnderConstructionError()
    
    # -------------------------------------------------------------------------
    def test_time_to_fire_false(self):
        """
        time_to_fire() should return False if time.time() - last_fired <= freq
        """
        raise testhelp.UnderConstructionError()

    # -------------------------------------------------------------------------
    def test_time_to_fire_true(self):
        """
        time_to_fire() should return True if freq < time.time() - last_fired
        """
        raise testhelp.UnderConstructionError()

    # -------------------------------------------------------------------------
    def make_plugin(self, pname, pdir=None):
        if None == pdir:
            pdir = self.plugdir
        if not os.path.isdir(pdir):
            os.mkdir(pdir)
        if not pname.endswith('.py'):
            fname = pname + '.py'
        else:
            fname = pname
        f = open('%s/%s' % (self.plugdir, fname), 'w')
        f.write("#!/bin/env python\n")
        f.write("def main(cfg):\n")
        f.write("    f = open('%s/%s', 'w')\n" % (pdir, pname))
        f.write("    f.write('my name is %s\\n')\n" % (pname))
        f.write("    f.close()\n")
        
        f.close()
        
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    toolframe.ez_launch(setup=CrawlPlugin_setup,
                        cleanup=CrawlPlugin_teardown,
                        test='CrawlPluginTest',
                        logfile='crawl_test.log')
