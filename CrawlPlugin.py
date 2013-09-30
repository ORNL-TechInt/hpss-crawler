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
import shutil
import sys
import testhelp
import time
import toolframe
import unittest
import util

# -----------------------------------------------------------------------------
class CrawlPlugin(object):
    # -------------------------------------------------------------------------
    def __init__(self, name, cfg):
        """
        Initialize this object.
        """
        self.cfg = cfg
        self.init_cfg_data(name)
        self.last_fired = time.time() - self.frequency - 1
        super(CrawlPlugin, self).__init__()

    # -------------------------------------------------------------------------
    def fire(self):
        """
        Run the plugin.
        """
        sys.modules[self.name].main(self.cfg)
        self.last_fired = time.time()
        
    # -------------------------------------------------------------------------
    def init_cfg_data(self, name=''):
        """
        Read data we care about from the configuration.
        """
        if name != '':
            self.name = name
        try:
            x = self.cfg.get(self.name, 'fire')
            if x.lower() in self.cfg._boolean_states.keys():
                self.firable = self.cfg._boolean_states[x]
            else:
                self.firable = False
        except CrawlConfig.NoOptionError:
            self.firable = True
        self.frequency = self.cfg.get_time(self.name, 'frequency', 3600)
        self.plugin_dir = self.cfg.get('crawler', 'plugin-dir')
        if self.plugin_dir not in sys.path:
            sys.path.append(self.plugin_dir)
        if self.name not in sys.modules.keys():
            __import__(self.name)
        else:
            reload(sys.modules[self.name])

    # -------------------------------------------------------------------------
    def reload(self, cfg):
        """
        Re-initialize this object from the configuration.
        """
        self.init_cfg_data(cfg)
        
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
    shutil.rmtree('test_plugins')

# -----------------------------------------------------------------------------
class CrawlPluginTest(unittest.TestCase):
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
        self.assertEqual(os.path.exists('./%s' % (pname)), True)
        self.assertEqual(util.contents('./%s' % (pname)),
                         'my name is %s\n' % (pname))

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
            
        self.assertEqual(p.firable, False)

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
            
        self.assertEqual(p.firable, True)
        
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
            
        self.assertEqual(p.firable, True)
        
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
            
        self.assertEqual(p.frequency, 19)

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
            
        self.assertEqual(p.frequency, 3600)

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
        self.assertEqual(pre, sys.path)

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
        self.assertNotEqual(pre, sys.path)

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
            got_exception = False
        except CrawlConfig.NoOptionError:
            got_exception = True
        self.assertEqual(got_exception, True)

    # -------------------------------------------------------------------------
    def test_init_plugin_inmod(self):
        """
        if plugin does exist and is in module list, should be reloaded
        """
        c = CrawlConfig.CrawlConfig()
        c.set('test_init', 'frequency', '19')
        c.set('crawler', 'plugin-dir', 'plugins')
        try:
            p = CrawlPlugin.CrawlPlugin('test_init', c)
            import_failed = False
        except ImportError:
            import_failed = True
            
        self.assertEqual(p.firable, True)
        self.assertEqual(p.frequency, 19)
        self.assertEqual(import_failed, True)
        
        raise testhelp.UnderConstructionError()

    # -------------------------------------------------------------------------
    def test_init_plugin_ninmod(self):
        """
        if plugin does exists and not in module list, should be imported
        """
        raise testhelp.UnderConstructionError()
    # -------------------------------------------------------------------------
    def test_init_plugin_nosuch(self):
        """
        if plugin does not exist, should get ImportError
        """
        raise testhelp.UnderConstructionError()

    # -------------------------------------------------------------------------
    def test_reload_fire(self):
        """
        Update fire
        """
        raise testhelp.UnderConstructionError()
    
    # -------------------------------------------------------------------------
    def test_reload_freq(self):
        """
        Update frequency
        """
        raise testhelp.UnderConstructionError()
    
    # -------------------------------------------------------------------------
    def test_reload_plugdir(self):
        """
        Update plugdir
        """
        raise testhelp.UnderConstructionError()
    
    # -------------------------------------------------------------------------
    def test_reload_plugdir(self):
        """
        Update plugin
        """
        raise testhelp.UnderConstructionError()
    
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
    def make_plugin(self, pname):
        if not os.path.isdir(self.plugdir):
            os.mkdir(self.plugdir)
        if not pname.endswith('.py'):
            fname = pname + '.py'
        else:
            fname = pname
        f = open('%s/%s' % (self.plugdir, fname), 'w')
        f.write("#!/bin/env python\n")
        f.write("def main(cfg):\n")
        f.write("    f = open('%s', 'w')\n" % (pname))
        f.write("    f.write('my name is %s\\n')\n" % (pname))
        f.write("    f.close()\n")
        
        f.close()
        
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    toolframe.ez_launch(setup=CrawlPlugin_setup,
                        cleanup=CrawlPlugin_teardown,
                        test='CrawlPluginTest',
                        logfile='crawl_test.log')
