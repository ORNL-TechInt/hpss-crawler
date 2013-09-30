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
import ConfigParser
import os
import sys
import time
import toolframe
import unittest

# -----------------------------------------------------------------------------
class CrawlPlugin(object):
    # -------------------------------------------------------------------------
    def __init__(self, name, cfg):
        """
        Initialize this object.
        """
        self.init_cfg_data(cfg, name)
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
    def init_cfg_data(self, cfg, name=''):
        """
        Read data we care about from the configuration.
        """
        self.cfg = cfg
        if name != '':
            self.name = name
        self.firable = cfg.getboolean(self.name, 'fire')
        self.frequency = cfg.get_time(self.name, 'frequency', 3600)
        self.plugin_dir = cfg.get('crawler', 'plugin-dir')
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
    pass

# -----------------------------------------------------------------------------
class CrawlPluginTest(unittest.TestCase):
    # -------------------------------------------------------------------------
    def test_init(self):
        pass

    # -------------------------------------------------------------------------
    def test_fire(self):
        pass

    # -------------------------------------------------------------------------
    def test_reload(self):
        pass
    
    # -------------------------------------------------------------------------
    def test_time_to_fire(self):
        pass

# -----------------------------------------------------------------------------
if __name__ == '__main__':
    toolframe.ez_launch(setup=CrawlPlugin_setup,
                        cleanup=CrawlPlugin_teardown,
                        test='CrawlPluginTest',
                        logfile='crawl_test.log')
