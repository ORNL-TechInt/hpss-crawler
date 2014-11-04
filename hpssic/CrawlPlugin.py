#!/usr/bin/env python
"""
Plugin class for HPSS integrity crawler

This module contains the CrawlPlugin class.
"""
import copy
import CrawlConfig
import re
import shutil
import sys
import time
import util


# -----------------------------------------------------------------------------
class CrawlPlugin(object):
    """
    An object of this class represents a crawler plugin which is to be run
    periodically. The plugin's attributes are defined in a configuration file
    and this class loads them from there.
    """
    # -------------------------------------------------------------------------
    def __init__(self, name=None, cfg=None):
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
        assert(name is not None)
        assert(cfg is not None)
        self.cfg = cfg
        l = CrawlConfig.log(cfg=cfg)
        CrawlConfig.log("%s: Initializing plugin data" % name)
        self.init_cfg_data(name, cfg)
        self.last_fired = time.time() - self.frequency - 1
        super(CrawlPlugin, self).__init__()

    # -------------------------------------------------------------------------
    def fire(self):
        """
        Run the plugin.
        """
        if self.firable:
            CrawlConfig.log("%s: firing" % self.name)
            # sys.modules[self.modname].main(self.cfg)
            self.plugin.main(self.cfg)
            self.last_fired = time.time()
        elif self.cfg.getboolean('crawler', 'verbose'):
            CrawlConfig.log("%s: not firable" % self.name)
            self.last_fired = time.time()

    # -------------------------------------------------------------------------
    def init_cfg_data(self, name='', cfg=None):
        """
        Read data we care about from the configuration.
        """
        if name != '':
            self.name = name
        if cfg is not None:
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
        self.plugin_dir = cfg.get('crawler', 'plugin-dir')
        if self.plugin_dir not in sys.path:
            sys.path.insert(0, self.plugin_dir)

        self.modname = self.cfg.get(self.name, 'module')

        if self.modname in sys.modules.keys():
            filename = re.sub("\.pyc?", ".pyc",
                              sys.modules[self.modname].__file__)
            util.conditional_rm(filename)

            del sys.modules[self.modname]

        try:
            self.plugin = __import__(self.modname)
        except ImportError:
            H = __import__('hpssic.plugins.' + self.modname)
            self.plugin = getattr(H.plugins, self.modname)

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
