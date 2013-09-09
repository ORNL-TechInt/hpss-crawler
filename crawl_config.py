class CrawlConfig(ConfigParser.ConfigParser):
    # --------------------------------------------------------------------------
    def __init__(self, **kwargs):
        self.filename = '<???>'
        self.loadtime = 0.0
        super(CrawlConfig, self).__init__(**kwargs)

    # --------------------------------------------------------------------------
    def changed(self):
        """
        Return True if the file we were loaded from has changed since load time.
        """
        if self.filename != '<???>' and self.loadtime != 0.0:
            s = os.stat(self.filename)
            rval = (self.loadtime < s[stat.ST_MTIME])
        else:
            rval = False
        return rval
    
    # --------------------------------------------------------------------------
    def dump(self, with_defaults=False):
        """
        Write the contents of the config except for the defaults to a string and
        return the string. If with_defaults = True, include the DEFAULTS
        section.
        """
        rstr = StringIO.StringIO()
        self.write(rstr)
        rval = rstr.getvalue()
        rstr.close()
        if not with_defaults:
            rval = re.sub('\[DEFAULT\][^\[]*\[', '[', rval)
        return rval

    # --------------------------------------------------------------------------
    def get_time(self, section, option, default=None, logger=None):
        """
        Retrieve the value of section/option. It is assumed to be a duration
        specification, like -- '10 seconds', '2hr', '7 minutes', or the like. We
        will call map_time_unit to convert the unit into a number of seconds,
        then multiply by the magnitude, and return an int number of seconds. If
        the caller specifies a default and we get a NoSectionError or
        NoOptionError, we will return the caller's default. Otherwise, we raise
        the exception.
        """
        try:
            spec = self.get(section, option)
            [(mag, unit)] = re.findall('(\d+)\s*(\w*)', spec)
            mult = self.map_time_unit(unit)
            rval = int(mag) * mult
        except ConfigParser.NoOptionError as e:
            if default != None:
                rval = default
                if logger != None:
                    logger.info(str(e) + '; using default value %d' % default)
            else:
                raise
        except ConfigParser.NoSectionError as e:
            if default != None:
                rval = default
                if logger != None:
                    log.info(str(e) + '; using default value %d' % default)
            else:
                raise
        
    # --------------------------------------------------------------------------
    def load_dict(self, dict, defaults=None):
        """
        Initialize the config from dict. If one of the keys in dict is
        'defaults' or 'DEFAULTS', that sub-dict will be used to initialize the
        _defaults member
        """
        # make sure we're cleaned out
        for k in self._defaults.keys():
            del self._defaults[k]
        for s in self.sections():
            self.remove_section(s)

        # If we got defaults, set them first
        if defaults != None:
            for k in defaults.keys():
                self._defaults[k] = defaults[k]

        # Now file the config with the material from the dict
        for s in sorted(dict.keys()):
            self.add_section(s)
            for o in sorted(dict[s].keys()):
                self.set(s, o, dict[s][o])
    
    # --------------------------------------------------------------------------
    def map_time_unit(self, spec):
        """
        1s         => 1
        1 min      => 60
        2 days     => 2 * 24 * 3600
        """
        done = False
        while not done:
            try:
                rval = self._map[spec]
                done = True
            except AttributeError:
                self._map = {'': 1,
                             's': 1,
                             'sec': 1,
                             'second': 1,
                             'seconds': 1,
                             'm': 60,
                             'min': 60,
                             'minute': 60,
                             'minutes': 60,
                             'h': 3600,
                             'hr': 3600,
                             'hour': 3600,
                             'hours': 3600,
                             'd': 24 * 3600,
                             'day': 24 * 3600,
                             'days': 24 * 3600,
                             'w': 7 * 24 * 3600,
                             'week': 7 * 24 * 3600,
                             'weeks': 7 * 24 * 3600,
                             'month': 30 * 24 * 3600,
                             'months': 30 * 24 * 3600,
                             'y': 365 * 24 * 3600,
                             'year': 365 * 24 * 3600,
                             'years': 365 * 24 * 3600,
                             }
                done = False
            except KeyError:
                rval = 1
                done = True

        return rval
        

    # --------------------------------------------------------------------------
    def write(self, filename):
        """
        Write the config material to filename with the 'crawler' section first.
        If there is no 'crawler' section, raise a NoSectionError.
        """
        pass
