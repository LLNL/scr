import os
import re

from scrjob import config
from scrjob.common import interpolate_variables, scr_prefix


class Param():

    def __init__(self):
        sysconf = config.SCR_CONFIG_FILE

        # get current working dir
        # use value in $SCR_PREFIX if set
        prefix = scr_prefix()

        # define user config file
        # use SCR_CONF_FILE if set
        # if not set look in the prefix directory
        usrfile = os.environ.get('SCR_CONF_FILE')
        if usrfile is None:
            usrfile = os.path.join(prefix, '.scrconf')

        # read in the app configuration file, if specified
        appfile = os.path.join(prefix, '.scr', 'app.conf')

        #if os.path.isfile(appfile) and os.access(appfile,'R_OK'):
        # better to just try than to check for permission
        # the read_config_file method has a try catch block
        self.appconf = self.read_config_file(appfile)

        # read in the user configuration file, if specified
        self.usrconf = self.read_config_file(usrfile)

        # read in the system configuration file
        self.sysconf = self.read_config_file(sysconf)

        self.compile = {}
        # set our compile time constants
        self.compile['CNTLDIR'] = {}
        self.compile['CNTLDIR'][config.SCR_CNTL_BASE] = {}
        self.compile['CACHEDIR'] = {}
        self.compile['CACHEDIR'][config.SCR_CACHE_BASE] = {}
        self.compile['SCR_CNTL_BASE'] = {}
        self.compile['SCR_CNTL_BASE'][config.SCR_CNTL_BASE] = {}
        self.compile['SCR_CACHE_BASE'] = {}
        self.compile['SCR_CACHE_BASE'][config.SCR_CACHE_BASE] = {}
        self.compile['SCR_CACHE_SIZE'] = {}
        self.compile['SCR_CACHE_SIZE'][config.SCR_CACHE_SIZE] = {}

        # set our restricted parameters,
        # these can't be set via env vars or user conf file
        self.no_user = {}

        # NOTE: At this point we could scan the environment and user config file
        # for restricted parameters to print a warning.  However, in this case
        # printing the extra messages from a perl script whose output is used by
        # other scripts as input may do more harm than good.  Printing the
        # warning in the library should be sufficient.

        # if CACHE_BASE and CACHE_SIZE are set and the user didn't set CACHEDESC,
        # create a single CACHEDESC in the user hash
        self.cache_base = self.get('SCR_CACHE_BASE')
        self.cache_size = self.get('SCR_CACHE_SIZE')
        if 'CACHE' not in self.usrconf and self.cache_base is not None and self.cache_size is not None:
            self.usrconf['CACHE'] = {}
            self.usrconf['CACHE'][self.cache_base] = {}
            self.usrconf['CACHE'][self.cache_base]['SIZE'] = {}
            self.usrconf['CACHE'][self.cache_base]['SIZE'][
                self.cache_size] = {}

    def read_config_file(self, filename):
        h = {}
        try:
            os.makedirs('/'.join(filename.split('/')[:-1]), exist_ok=True)
            with open(filename, 'r') as infile:
                for line in infile.readlines():
                    line = line.strip()
                    line = re.sub('=', ' ', line)  # replace '=' with spaces
                    parts = line.split(' ')
                    key = ''
                    top_key = ''
                    top_value = ''
                    lastpart = len(parts) - 1
                    first = True  # need the next top_key
                    for i, part in enumerate(parts):
                        if len(part) == 0:  # input had double-spaces
                            continue
                        if part[0] == '#':
                            break
                        # read in the value (should have at least one more item in the list)
                        if first == True:
                            if i == lastpart:
                                print(
                                    'scr_param: ERROR: Invalid key=value pair detected in '
                                    + filename + '.')
                                return {}
                            key = part.upper()
                            first = False
                        else:
                            value = interpolate_variables(part)
                            if top_key != '':
                                if key not in h[top_key][top_value]:
                                    h[top_key][top_value][key] = {}
                                h[top_key][top_value][key][value] = {}
                            else:
                                top_key = key
                                top_value = value
                                if top_key not in h:
                                    h[top_key] = {}
                                if top_value not in h[top_key]:
                                    h[top_key][top_value] = {}
                            first = True

        except Exception as e:
            # print(e)
            # print('scr_param: ERROR: Could not open file: '+filename)
            return {}
        return h

    def get(self, name):
        val = os.environ.get(name)

        # if param is set in environment, return that value
        if name not in self.no_user and val is not None:
            return interpolate_variables(val)

        # otherwise, check whether we have it defined in our user config file
        if name not in self.no_user and name in self.usrconf:
            return list(self.usrconf[name].keys())[0]

        # if param was set by the code, return that value
        if name in self.appconf:
            return list(self.appconf[name].keys())[0]

        # otherwise, check whether we have it defined in our system config file
        if name in self.sysconf:
            return list(self.sysconf[name].keys())[0]

        # otherwise, check whether its a compile time constant
        if name in self.compile:
            return list(self.compile[name].keys())[0]

        return None

    def get_hash(self, name):
        val = os.environ.get(name)

        # if param is set in environment, return that value
        if name not in self.no_user and val is not None:
            h = {}
            name = interpolate_variables(val)
            h[name] = {}
            return h

        # otherwise, check whether we have it defined in our user config file
        if name not in self.no_user and name in self.usrconf:
            return self.usrconf[name].copy()

        # otherwise, check whether we have it defined in our system config file
        if name in self.sysconf:
            return self.sysconf[name].copy()

        # otherwise, check whether its a compile time constant
        if name in self.compile:
            return self.compile[name].copy()

        return None

    # convert byte string like 2kb, 1.5m, 200GB, 1.4T to integer value
    def abtoull(self, stringval):
        number = ''
        units = ''
        tokens = re.match('(\d*)(\.?)(\d*)(\D+)', stringval).groups()
        if len(tokens) > 1:
            number = ''.join(tokens[:-1])
            units = tokens[-1].lower()
        else:
            return int(stringval)  # TODO: print error? unknown unit string
        factor = None
        if units != '':
            if units == 'b':
                factor = 1
            elif units == 'kb' or units == 'k':
                factor = 1024
            elif units == 'mb' or units == 'm':
                factor = 1024 * 1024
            elif units == 'gb' or units == 'g':
                factor = 1024 * 1024 * 1024
            elif units == 'tb' or units == 't':
                factor = 1024 * 1024 * 1024 * 1024
            elif units == 'pb' or units == 'p':
                factor = 1024 * 1024 * 1024 * 1024 * 1024
            elif units == 'eb' or units == 'e':
                factor = 1024 * 1024 * 1024 * 1024 * 1024 * 1024
            else:
                return int(stringval)  # TODO: print error? unknown unit string
        val = float(number)
        if factor is not None:
            val *= factor
        elif units != '':
            val = 0.0  # got a units string but couldn't parse it
        val = int(val)
        return val


if __name__ == '__main__':
    scr_param = Param()
    for key in scr_param.compile:
        print('scr_param.compile[' + key + '] = ' +
              str(scr_param.compile[key]))
