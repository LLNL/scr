#! /usr/bin/env python

# scr_param.py
# class SCR Param

import os, re
import scr_common

sysconf = "@SCR_CONFIG_FILE@"

class SCR_Param():
  def __init__(self):
    self.prog = 'scr_param'
    self.usrconf = {}
    self.sysconf = {}
    self.compile = {}
    self.no_user = {}
    self.appconf = {}
    # get current working dir
    # use value in $SCR_PREFIX if set
    prefix = scr_common.scr_prefix()
    # define user config file
    # use SCR_CONF_FILE if set
    usrfile = os.environ.get('SCR_CONF_FILE')
    # if not set look in the prefix directory
    if usrfile is None:
      usrfile = prefix+'/.scrconf'
    # read in the app configuration file, if specified
    appfile = prefix+'/.scr/app.conf'
    if os.path.isfile(appfile) and os.access(appfile,'R_OK'):
      self.appconf = self.read_config_file(appfile)

    # read in the user configuration file, if specified
    if os.path.isfile(usrfile) and os.access(usrfile,'R_OK'):
      self.usrconf = self.read_config_file(usrfile)

    # read in the system configuration file
    if os.path.isfile(sysconf) and os.access(sysconf,'R_OK'):
      self.sysconf = self.read_config_file(sysconf)
    # set our compile time constants
    self.compile["CNTLDIR"] = "@SCR_CNTL_BASE@"
    self.compile["CACHEDIR"] = "@SCR_CACHE_BASE@"
    self.compile["SCR_CNTL_BASE"] = "@SCR_CNTL_BASE@"
    self.compile["SCR_CACHE_BASE"] = "@SCR_CACHE_BASE@"
    self.compile["SCR_CACHE_SIZE"] = "1"
    self.cache_base = self.get('SCR_CACHE_BASE')
    self.cache_size = self.get('SCR_CACHE_SIZE')
    if 'CACHE' not in self.usrconf:
      self.usrconf['CACHE'] = (self.cache_base,self.cache_size)
  '''
  $self->{compile} = {};
  $self->{compile}{"CNTLDIR"}{"@SCR_CNTL_BASE@"} = {};
  $self->{compile}{"CACHEDIR"}{"@SCR_CACHE_BASE@"} = {};
  $self->{compile}{"SCR_CNTL_BASE"}{"@SCR_CNTL_BASE@"} = {};
  $self->{compile}{"SCR_CACHE_BASE"}{"@SCR_CACHE_BASE@"} = {};
  $self->{compile}{"SCR_CACHE_SIZE"}{"1"} = {};

  # set our restricted parameters,
  # these can't be set via env vars or user conf file
  $self->{no_user} = {};
  # NOTE: At this point we could scan the environment and user config file
  # for restricted parameters to print a warning.  However, in this case
  # printing the extra messages from a perl script whose output is used by
  # other scripts as input may do more harm than good.  Printing the
  # warning in the library should be sufficient.

  # if CACHE_BASE and CACHE_SIZE are set and the user didn't set CACHEDESC,
  # create a single CACHEDESC in the user hash
  my $cache_base = get($self, "SCR_CACHE_BASE");
  my $cache_size = get($self, "SCR_CACHE_SIZE");
  if (not defined $self->{usrconf}{"CACHE"} and
      defined $cache_base and
      defined $cache_size)
  {
    $self->{usrconf}{"CACHE"}{$cache_base}{"SIZE"}{$cache_size} = {};
  }

  return bless $self, $type;
  '''
  def read_config_file(self,filename):
    h = {}
    with open (filename,'r') as infile:
      for line in infile.readlines():
        line=line.rstrip('\n')
        line = re.sub('^\s*','',line) # strip any leading whitespace from line
        line = re.sub('\s*$','',line) # strip any trailing whitespace from line
        line = re.sub('=',' ',line) # replace '=' with spaces
        parts = line.split(' ')
        key = ''
        for part in parts:
          if len(part)==0: # input had double-spaces
            continue
          if key=='':
            if part[0]=='#': # comment line
              break
            key=part
            continue
  def get(self,key):
    val = os.environ.get(key)

    # if param is set in environment, return that value
    if key not in self.no_user and val is not None:
      val = os.path.expandvars(val)
      return val

    # otherwise, check whether we have it defined in our user config file
    if key not in self.no_user and key in self.usrconf:
      return self.usrconf[key]

    # if param was set by the code, return that value
    if key in self.appconf:
      return self.appconf[key]

    # otherwise, check whether we have it defined in our system config file
    if key in self.sysconf:
      return self.sysconf[key]

    # otherwise, check whether its a compile time constant
    if key in self.compile:
      return self.compile[key]

    return None

  # (the gethash seems unnecessary ... ?)
  def get_hash(self,key):
    val = self.get(key)
    return val

  # convert byte string like 2kb, 1.5m, 200GB, 1.4T to integer value
  def abtoull(self,stringval):
    number = ''
    units = ''
    tokens = re.match('(\d*)(\.?)(\d*)(\D+)',stringval).groups()
    if len(tokens)>1:
      number = ''.join(tokens[:-1])
      units = tokens[-1].lower()
    else:
      number = stringval
    factor = None
    if units!='':
      if units=='b':
        factor=1
      elif units=='kb' or units=='k':
        factor=1024
      elif units=='mb' or units=='m':
        factor=1024*1024
      elif units=='gb' or units=='g':
        factor=1024*1024*1024
      elif units=='tb' or units=='t':
        factor=1024*1024*1024*1024
      elif units=='pb' or units=='p':
        factor=1024*1024*1024*1024*1024
      elif units=='eb' or units=='e':
        factor=1024*1024*1024*1024*1024*1024
      else:
        pass # TODO: print error? unknown unit string
    val = float(number)
    if factor is not None:
      val*=factor
    elif units!='':
      val = 0.0 # got a units string but couldn't parse it
    val = int(val)
    return val

