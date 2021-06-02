#! /usr/bin/env python

# SCR Param

import os, re

#####
sysconf = "@SCR_CONFIG_FILE@"

class SCR_Param():
  def __init__(self):
    self.prog = None
    self.userconf = None
    self.sysconf = None
    self.compile = None
    self.no_user = None
  def new(): # new in init ..? ^^
    self.prog = 'scr_param'
    # get current working dir
    # use value in $SCR_PREFIX if set
    prefix = os.environ.get('SCR_PREFIX')
    if val is None:
      prefix = os.getcwd()
    else:
      # tack on current working dir if needed # don't resolve symlinks # don't worry about missing parts, the calling script calling might create it
      if prefix[0]=='~':
        prefix = '$HOME'+prefix[1:]
      elif prefix[0]=='.':
        prefix = os.getcwd()+prefix[1:]
      prefix = os.path.expandvars(prefix)
    # define user config file
    # use SCR_CONF_FILE if set
    usrfile = os.environ.get('SCR_CONF_FILE')
    if usrfile is None:
      #  } elsif (defined $prefix) { # otherwise, look in the prefix directory
      usrfile = prefix+'/.scrconf'

    # read in the app configuration file, if specified
    appfile = prefix+'/.scr/app.conf'
    if os.path.isfile(appfile) and os.access(appfile,'R_OK'):
      self.appconf = read_config_file(appfile)

    # read in the user configuration file, if specified
    if os.path.isfile(usrfile) and os.access(usrfile,'R_OK'):
      self.usrconf = read_config_file(usrfile)

    # read in the system configuration file
    if os.path.isfile(sysconf) and os.access(sysconf,'R_OK'):
      self.sysconf = read_config_file(sysconf)
{
    # set our compile time constants
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
}
