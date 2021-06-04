#! /usr/bin/env python

# scr_prefix.py

# get the current working dir

import os

def scr_prefix():
  prefix = os.environ.get('SCR_PREFIX')
  if prefix is None:
    prefix = os.getcwd()
  else:
    # tack on current working dir if needed
    # don't resolve symlinks
    # don't worry about missing parts, the calling script calling might create it
    # replace ~ and . symbols from front of path
    if prefix[0]=='~':
      prefix = '$HOME'+prefix[1:]
    elif prefix[0]=='.':
      prefix = os.getcwd()+prefix[1:]
    # expand any environment vars in path
    prefix = os.path.expandvars(prefix)
  return prefix
