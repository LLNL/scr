#! /usr/bin/env python

# compileconsts.py
# called to set compile time constants (from scr_param.py(?) or during building)

import re, sys, os

# This script replaces the compile time constants in python scripts in this directory
# takes vals -> a dictionary
# the keys of the dictionary are words to replace with the key's value
# vals['@SCR_CNTL_BASE@'] = 'CNTLDIR'
# any instances of @SCR_CNTL_BASE@ are replaced with CNTLDIR

vals = {}
vals['@SCR_CNTL_BASE@']='CNTLDIR'
vals['@SCR_CACHE_BASE@']='CACHEDIR'
vals['@SCR_CNTL_BASE@']='SCR_CNTL_BASE'
vals['@SCR_CACHE_BASE@']='SCR_CACHE_BASE'
vals['SCR_CACHE_SIZE']='1'

filenames = os.listdir(os.getcwd())
for filename in filenames:
  if not filename.endswith('.py') or filename == sys.argv[0]:
    continue
  lines = []
  with open(filename,'r') as infile:
    lines = infile.readlines()
  incomment=False
  with open(filename,'w') as outfile:
    for line in lines:
      if incomment==True:
        if line.endswith('\'\'\''):
          incomment=False
      elif line.beginswith('\'\'\''):
        incomment=True
      elif line[0]!='\n' and line[0]!='#':
        for key in vals:
          if key in line:
            line = re.sub(key,vals[key],line)
      outfile.write(line)
