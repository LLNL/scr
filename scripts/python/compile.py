#! /usr/bin/env python

# This script replaces the compile time constants in the other files

import re, sys, os

'''
definevars = {}
var=''
for i in range(1,len(sys.argv)):
  if len(var)>0:
    definevars[var]=sys.argv[i]
    var=''
  else:
    var=sys.argv[i]
'''
definevars = { '@SCR_CNTL_BASE@':'./', '@SCR_CACHE_BASE@':'./', '@SCR_CONFIG_FILE@':'test.cfg' }

filenames = os.listdir(os.getcwd())

for filename in filenames:
  if not filename.endswith('.py') or filename == sys.argv[0]:
    continue
  lines = []
  with open(filename,'r') as infile:
    lines = infile.readlines()
  with open(filename,'w') as outfile:
    for line in lines:
      if line[0]=='\n' or line[0]=='#':
        outfile.write(line)
        continue
      for key in definevars:
        if key in line:
          line = re.sub(key,definevars[key],line)
      outfile.write(line)

