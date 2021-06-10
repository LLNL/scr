#! /usr/bin/env python

#scr_glob_hosts.py

import sys
from scr_common import getconf
import scr_hostlist

def print_usage(prog):
  print('')
  print('  Usage:  '+prog+' [options]')
  print('')
  print('  Options:')
  print('    -c, --count                 Print the number of hosts.')
  print('    -n, --nth <num>             Output the Nth host (1=lo, -1=hi).')
  print('    -h, --hosts <hosts>         Use this hostlist.')
  print('    -m, --minus <s1:s2>         Elements of s1 not in s2.')
  print('    -i, --intersection <s1:s2>  Intersection of s1 and s2 hosts.')
  print('    -C, --compress <csv hosts>  Compress the csv hostlist')
  print('')
  
def scr_glob_hosts(argv):
  prog = 'scr_glob_hosts'
  conf = getconf(argv,{'-c':'count','--count':'count','-n':'nth','--nth':'nth','-h':'hosts','--hosts':'hosts','-m':'minus','--minus':'minus','-i':'intersection','--intersection':'intersection','-C':'compress','--compress':'compress'})
  if conf is None:
    print_usage(prog)
    return 1
  hostset = []
  if 'hosts' in conf:
    hostset = scr_hostlist.expand(conf['hosts'])
  elif 'minus' in conf:
    if ':' in conf['minus']:
      pieces = conf['minus'].split(':')
      set1 = scr_hostlist.expand(pieces[0])
      set2 = scr_hostlist.expand(pieces[1])
      hostset = scr_hostlist.diff(set1,set2)
    else:
      print_usage(prog)
      return 1
  elif 'intersection' in conf:
    if ':' in conf['intersection']:
      pieces = conf['intersection'].split(':')
      set1 = scr_hostlist.expand(pieces[0])
      set2 = scr_hostlist.expand(pieces[1])
      hostset = scr_hostlist.intersect(set1,set2)
    else:
      print_usage(prog)
      return 1
  elif 'compress' in conf:
    # if the argument is a csv then the compress function has no effect
    # this python implementation can just return the parameter . . .
    # (to act as 'printing' it then exiting)
    return conf['compress'] # returns the csv string
  else: #if not valid
    print_usage(prog)
    return 1
  # ok, got our resulting nodeset, now print stuff to the screen
  if 'nth' in conf:
    # print out the nth node of the nodelist
    n = int(conf['nth'])
    if n > len(hostset) or n < -len(hostset):
      print(prog+': ERROR: Host index ('+str(n)+') is out of range for the specified host list.')
      return 1
    if n>0: # an initial n=0 or n=1 both return the same thing
      n-=1
    # return the nth element
    return hostset[n]
  # return the number of nodes (length) in the nodelist
  if 'count' in conf:
    return len(hostset)
  # return a csv string representation of the nodelist
  return scr_hostlist.compress(hostset)

if __name__=='__main__':
  ret = scr_glob_hosts(sys.argv[1:])
  print('scr_glob_hosts returned '+str(ret))

