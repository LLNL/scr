#! /usr/bin/env python3

# scr_hostlist.py

# This package processes slurm-style hostlist strings.
#
# expand($hostlist)
#   returns a list of individual hostnames given a hostlist string
# compress(@hostlist)
#   returns an ordered hostlist string given a list of hostnames
#
# Author:  Adam Moody (moody20@llnl.gov)
# modified by Christopher Holguin <christopher.a.holguin@intel.com>

import argparse, re, sys


# numbersfromrange is a helper function, allows leading zeroes
# returns a list of string representations of numbers
# '2-4,6' -> '2','3','4','6'
# left fills zeroes if any number starts with a zero
# (so all numbers are same length)
# '08-10' -> '08','09','10'
# numbersfromrange('0003,99,999,123,1234,2345667')
# ret -> ['0000003', '0000099', '0000999', '0000123', '0001234', '2345667']
def numbersfromrange(numstring):
  numbers = []
  startrange = 0
  buildnum = 0
  havestart = False
  digits = 0
  maxdigits = 0
  once = False
  leadzero = False
  for c in numstring:
    if c == ',':
      if havestart == True:
        for num in range(startrange, buildnum + 1):
          numbers.append(num)
        havestart = False
      else:
        numbers.append(buildnum)
      buildnum = 0
      if digits > maxdigits:
        maxdigits = digits
      digits = 0
    elif c == '-':
      havestart = True
      startrange = buildnum
      buildnum = 0
      if digits > maxdigits:
        maxdigits = digits
      digits = 0
    elif c.isnumeric() == True:
      if buildnum == 0 and digits > 0:
        leadzero = True
      else:
        buildnum *= 10
      digits += 1
      buildnum += int(c)
  if havestart == True:
    for num in range(startrange, buildnum + 1):
      numbers.append(num)
  else:
    numbers.append(buildnum)
  ret = []
  if leadzero == True:
    if digits > maxdigits:
      maxdigits = digits
    for num in numbers:
      strval = str(num)
      if len(strval) < maxdigits:
        strval = ('0' * (maxdigits - len(strval))) + strval
      ret.append(strval)
  else:
    for num in numbers:
      ret.append(str(num))
  return ret


# return the int value of the rightmost number
def numfromhost(host=''):
  number = 0
  place = 1
  for i in range(len(host)-1,-1,-1):
    if host[i] == ',' or host[i] == '-' or host[i] == '[':
      break
    if host[i].isnumeric():
      number += int(host[i]) * place
      place *= 10
  return number


# return a list from a host string
def splithosts(hosts=''):
  if type(hosts) is not str:
    return hosts
  # split the host string on commas
  hosts = hosts.split(',')
  # we could have a host set of the form: host[1,3-5]
  # we shouldn't have split on commas if they are within brackets
  parts = []
  # track whether we are continuing the same part
  samehost = False
  i = -1
  for host in hosts:
    if samehost:
      parts[i] += ',' + host
      if ']' in host:
        samehost = False
    else:
      parts.append(host)
      i += 1
      if '[' in host and ']' not in host:
        samehost = True
  return parts


# sort a string of hosts in ascending order
def sorthoststring(hosts=''):
  if type(hosts) is list:
    hosts = ','.join(hosts)
  if hosts is None or hosts == '' or ',' not in hosts:
    return hosts if hosts is not None else ''
  parts = splithosts(hosts)
  hosts = [ parts[0] ]
  leftnum = numfromhost(parts[0])
  for righti in range(1,len(parts)):
    number = numfromhost(parts[righti])
    # we need to put this one earlier
    if number < leftnum:
      hosts.append('')
      for lefti in range(righti-1,-1,-1):
        if number < numfromhost(hosts[lefti]):
          hosts[lefti+1] = hosts[lefti]
        else:
          hosts[lefti+1] = parts[righti]
          break
      if number < numfromhost(hosts[0]):
        hosts[0] = parts[righti]
    else:
      hosts.append(parts[righti])
      leftnum = number
  return ','.join(hosts)


# rangefromnumbers is a helper function to collapse a list of numbers, if possible.
# accepts number strings that already have a range ( the '-' operator )
# '1,2,3,4,5,9,11,12' -> '1-5,9,11-12'
# '1,2,3,5-7' -> '1-3,5-7'
def rangefromnumbers(numstring):
  if len(numstring) == 0:
    return ''
  if ',' not in numstring:
    return numstring
  ret = ''
  firstval = 0
  checknext = False
  haverange = False
  nextval = 0
  buildval = 0
  for c in numstring:
    if c.isnumeric() == True:
      buildval *= 10
      buildval += int(c)
    elif c == ',':
      if haverange == True:
        haverange = False
        checknext = True
        nextval = buildval + 1
      elif checknext == True:
        if buildval > nextval:
          if nextval == firstval + 1:
            ret += str(firstval) + ','
          else:
            ret += str(firstval) + '-' + str(nextval - 1) + ','
          firstval = buildval
        nextval = buildval + 1
      else:  #if checknext==False:
        checknext = True
        firstval = buildval
        nextval = firstval + 1
      buildval = 0
    elif c == '-':
      haverange = True
      if checknext == False or buildval != nextval:
        if nextval == firstval + 1:
          ret += str(firstval) + ','
        else:
          ret += str(firstval) + '-' + str(nextval - 1) + ','
        firstval = buildval
      buildval = 0
  if haverange == True:
    ret += str(firstval) + '-' + str(buildval)
  elif buildval > nextval:
    if nextval == firstval + 1:
      ret += str(firstval) + ',' + str(buildval)
    else:
      ret += str(firstval) + '-' + str(nextval - 1) + ',' + str(buildval)
  else:
    ret += str(firstval) + '-' + str(buildval)
  return ret


# Returns a list of hostnames given a hostlist string
# expand("rhea[2-4,6]") returns ['rhea2','rhea3','rhea4','rhea6']
# hostrange can contain an optional suffix after brackets:
#   rhea[2-4,6].llnl.gov
# multiple ranges can be listed as csv:
#   machine[1-3,5],machine[7-8],machine10
# left fills with 0 if a host starts with 0:
#   machine[08-10] --> machine08,machine09,machine10
def expand(nodelist):
  if nodelist is None:
    return []
  nodelist = sorthoststring(nodelist)
  if nodelist == '':
    return []
  # list of gathered nodes (string list)
  prefixes = []
  # list of numstrings
  numstrings = []
  # corresponding list of suffixes
  suffixes = []
  # building strings
  prefix = ''
  numstring = ''
  suffix = ''
  # split the input on commas
  chunks = nodelist.split(',')
  # iterate over each chunk, can be one of the forms:
  # 'rhea2' 'rhea[2' 'rhea[2-3' '3' '5-7' '8]' '8].llnl' 'rhea[2-3].llnl' 'rhea2.llnl'
  for chunk in chunks:
    # this chunk has a prefix and at least one number
    if '[' in chunk:
      pieces = chunk.split('[')
      # if we have the closing bracket this chunk is complete
      if ']' in pieces[1]:
        prefixes.append(pieces[0])
        prefix = ''
        pieces = pieces[1].split(']')
        numstrings.append(pieces[0])
        if len(pieces) > 1:
          suffixes.append(pieces[1])
        else:
          suffixes.append('')
      # otherwise there is only the prefix and some number/range
      else:
        prefix = pieces[0]
        numstring = pieces[1]
    # a bracket is closed
    elif ']' in chunk:
      pieces = chunk.split(']')
      if len(numstring) > 0:
        numstring += ','
      numstring += pieces[0]
      if len(pieces) > 1:
        suffix = pieces[1]
      else:
        suffix = ''
      if prefix != '':
        prefixes.append(prefix)
        prefix = ''
      numstrings.append(numstring)
      numstring = ''
      suffixes.append(suffix)
      suffix = ''
    # this chunk is a single machine and has no bracket
    elif prefix == '':
      pieces = re.split(r'(\D+)', chunk)
      # a correctly formed machine name will create the list:
      # ['', 'rhea', '42', '.llnl', '']
      # or
      # ['', 'rhea', '42']
      if len(pieces) > 3:
        suffix = pieces[3]
      else:
        suffix = ''
      if len(pieces) > 1:
        prefix = pieces[1]
      else:
        prefix = ''
      if len(pieces) > 2:
        numstring = pieces[2]
      prefixes.append(prefix)
      numstrings.append(numstring)
      suffixes.append(suffix)
      prefix = ''
      numstring = ''
      suffix = ''
    # otherwise we must be in a number section (or malformed / mismatched brackets)
    else:
      numstring += ',' + chunk
  if prefix != '':
    prefixes.append(prefix)
    numstrings.append(numstring)
    suffixes.append(suffix)
  # the lists are ready
  ret = []
  for i in range(len(prefixes)):
    nums = []
    # no prefix, the number is stored in the prefix
    if numstrings[i] == '':
      nums = numbersfromrange(prefixes[i])
    else:
      nums = numbersfromrange(numstrings[i])
    for num in nums:
      ret.append(prefixes[i] + num + suffixes[i])
  return ret


# Returns a hostlist string given a list of hostnames
# compress('rhea2','rhea3','rhea4','rhea6') returns "rhea[2-4,6]"
def compress_range(nodelist):
  if nodelist is None:
    return ''
  nodelist = sorthoststring(nodelist)
  nodelist = splithosts(nodelist)
  if len(nodelist) == 0:
    return ''
  # dictionary keyed on prefix+'0'+suffix
  # holds a number string
  nodedict = {}
  for node in nodelist:
    node = node.strip()
    if len(node) == 0:
      continue
    # a correctly formed machine name will create the list:
    # ['', 'rhea', '42', '.llnl', '']
    # or
    # ['', 'rhea', '42']
    pieces = re.split(r'(\D+)', node)
    key = pieces[1]
    if len(pieces) > 3:
      key += '#' + pieces[3]
    if key in nodedict:
      nodedict[key] += ',' + pieces[2]
    else:
      nodedict[key] = pieces[2]
  ret = ''
  for key in nodedict:
    prefix = key
    suffix = ''
    if '#' in key:
      pieces = key.split('#')
      prefix = pieces[0]
      suffix = pieces[1]
    rangenums = rangefromnumbers(nodedict[key])
    if ret != '':
      ret += ','
    ret += prefix
    if ',' in rangenums or '-' in rangenums:
      ret += '[' + rangenums + ']'
    else:
      ret += rangenums
    ret += suffix
  return ret


# Returns a hostlist string given a list of hostnames
# ( will also try to ensure a string is a comma separated string )
# compress(['rhea2','rhea3','rhea4','rhea6']) returns "rhea2,rhea3,rhea4,rhea6"
def compress(hostlist):
  if hostlist is None:
    return ''
  if type(hostlist) is str:
    # turn any commas (plus space) into just a space
    hostlist = re.sub('\s*,\s*', ' ', hostlist)
    # collapse all whitespace to single space, then remove any leading/trailing space
    hostlist = re.sub('\s+', ' ', hostlist).strip()
    # put commas into the spaces
    hostlist = re.sub('\s', ',', hostlist)
  hostlist = sorthoststring(hostlist)
  return hostlist


# Given references to two lists, subtract elements in list 2 from list 1 and return remainder
def diff(set1, set2):
  if type(set1) is str:
    set1 = set1.split(',')
  if type(set2) is str:
    set2 = set2.split(',')
  # we should have two list references
  if set1 is None:
    return []
  if set2 is None:
    return set1
  if len(set1) == 0:
    return []
  if len(set2) == 0:
    return set1
  ret = set1.copy()
  for node in set2:
    if node in ret:
      ret.remove(node)
  listvals = compress(ret)
  ret = expand(listvals)
  return ret


def intersect(set1, set2):
  if type(set1) is str:
    set1 = set1.split(',')
  if type(set2) is str:
    set2 = set2.split(',')
  if set1 is None or set2 is None:
    return []
  if len(set1) == 0 or len(set2) == 0:
    return []
  ret = []
  for node in set1:
    if node in set2:
      ret.append(node)
  listvals = compress(ret)
  ret = expand(listvals)
  return ret


if __name__ == '__main__':
  parser = argparse.ArgumentParser(
      add_help=False,
      argument_default=argparse.SUPPRESS,
      prog='scr_hostlist',
      epilog='(use a colon to separate sets when using diff or intersect)')
  parser.add_argument('-h',
                      '--help',
                      action='store_true',
                      help='Show this help message and exit.')
  parser.add_argument(
      '--numbersfromrange',
      metavar='<numberlist>',
      type=str,
      help='Expands a number list/range using numbers, commas, and hyphens.')
  parser.add_argument(
      '--rangefromnumbers',
      metavar='<numberlist>',
      type=str,
      help='Compresses a number list/range using numbers, commas, and hyphens.'
  )
  parser.add_argument('--expand',
                      metavar='<numberlist>',
                      type=str,
                      help='Returns a list from a given hostname string.')
  parser.add_argument(
      '--compress_range',
      metavar='host',
      nargs='+',
      help='Returns a compressed string given a list of hostnames')
  parser.add_argument('--compress',
                      metavar='host',
                      nargs='+',
                      help='Returns the hosts as a comma separated string')
  parser.add_argument('--diff',
                      metavar='<set1:set2>',
                      type=str,
                      help='Returns elements of set1 not in set2.')
  parser.add_argument('--intersect',
                      metavar='<set1:set2>',
                      type=str,
                      help='Returns elements of set1 that are in set2.')
  args = vars(parser.parse_args())
  if 'help' in args:
    parser.print_help()
    sys.exit(0)
  if 'numbersfromrange' in args:
    print('numbersfromrange(' + args['numbersfromrange'] + ')')
    print('  -> ' + str(numbersfromrange(args['numbersfromrange'])))
  if 'rangefromnumbers' in args:
    print('rangefromnumbers(' + args['rangefromnumbers'] + ')')
    print('  -> ' + str(rangefromnumbers(args['rangefromnumbers'])))
  if 'expand' in args:
    print('expand(' + args['expand'] + ')')
    print('  -> ' + str(expand(args['expand'])))
  if 'compress_range' in args:
    print('compress_range(' + str(args['compress_range']) + ')')
    print('  -> ' + str(compress_range(args['compress_range'])))
  if 'compress' in args:
    print('compress(' + str(args['compress']) + ')')
    print('  -> ' + str(compress(args['compress'])))
  if 'diff' in args and ':' in args['diff']:
    parts = args['diff'].split(':')
    parts[0] = parts[0].split(',')
    parts[1] = parts[1].split(',')
    print('diff(' + str(parts[0]) + ':' + str(parts[1]) + ')')
    print('  -> ' + str(diff(parts[0], parts[1])))
  if 'intersect' in args and ':' in args['intersect']:
    parts = args['intersect'].split(':')
    parts[0] = parts[0].split(',')
    parts[1] = parts[1].split(',')
    print('intersect(' + str(parts[0]) + ':' + str(parts[1]) + ')')
    print('  -> ' + str(intersect(parts[0], parts[1])))
