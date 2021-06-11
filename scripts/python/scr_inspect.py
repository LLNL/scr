#! /usr/bin/env python

# scr_inspect.py

# This file runs scr_inspect_cache on each node using pdsh.
# It lists the ids of all cached datasets for which there
# may be a chance to flush and rebuild data.

# requires: pdsh

import os, re, subprocess, sys
from scr_common import getconf
from scr_env import SCR_Env
import scr_const, scr_hostlist

def print_usage(prog):
  print('')
  print('  Usage:  '+prog+' [--up <nodeset>] --from <cntldir>')
  print('')

def scr_inspect(argv,scr_env=None):
  bindir = scr_const.X_BINDIR
  prog = 'scr_inspect'

  pdsh = scr_const.PDSH_EXE

  if scr_env is None:
    scr_env = SCR_Env()

  # tag output files with jobid
  jobid = scr_enev.getjobid()
  if jobid is None:
    print(prog+': ERROR: Could not determine jobid.')
    return 1

  # read node set of job
  jobset = scr_env.get_job_nodes()
  if jobset is None:
    print(prog+': ERROR: Could not determine nodeset.')
    return 1

  # read in command line arguments
  conf = getconf(argv,{'-j':'nodeset_job','--jobset':'nodeset_job','-u':'nodeset_up','--up':'nodeset_up','-d':'nodeset_down','--down':'nodeset_down','-f':'dir_from','--from':'dir_from'},{'-v':'verbose','--verbose':'verbose'})
  if conf is None:
    print_usage(prog)
    return 1

  if 'dir_from' not in conf:
    print(prog+': ERROR: Control directory must be specified.')
    return 1

  # get directories
  cntldir = conf['dir_from']

  # get nodesets
  jobnodes  = scr_hostlist.expand(conf['nodeset_job'])
  upnodes   = []
  downnodes = []
  if 'nodeset_down' in conf:
    downnodes = scr_hostlist.expand(conf['nodeset_down'])
    upnodes   = scr_hostlist.diff(jobnodes, downnodes)
  elif 'nodeset_up' in conf:
    upnodes   = scr_hostlist.expand(conf['nodeset_up'])
    downnodes = scr_hostlist.diff(jobnodes, upnodes)
  else:
    upnodes = jobnodes

  # format up and down node sets for pdsh command
  upnodes   = scr_hostlist.compress(upnodes)

  # TODO: should check that .scr direcotry exists
  # build the output filenames
  pwd = os.getcwd()
  output = pwd+'/.scr/'+prog+'.pdsh.o.'+jobid
  error  = pwd+'/.scr/'+prog+'.pdsh.e.'+jobid
  try:
    os.remove(output)
  except:
    pass
  try:
    os.remove(error)
  except:
    pass

  # run scr_inspect_cache via pdsh
  filemap = cntldir+'/filemap.scrinfo'
  cmd = bindir+'/scr_inspect_cache '+filemap

  argv = [pdsh,'-f','256','-S','-w',upnodes,cmd]
  runproc = subprocess.Popen(argv,bufsize=1,stdin=None,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell = True, universal_newlines = False)
  with
  `$pdsh -f 256 -S -w '$upnodes'  "$cmd"  >$output 2>$error`;
  out = runproc.communicate()
  with open(output,'w') as outfile:
    outfile.write(out[0])
  with open(error,'w') as errfile:
    errfile.write(out[1])

  # scan output file for list of partners and failed copies
  groups = {}
  types = {}

  # open the file, exit with error if we can't
  readout=False
  with open(output,'r') as infile:
    readout=True
    for line in infile.readlines():
      line=line.rstrip()
      search = re.search(r'DSET=(\d+) RANK=(\d+) TYPE=(\w+) GROUPS=(\d+) GROUP_ID=(\d+) GROUP_SIZE=(\d+) GROUP_RANK=(\d+)',line)
      if search is not None:
        dset = search.group(1)
        rank = search.group(2)
        atype = search.group(3)
        ngroups = search.group(4)
        group_id = search.group(5)
        group_size = search.group(6)
        group_rank = search.group(7)
        if dset not in groups:
          groups[dset] = {}
        if 'ids' not in groups[dset]:
          groups[dset]['ids'] = {}
        if group_id not in groups[dset]['ids']:
          groups[dset]['ids'][group_id] = {}
        if 'ranks' not in groups[dset]['ids'][group_id]:
          groups[dset]['ids'][group_id]['ranks'] = {}
        groups[dset]['ids'][group_id]['ranks'][group_rank] = 1
        groups[dset]['ids'][group_id]['size'] = group_size
        groups[dset]['groups'] = ngroups
        types[dset] = atype

  if readout==False:
    print(prog+': ERROR: Unable to read output file \"'+output+'\"')
    return 1

  # starting with the most recent dataset, check whether we have (or may be able to recover) all files
  possible_dsets = []
  dsets = list(groups.keys())
  dsets.sort()
  for dset in dsets:
    # get the expected number of groups and the dataset type
    expected_groups = groups[dset]['groups']
    atype = types[dset]

    # count the number of groups we find, and the number we can't recover
    num_groups = 0
    missing_groups = 0
    sortedids = list(groups[dset]['ids'].keys())
    sortedids.sort()
    for group in sortedids:
      # add this group to our running total, and get its size
      num_groups+=1
      group_size = groups[dset]['ids'][group]['size']

      # count the number of ranks we're missing from this dataset
      missing_ranks = []
      for i in range(group_size):
        if i not in groups[dset]['ids'][group]['ranks']:
          missing_ranks.append(i)

      # determine whether we are missing too many ranks from this group based on the dataset type
      missing_too_many = False
      if (type == 'LOCAL' or type == 'PARTNER') and len(missing_ranks)>0:
        missing_too_many = True
      elif type == 'XOR' and len(missing_ranks)>1:
        missing_too_many = True

      # if we're missing too many ranks from this group, add it to the total
      if missing_too_many==True:
        missing_groups+=1

    # if we have a chance to recover files from all groups of this dataset, add it to the list
    if num_groups == expected_groups and missing_groups == 0:
      possible_dsets.append(dset)

  # failed to find a full dataset to even attempt
  if len(possible_dsets)==0:
    return 1

  # return the list the datasets we have a shot of recovering
  return possible_dsets


if __name__=='__main__':
  ret = scr_inspect(sys.argv[1:])
  print('scr_inspect returned '+str(ret))

