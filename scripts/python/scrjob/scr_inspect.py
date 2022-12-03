#! @Python_EXECUTABLE@

# scr_inspect.py

import os, sys

if 'scrjob' not in sys.path:
  sys.path.insert(0, '/'.join(os.path.realpath(__file__).split('/')[:-2]))
  import scrjob

import argparse, re, subprocess
from scrjob import scr_const, scr_hostlist
from scrjob.scr_environment import SCR_Env
from scrjob.resmgrs import AutoResourceManager
from scrjob.launchers import AutoJobLauncher


def scr_inspect(jobnodes=None, up=None, down=None, cntldir=None, scr_env=None):
  """this method runs scr_inspect_cache on each node using Joblauncher.parallel_exec

  Returns
  -------
  string   A space separated list of cached datasets which may be able to flush and rebuild
  
  On error this method returns the integer 1
  """
  bindir = scr_const.X_BINDIR
  pdsh = scr_const.PDSH_EXE

  if scr_env is None:
    scr_env = SCR_Env()
  if scr_env.resmgr is None:
    scr_env.resmgr = AutoResourceManager()
  if scr_env.launcher is None:
    scr_env.launcher = AutoJobLauncher()

  # tag output files with jobid
  jobid = scr_env.getjobid()
  if jobid is None:
    print('scr_inspect: ERROR: Could not determine jobid.')
    return 1

  # read node set of job
  jobset = scr_env.get_scr_nodelist()
  if jobset is None:
    jobset = scr_env.resmgr.get_job_nodes()
    if jobset is None:
      print('scr_inspect: ERROR: Could not determine nodeset.')
      return 1

  # can't get directories
  if cntldir is None:
    print('scr_inspect: ERROR: Control directory must be specified.')
    return 1

  # get nodesets
  if jobnodes is None:
    print('scr_inspect: ERROR: Job nodes must be specified.')
    return 1

  jobnodes = scr_hostlist.expand(jobnodes)
  upnodes = []
  downnodes = []
  if down is not None:
    downnodes = scr_hostlist.expand(down)
    upnodes = scr_hostlist.diff(jobnodes, downnodes)
  elif up is not None:
    upnodes = scr_hostlist.expand(up)
    downnodes = scr_hostlist.diff(jobnodes, upnodes)
  else:
    upnodes = jobnodes

  # make the list a comma separated string
  upnodes = ','.join(upnodes)

  # build the output filenames
  pwd = os.getcwd()
  os.makedirs(pwd + '/.scr', exist_ok=True)
  output = pwd + '/.scr/scr_inspect.pdsh.o.' + jobid
  error = pwd + '/.scr/scr_inspect.pdsh.e.' + jobid

  # run scr_inspect_cache via pdsh / clustershell
  argv = [bindir + '/scr_inspect_cache', cntldir + '/filemap.scrinfo']
  out = scr_env.launcher.parallel_exec(argv=argv,
                                       runnodes=upnodes)[0]
  try:
    with open(output, 'w') as outfile:
      outfile.write(out[0])
  except Exception as e:
    print(e)
    print('scr_inspect: ERROR: Error writing scr_inspect_cache stdout')
  try:
    with open(error, 'w') as errfile:
      errfile.write(out[1])
  except Exception as e:
    print(e)
    print('scr_inspect: ERROR: Error writing scr_inspect_cache stderr')

  # scan output file for list of partners and failed copies
  groups = {}
  types = {}

  # open the file, exit with error if we can't
  readout = False
  try:
    with open(output, 'r') as infile:
      readout = True
      for line in infile.readlines():
        line = line.rstrip()
        search = re.search(
            r'DSET=(\d+) RANK=(\d+) TYPE=(\w+) GROUPS=(\d+) GROUP_ID=(\d+) GROUP_SIZE=(\d+) GROUP_RANK=(\d+)',
            line)
        if search is not None:
          dset = int(search.group(1))
          rank = int(search.group(2))
          atype = search.group(3)
          ngroups = int(search.group(4))
          group_id = int(search.group(5))
          group_size = int(search.group(6))
          group_rank = int(search.group(7))
          if dset not in groups:
            groups[dset] = {}
            groups[dset]['ids'] = {}
          if group_id not in groups[dset]['ids']:
            groups[dset]['ids'][group_id] = {}
            groups[dset]['ids'][group_id]['ranks'] = {}
          groups[dset]['ids'][group_id]['ranks'][group_rank] = 1
          groups[dset]['ids'][group_id]['size'] = group_size
          groups[dset]['groups'] = ngroups
          types[dset] = atype
  except Exception as e:
    print(e)
    print('scr_inspect: ERROR: Reading and processing output file \"' +
          output + '\"')
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
      num_groups += 1
      group_size = groups[dset]['ids'][group]['size']

      # count the number of ranks we're missing from this dataset
      missing_ranks = []
      for i in range(group_size):
        if i not in groups[dset]['ids'][group]['ranks']:
          missing_ranks.append(i)

      # determine whether we are missing too many ranks from this group based on the dataset type
      missing_too_many = False
      if (type == 'LOCAL' or type == 'PARTNER') and len(missing_ranks) > 0:
        missing_too_many = True
      elif type == 'XOR' and len(missing_ranks) > 1:
        missing_too_many = True

      # if we're missing too many ranks from this group, add it to the total
      if missing_too_many == True:
        missing_groups += 1

    # if we have a chance to recover files from all groups of this dataset, add it to the list
    if num_groups == expected_groups and missing_groups == 0:
      possible_dsets.append(dset)

  # failed to find a full dataset to even attempt
  if len(possible_dsets) == 0:
    return 1

  # return the list the datasets we have a shot of recovering
  return ' '.join(possible_dsets)


if __name__ == '__main__':
  parser = argparse.ArgumentParser(
      add_help=False,
      argument_default=argparse.SUPPRESS,
      prog='scr_inspect',
      epilog=
      'The jobid and job node set must be able to be obtained from the environment.'
  )
  parser.add_argument('-h',
                      '--help',
                      action='store_true',
                      help='Show this help message and exit.')
  parser.add_argument('-j',
                      '--jobset',
                      default=None,
                      metavar='<nodeset>',
                      type=str,
                      help='Job nodes.')
  parser.add_argument('-u',
                      '--up',
                      default=None,
                      metavar='<nodeset>',
                      type=str,
                      help='Up nodes.')
  parser.add_argument('-d',
                      '--down',
                      default=None,
                      metavar='<nodeset>',
                      type=str,
                      help='Down nodes.')
  parser.add_argument('-f',
                      '--from',
                      default=None,
                      metavar='<ctrl dir>',
                      type=str,
                      help='Control directory.')
  args = vars(parser.parse_args())
  if 'help' in args:
    parser.print_help()
  elif args['jobset'] is None or args['from'] is None:
    parser.print_help()
    print('Job nodes and control directory must be specified.')
  else:
    ret = scr_inspect(jobnodes=args['jobset'],
                      up=args['up'],
                      down=args['down'],
                      cntldir=args['from'])
    print('scr_inspect returned ' + str(ret))
