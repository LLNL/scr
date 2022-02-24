#! /usr/bin/env python3

# mpirun.py
# The MPIRUN class provides interpretation for the mpirun launcher

import os
from pyfe import scr_hostlist, scr_const
from pyfe.launchers import JobLauncher
from pyfe.scr_common import runproc, pipeproc


class MPIRUN(JobLauncher):
  def __init__(self, launcher='mpirun'):
    super(MPIRUN, self).__init__(launcher=launcher)

  def get_hostfile_hosts(self, downlist=[]):
    # get the file name and read the file
    val = os.environ.get('LSB_DJOB_HOSTFILE')
    if val is None:
      return None
    # LSB_HOSTS would be easier, but it gets truncated at some limit
    # only reliable way to build this list is to process file specified
    # by LSB_DJOB_HOSTFILE
    hosts = []
    try:
      # got a file, try to read it
      with open(val, 'r') as hostfile:
        hosts = [line.strip() for line in hostfile.readlines()]
      if len(hosts) == 0:
        raise ValueError('Hostfile empty')
    except:
      return None
    # build set of unique hostnames, one hostname per line
    hosts = list(set(hosts))
    ### if there could possibly be an empty line in the file
    hosts = [host for host in hosts if host != '']
    return hosts

  # returns the process and PID of the launched process
  # as returned by runproc(argv=argv, wait=False)
  def launchruncmd(self, up_nodes='', down_nodes='', launcher_args=[]):
    if type(launcher_args) is str:
      launcher_args = launcher_args.split()
    if len(launcher_args) == 0:
      return None, -1
    target_hosts = self.get_hostfile_hosts(
        downlist=scr_hostlist.expand(down_nodes))
    try:
      # need to first ensure the directory exists
      basepath = '/'.join(self.hostfile.split('/')[:-1])
      os.makedirs(basepath, exist_ok=True)
      with open(self.hostfile, 'w') as hostfile:
        print(','.join(target_hosts), file=self.hostfile)
      argv = [self.launcher, '--hostfile', self.hostfile]
      argv.extend(launcher_args)
      return runproc(argv=argv, wait=False)
    except Exception as e:
      print(e)
      print('scr_mpirun: Error writing hostfile and creating launcher command')
      print('launcher file: \"' + self.hostfile + '\"')
      #return None, -1
      ### We could just do -N 1 to run a single process on every node of the allocation?
      argv = [self.launcher, '-N', '1']
      argv.extend(launcher_args)
      return runproc(argv=argv, wait=False)

  # perform a generic pdsh / clustershell command
  # returns [ [ stdout, stderr ] , returncode ]
  def parallel_exec(self, argv=[], runnodes='', use_dshbak=True):
    if len(argv) == 0:
      return [['', ''], 0]
    if self.clustershell_task != False:
      return self.clustershell_exec(argv=argv,
                                    runnodes=runnodes,
                                    use_dshbak=use_dshbak)
    pdshcmd = [scr_const.PDSH_EXE, '-Rexec', '-f', '256', '-S', '-w', runnodes]
    pdshcmd.extend(argv)
    if use_dshbak:
      argv = [pdshcmd, [scr_const.DSHBAK_EXE, '-c']]
      return pipeproc(argvs=argv, getstdout=True, getstderr=True)
    return runproc(argv=pdshcmd, getstdout=True, getstderr=True)

  # perform the scavenge files operation for scr_scavenge
  # uses either pdsh or clustershell
  # returns a list -> [ 'stdout', 'stderr' ]
  def scavenge_files(self,
                     prog='',
                     upnodes='',
                     downnodes_spaced='',
                     cntldir='',
                     dataset_id='',
                     prefixdir='',
                     buf_size='',
                     crc_flag=''):
    argv = [
        prog, '--cntldir', cntldir, '--id', dataset_id, '--prefix', prefixdir,
        '--buf', buf_size, crc_flag, downnodes_spaced
    ]
    output = self.parallel_exec(argv=argv, runnodes=upnodes,
                                use_dshbak=False)[0]
    return output
