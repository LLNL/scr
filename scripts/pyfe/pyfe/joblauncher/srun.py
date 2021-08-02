#! /usr/bin/env python3

#srun.py
# The SRUN class provides interpretation for the srun launcher

from time import sleep

from pyfe import scr_const
from pyfe.scr_common import runproc, pipeproc
from pyfe.joblauncher import JobLauncher


class SRUN(JobLauncher):
  def __init__(self, launcher='srun'):
    super(SRUN, self).__init__(launcher=launcher)
    # The Popen.kill() seems to work for srun
    if self.flux is not None or scr_const.USE_JOBLAUNCHER_KILL == '1':
      self.watchprocess = True

  # a command to run immediately before prerun is ran
  # NOP srun to force every node to run prolog to delete files from cache
  # TODO: remove this if admins find a better place to clear cache
  def prepareforprerun(self):
    # NOP srun to force every node to run prolog to delete files from cache
    # TODO: remove this if admins find a better place to clear cache
    argv = ['srun', '/bin/hostname']  # ,'>','/dev/null']
    runproc(argv=argv)

  # returns the process and PID of the launched process
  # as returned by runproc(argv=argv, wait=False)
  def launchruncmd(self, up_nodes='', down_nodes='', launcher_args=[]):
    if type(launcher_args) is str:
      launcher_args = launcher_args.split()
    if len(launcher_args) == 0:
      return None, -1
    argv = [self.launcher]
    if len(down_nodes) > 0:
      argv.extend(['--exclude', down_nodes])
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

  # query SLURM for the most recent jobstep in current allocation
  def get_jobstep_id(self, user='', allocid='', pid=-1):
    # allow launched job to show in squeue
    sleep(10)
    if user == '' or allocid == '':
      return None

    # get job steps for this user and job, order by decreasing job step
    # so first one should be the one we are looking for
    #   squeue -h -s -u $user -j $jobid -S "-i"
    # -h means print no header, so just the data in this order:
    # STEPID         NAME PARTITION     USER      TIME NODELIST
    cmd = "squeue -h -s -u " + user + " -j " + allocid + " -S \"-i\""
    output = runproc(cmd, getstdout=True)[0]
    output = re.search('\d+', output)
    if output is None:
      return None
    return output[0]

  def scr_kill_jobstep(self, jobstepid=None):
    if jobstepid is not None:
      runproc(argv=['scancel', jobstepid])
