#! /usr/bin/env python3

# scr_srun.py

import os, sys

if 'scrjob' not in sys.path:
  sys.path.insert(0, '/'.join(os.path.realpath(__file__).split('/')[:-2]))
  import scrjob

from scrjob.scr_run import *

if __name__ == '__main__':
  """scr_srun.py calls scr_run.py with the launcher 'srun'

  These commands are equivalent:
    scr_run.py srun <launcher args> <launch cmd> <cmd args>
    scr_srun.py <launcher args> <launch cmd> <cmd args>
  """
  # just printing help, print the help and exit
  if len(sys.argv) < 3 or '-h' in sys.argv[1:] or '--help' in sys.argv[1:]:
    print_usage('srun')
  elif not any(
      arg.startswith('-h') or arg.startswith('--help') or arg.startswith('-rc')
      or arg.startswith('--run-cmd') or arg.startswith('-rs')
      or arg.startswith('--restart-cmd') for arg in sys.argv):
    # then we were called with: scr_srun [launcher args]
    scr_run(launcher='srun', launcher_args=sys.argv[1:])
  else:
    launcher, launcher_args, run_cmd, restart_cmd, restart_args = parseargs(
        sys.argv[1:], launcher='srun')
    scr_run(launcher='srun',
            launcher_args=launcher_args,
            run_cmd=run_cmd,
            restart_cmd=restart_cmd,
            restart_args=restart_args)
