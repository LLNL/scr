#! /usr/bin/env python

import os, sys

launcher='srun'
prog='scr_'+launcher

libdir='@X_LIBDIR@'
bindir='@X_BINDIR'

if len(sys.argv)==1:
  print('USAGE:')
  print('scr_$launcher [$launcher args] [-rc|--run-cmd=<run_command>] 
[-rs|--restart-cmd=<restart_command>] [$launcher args]"
  print('<run_command>: The command to run when no restart file is present"
  print('<restart_command>: The command to run when a restart file is present"
  print('"
  print('The invoked command will be \`$launcher [${launcher} args] 
[run_command]\` when no restart file is present"
  print('The invoked command will be \`$launcher [${launcher} args] 
[restart_command]\` when a restart file is present"
  print('If the string \"SCR_CKPT_NAME\" appears in the restart command, it will 
be replace by the name "
  print('presented to SCR when the most recent checkpoint was written."
  print('"
  print('If no restart command is specified, the run command will always be 
used"
  print('If no commands are specified, the $launcher arguments will be passed 
directly to $launcher in all circumstances"
  print('If no run command is specified, but a restart command is specified,"
  print('then the restart command will be appended to the $launcher arguments 
when a restart file is present."
  sys.exit(0)
