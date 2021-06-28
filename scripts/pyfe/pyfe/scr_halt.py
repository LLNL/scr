#! /usr/bin/env python3

# scr_halt.py
# (from scripts/SLURM)

import argparse, os, sys
from pyfe import scr_const
from pyfe.scr_common import runproc
from pyfe.parsetime import parsetime

def scr_halt(bindir=None,bash=None,mkdir=None,rm=None,echo=None,umask=None,checkpoints=None, before=None, after=None, immediate=False, seconds=None, dolist=False, unset_checkpoints=False, unset_before=False, unset_after=False, unset_seconds=False, unset_reason=False, remove=False, verbose=False, dirs=None):
  # requires: squeue, scontrol, scancel, umask (shell command)

  if bindir is None:
    bindir = scr_const.X_BINDIR
  # use absolute paths to internal commands
  if bash is None:
    bash  = '/bin/bash'
  if mkdir is None:
    mkdir = '/bin/mkdir'
  if rm is None:
    rm    = '/bin/rm'
  if echo is None:
    echo  = '/bin/echo'
  if umask is None:
    umask = 'umask' # shell command

  # get the directories
  # if find some arguments on the command line, assume they are target directories
  if dirs is None:
    # use current working directory if none specified
    dirs = [ os.getcwd ]

  ret = 0

  # commands to build halt file
  halt_conditions = []

  # the -r option overrides everything else
  if remove:
    pass
  else:
    # halt after X checkpoints
    checkpoints_left = None
    if checkpoints is not None:
      checkpoints = str(checkpoints)
      # TODO: check that a valid value was given
      halt_conditions = ['-c',checkpoints]

    # halt before time
    if before is not None:
      secs = parsetime(before).seconds
      #  print "$prog: Exit before: " . localtime($secs) . "\n";
      halt_conditions.append('-b')
      halt_conditions.append(str(secs))

    # halt after time
    if after is not None:
      secs = parsetime(after).seconds
      #  print "$prog: Exit after: " . localtime($secs) . "\n";
      halt_conditions.append('-a')
      halt_conditions.append(str(secs))

    # set (reset) SCR_HALT_SECONDS value
    if seconds is not None:
      # halt_seconds = seconds
      # TODO: check that a valid value was given
      halt_conditions.append('-s')
      halt_conditions.append(seconds)

    # list halt options
    if dolist:
      halt_conditions.append('-l')

    # push options to unset any values
    if unset_checkpoints:
      halt_conditions.append('-xc')
    if unset_before:
      halt_conditions.append('-xb')
    if unset_after:
      halt_conditions.append('-xa')
    if unset_seconds:
      halt_conditions.append('-xs')
    if unset_reason:
      halt_conditions.append('-xr')

    # if we were not given any conditions, set the exit reason to JOB_HALTED
    if len(halt_conditions)==0 or immediate:
      halt_conditions.append('-r')
      halt_conditions.append('JOB_HALTED')

  # create a halt file on each node
  for adir in dirs:
    rc = 0

    print('Updating halt file in '+adir)

    # build the name of the halt file
    halt_file = adir+'/.scr/halt.scr'

    # TODO: Set halt file permissions so system admins can modify them
    halt_cmd = []
    if len(halt_conditions)>0:
      # create the halt file with specified conditions
      halt_file_options = ' '.join(halt_conditions)
      # can specify a different bash with popen (?)
      halt_cmd = [bindir+'/scr_halt_cntl', '-f', halt_file]
      halt_cmd.extend(halt_file_options.split(' '))
      #halt_cmd = [bash,'-c','\"'+bindir+'/scr_halt_cntl -f '+halt_file+' '+halt_file_options+';\"']
    else:
      # remove the halt file
      #halt_cmd = [bash,' -c \"'+rm+' -f '+halt_file+'\"']
      try:
        os.remove(halt_file)
      except Exception as e:
        print(e)
        print('scr_halt: ERROR: Failed to remove the halt file \"'+str(halt_file)+'\"')
      return 0

    # execute the command
    #########################
    #if verbose:
    print(' '.join(halt_cmd))
    # RUN HALT CMD
    output, rc = runproc(halt_cmd,getstdout=True,getstderr=True)
    if rc!=0:
      print(output[1].strip())
      print('scr_halt: ERROR: Failed to update halt file for '+adir)
      ret = 1

    # print output to screen
    print(output[0].strip())

  # TODO: would like to protect against killing a job in the middle of a checkpoint if possible

  # kill job if immediate was set
  if immediate:
    # TODO: lookup active jobid for given prefix directory and halt job based on system
    print('scr_halt: ERROR: --immediate option not yet supported')
    ret = 1

  return ret

if __name__=='__main__':
  parser = argparse.ArgumentParser(add_help=False, argument_default=argparse.SUPPRESS, prog='scr_halt', epilog='TIME arguments are parsed using parsetime.py,\nand t may be specified in one of many formats.\nExamples include \'12pm\', \'yesterday noon\', \'12/25 15:30:33\', and so on.\nIf no directory is specified, the current working directory is used.')
  # when prefixes are unambiguous then also adding shortcodes isn't necessary
  parser.add_argument('-c', '--checkpoints', metavar='N', default=None, type=int, help='Halt job after N checkpoints.')
  parser.add_argument('-b', '--before', metavar='TIME', default=None, type=str, help='Halt job before specified TIME. Uses SCR_HALT_SECONDS if set.')
  parser.add_argument('-a', '--after', metavar='TIME', default=None, type=str, help='Halt job after specified TIME.')
  parser.add_argument('-i', '--immediate', action='store_true', default=False, help='Halt job immediately.')
  parser.add_argument('-s', '--seconds', metavar='N', default=None, type=str, help='Set or reset SCR_HALT_SECONDS for active job.')
  parser.add_argument('-l', '--list', action='store_true', default=False, help='List the current halt conditions specified for a job or jobs.')
  parser.add_argument('--unset-checkpoints', action='store_true', default=False, help='Unset any checkpoint halt condition.')
  parser.add_argument('--unset-before', action='store_true', default=False, help='Unset any halt before condition.')
  parser.add_argument('--unset-after', action='store_true', default=False, help='Unset halt after condition.')
  parser.add_argument('--unset-seconds', action='store_true', default=False, help='Unset halt seconds.')
  parser.add_argument('--unset-reason', action='store_true', default=False, help='Unset the current halt reason.')
  parser.add_argument('-r', '--remove', action='store_true', default=False, help='Remove halt file.')
  parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Increase verbosity.')
  parser.add_argument('-h','--help', action='store_true', help='Show this help message and exit.')
  parser.add_argument('dirs', nargs=argparse.REMAINDER, default=None)
  args = vars(parser.parse_args())
  if 'help' in args:
    parser.print_help()
  else:
    ret = scr_halt(checkpoints=args['checkpoints'], before=args['before'], after=args['after'], immediate=args['immediate'], seconds=args['seconds'], dolist=args['list'], unset_checkpoints=args['unset_checkpoints'], unset_before=args['unset_before'], unset_after=args['unset_after'], unset_seconds=args['unset_seconds'], unset_reason=args['unset_reason'], remove=args['remove'], verbose=args['verbose'], dirs=args['dirs'])
    print(str(ret))

