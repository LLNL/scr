#! /usr/bin/env python

# scr_halt.py
# (from scripts/SLURM)

import os, sys
from scr_common import getconf

def print_usage(prog):
  print('  '+prog+' -- set or modify halt conditions for an SCR job')
  print('')
  print('  Usage:  '+prog+' [options] [prefixdir ...]')
  print('')
  print('  Options:')
  print('    -c, --checkpoints=N')
  print('          Halt job after N checkpoints.')
  print('    -b, --before=TIME')
  print('          Halt job before specified TIME.  Uses SCR_HALT_SECONDS if set.')
  print('    -a, --after=TIME')
  print('          Halt job after specified TIME.')
  print('    -i, --immediate')
  print('          Halt job immediately.')
  print('    -s, --seconds=N')
  print('          Set or reset SCR_HALT_SECONDS for active job.')
  print('')
  print('    -l, --list')
  print('          List the current halt conditions specified for a job or jobs.')
  print('')
  print('    --unset-checkpoints')
  print('          Unset any checkpoint halt condition.')
  print('    --unset-before')
  print('          Unset any halt before condition.')
  print('    --unset-after')
  print('          Unset halt after condition.')
  print('    --unset-seconds')
  print('          Unset halt seconds.')
  print('    --unset-reason')
  print('          Unset the current halt reason.')
  print('')
  print('    -r, --remove')
  print('          Remove halt file.')
  print('')
  print('    -v, --verbose')
  print('          Increase verbosity.')
  print('    -h, --help')
  print('          Print usage.')
  print('')
  print('  TIME arguments are parsed using the perl Date::Manip(3pm) package, and thus')
  print('  may be specified in one of many formats. Examples include \'12pm\',')
  print('  \'yesterday,noon\', \'12/25-15:30:33\', and so on. See the Date::Manip(3pm)')
  print('  manpage for more examples.')
  print('')
  print('  If no directory is specified, the current working directory is used.')
  print('')

def Date_Init():
  return ''

def scr_halt(argv):
  # requires: squeue, scontrol, scancel, umask (shell command)

  prog='scr_halt'
  # use absolute paths to internal commands
  bash  = "/bin/bash";
  mkdir = "/bin/mkdir";
  rm    = "/bin/rm";
  echo  = "/bin/echo";
  umask = "umask"; # shell command

  conf = getconf(argv,{'-c':'checkpoints','--checkpoints':'checkpoints','-b':'before','--before':'before','-a':'after','--after':'after','-s':'seconds','--seconds':'seconds'},{'-i':'immediate','--immediate':'immediate','-l':'dolist','--list':'dolist','--unset-checkpoints':'unset_checkpoints','--unset-before':'unset_before','--unset-after':'unset_after','--unset-seconds':'unset_seconds','--unset-reason':'unset_reason','-r':'remove','--remove':'remove','-v':'verbose','--verbose':'verbose','-h':'help','--help':'help'},strict=False)
  if conf is None or 'help' in conf:
    print_usage(prog)
    return 0

  # Initialize Date::Manip
  Date_Init();

  # get the directories
  dirs = []
  if 'argv' in conf:
    # if find some arguments on the command line, assume they are target directories
    dirs = conf['argv']
  else:
    # use current working directory if none specified
    dirs.append(os.getcwd)

  ret = 0;

  # commands to build halt file
  halt_conditions = []

  # halt after X checkpoints
  checkpoints_left = None
  if 'checkpoints' in conf:
    # TODO: check that a valid value was given
    halt_conditions.append('-c '+conf['checkpoints'])

  # halt before time
  if 'before' in conf:
    date = ParseDate(conf['before'])
    if date is None:
      print(prog+': ERROR: Invalid time specified in --before: '+conf['before'])
      return 1
    secs = UnixDate(date,"%s");
    #  print "$prog: Exit before: " . localtime($secs) . "\n";
    halt_conditions.append('-b '+secs)

  # halt after time
  if 'after' in conf:
    date = ParseDate(conf['after'])
    if date is None:
      print(prog+': ERROR: Invalid time specified in --after: '+conf['after'])
      return 1
    secs = UnixDate(date,"%s");
    #  print "$prog: Exit after: " . localtime($secs) . "\n";
    halt_conditions.append('-a '+secs)

  # set (reset) SCR_HALT_SECONDS value
  if 'seconds' in conf:
    halt_seconds = conf['seconds']
    # TODO: check that a valid value was given
    halt_conditions.append('-s '+halt_seconds)

  # list halt options
  if 'list' in conf:
    halt_conditions.append('-l')

  # push options to unset any values
  if 'unset_checkpoints' in conf:
    halt_conditions.append('-xc')
  if 'unset_before' in conf:
    halt_conditions.append('-xb')
  if 'unset_after' in conf:
    halt_conditions.append('-xa')
  if 'unset_seconds' in conf:
    halt_conditions.append('-xs')
  if 'unset_reason' in conf:
    halt_conditions.append('-xr')

  # if we were not given any conditions, set the exit reason to JOB_HALTED
  if len(halt_conditions)==0 or 'immediate' in conf:
    halt_conditions.append('-r JOB_HALTED')

  # the -r option overrides everything else
  if 'remove' in conf:
    halt_conditions = []

  # create a halt file on each node
  for adir in dirs:
    rc = 0

    print('Updating halt file in '+adir)

    # build the name of the halt file
    halt_file = adir+'/.scr/halt.scr'

    # TODO: Set halt file permissions so system admins can modify them
    halt_cmd = ''
    if len(halt_conditions)>0:
      # create the halt file with specified conditions
      halt_file_options = halt_conditions.join(' ')
      #    $halt_cmd = "$bash -c \"$mkdir -p $dir/.scr; $bindir/scr_halt_cntl -f $halt_file $halt_file_options;\"";
      halt_cmd = bash+' -c \"'+bindir+'/scr_halt_cntl -f '+halt_file+' '+halt_file_options
    else:
      # remove the halt file
      halt_cmd = bash+' -c \"'+rm+' -f '+halt_file

    # execute the command
    if 'verbose' in conf:
      print(halt_cmd)
    output = halt_cmd # RUN HALT CMD
    rc = 0 # rc = return code
    if rc!=0:
      print('')
      print(prog+': ERROR: Failed to update halt file for '+adir)
      ret = 1

    # print output to screen
    print(output)

  # TODO: would like to protect against killing a job in the middle of a checkpoint if possible

  # kill job if immediate was set
  if 'immediate' in conf:
    # TODO: lookup active jobid for given prefix directory and halt job based on system
    print(prog+': ERROR: --immediate option not yet supported')
    ret = 1

  return ret

if __name__=='__main__':
  ret = scr_halt(sys.argv[1:])
  print('scr_halt returned '+str(ret))

