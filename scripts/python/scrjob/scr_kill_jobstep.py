#! @Python_EXECUTABLE@

# scr_kill_jobstep.py

"""
This script can use the 'scancel' or equivalent command
to kill a jobstep with the jobstep id supplied via the command line.

This requires specifying both the joblauncher and a jobstep id.
"""

import os, sys

if 'scrjob' not in sys.path:
  sys.path.insert(0, '/'.join(os.path.realpath(__file__).split('/')[:-2]))
  import scrjob

import argparse
from scrjob import scr_const
from scrjob.scr_common import runproc
from scrjob.launchers import AutoJobLauncher

if __name__ == '__main__':
  parser = argparse.ArgumentParser(add_help=False,
                                   argument_default=argparse.SUPPRESS,
                                   prog='scr_kill_jobstep')
  parser.add_argument('-h',
                      '--help',
                      action='store_true',
                      help='Show this help message and exit.')
  parser.add_argument('-b',
                      '--bindir',
                      metavar='<bindir>',
                      default=None,
                      help='Specify the bin directory.')
  parser.add_argument('-l',
                      '--launcher',
                      metavar='<launcher>',
                      default=None,
                      help='Specify the job launcher.')
  parser.add_argument('-j',
                      '--jobStepId',
                      metavar='<jobstepid>',
                      type=str,
                      help='The job step id to kill.')
  args = vars(parser.parse_args())
  if 'help' in args:
    parser.print_help()
  elif 'jobStepId' not in args:
    print('You must specify the job step id to kill.')
  elif 'launcher' not in args:
    print('You must specify the job launcher used to launch the job.')
  else:
    launcher = AutoJobLauncher(args['launcher'])
    print('Joblauncher:')
    print(str(type(launcher)))
    print('Jobstep id: ' + args['jobStepId'])
    print('Calling launcher.scr_kill_jobstep . . .')
    launcher.scr_kill_jobstep(jobstep=args['jobStepId'])
