"""This script can use the 'scancel' or equivalent command to kill a jobstep
with the jobstep id supplied via the command line.

This requires specifying both the joblauncher and a jobstep id.
"""

import argparse

from scrjob.launchers import AutoJobLauncher

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-b',
                        '--bindir',
                        metavar='<bindir>',
                        default=None,
                        help='Specify the bin directory.')
    parser.add_argument('-l',
                        '--launcher',
                        metavar='<launcher>',
                        default=None,
                        required=True,
                        help='Specify the job launcher.')
    parser.add_argument('-j',
                        '--jobStepId',
                        metavar='<jobstepid>',
                        type=str,
                        required=True,
                        help='The job step id to kill.')

    args = parser.parse_args()

    launcher = AutoJobLauncher(args.launcher)
    print('Joblauncher:')
    print(str(type(launcher)))

    print('Jobstep id: ' + args.jobStepId)

    print('Calling launcher.kill_jobstep . . .')
    launcher.kill_jobstep(jobstep=args.jobStepId)
