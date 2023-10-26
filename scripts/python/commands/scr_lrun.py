#! /usr/bin/env python3

# calls scr_run specifying to use launcher 'lrun'
# the launcher object could be created here.

# add path holding scrjob to PYTHONPATH
import sys
sys.path.insert(0, '@X_LIBEXECDIR@/python')

from scrjob.scr_run import *


if __name__ == '__main__':
    # just printing help, print the help and exit
    if len(sys.argv) < 3 or '-h' in sys.argv[1:] or '--help' in sys.argv[1:]:
        print_usage('lrun')
    elif not any(
            arg.startswith('-h') or arg.startswith('--help')
            or arg.startswith('-rc') or arg.startswith('--run-cmd')
            or arg.startswith('-rs') or arg.startswith('--restart-cmd')
            for arg in sys.argv):
        # then we were called with: scr_srun [launcher args]
        scr_run(launcher='lrun', launcher_args=sys.argv[1:])
    else:
        launcher, launcher_args, run_cmd, restart_cmd, restart_args = parseargs(
            sys.argv[1:], launcher='lrun')
        scr_run(launcher='lrun',
                launcher_args=launcher_args,
                run_cmd=run_cmd,
                restart_cmd=restart_cmd,
                restart_args=restart_args)
