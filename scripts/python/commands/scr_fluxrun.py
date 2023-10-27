#! /usr/bin/env python3

# add path holding scrjob to PYTHONPATH
import sys

sys.path.insert(0, '@X_LIBEXECDIR@/python')

from scrjob.run import run, parseargs, print_usage

if __name__ == '__main__':
    # just printing help, print the help and exit
    if len(sys.argv) < 3 or '-h' in sys.argv[1:] or '--help' in sys.argv[1:]:
        print_usage('flux')
    elif not any(
            arg.startswith('-rc') or arg.startswith('--run-cmd')
            or arg.startswith('-rs') or arg.startswith('--restart-cmd')
            for arg in sys.argv):
        # then we were called with: scr_fluxrun [launcher args]
        run(launcher='flux', launcher_args=sys.argv[1:])
    else:
        # remove prepended flux args which have no use for us
        argv = sys.argv[1:]
        if argv[0] == 'mini':
            argv = argv[1:]
        if argv[0] == 'run' or argv[0] == 'submit':
            argv = argv[1:]
        launcher, launcher_args, run_cmd, restart_cmd, restart_args = parseargs(
            argv, launcher='flux')
        run(launcher='flux',
            launcher_args=launcher_args,
            run_cmd=run_cmd,
            restart_cmd=restart_cmd,
            restart_args=restart_args)
