#! /usr/bin/env python3

# add path holding scrjob to PYTHONPATH
import sys

sys.path.insert(0, '@X_LIBEXECDIR@/python')

from scrjob.run import run, parseargs, validate_launcher, print_usage

# argparse doesn't handle parsing of mixed positionals
# there is a parse_intermixed_args which requires python 3.7 which may work
# I'm just going to skip the argparse
if __name__ == '__main__':
    # just printing help, print the help and exit
    if len(sys.argv) < 3 or '-h' in sys.argv[1:] or '--help' in sys.argv[1:]:
        print_usage()
    elif not any(
            arg.startswith('-rc') or arg.startswith('--run-cmd')
            or arg.startswith('-rs') or arg.startswith('--restart-cmd')
            for arg in sys.argv):
        # then we were called with: scr_run launcher [args]
        launcher = sys.argv[1]
        validate_launcher(launcher)
        run(launcher, launcher_args=sys.argv[2:])
    else:
        launcher, launcher_args, run_cmd, restart_cmd, restart_args = parseargs(
            sys.argv[1:])
        #if launcher=='flux', remove 'mini' 'submit' 'run' from front of args
        run(launcher_args=launcher_args,
            run_cmd=run_cmd,
            restart_cmd=restart_cmd,
            restart_args=restart_args)
