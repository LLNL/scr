# the general launcher for the scripts
# if called directly the launcher to use (srun/jsrun/mpirun) should be specified as an argument
# scr_{srun,jsrun,mpirun} scripts call this script with the launcher specified

# add path holding scrjob to PYTHONPATH
import sys

sys.path.insert(0, '@X_LIBEXECDIR@/python')

import os
from datetime import datetime
from time import time, sleep

from scrjob import config
from scrjob.list_down_nodes import list_down_nodes
from scrjob.common import tracefunction, runproc
from scrjob.prerun import prerun
from scrjob.postrun import postrun
from scrjob.should_exit import should_exit
from scrjob.watchdog import Watchdog
from scrjob.jobenv import JobEnv
from scrjob.cli import SCRLog, SCRRetriesHalt


def run(launcher='',
        launcher_args=[],
        run_cmd='',
        restart_cmd='',
        restart_args=[],
        verbose=False):
    prog = 'scr_' + launcher

    bindir = config.X_BINDIR

    val = os.environ.get('SCR_ENABLE')
    if val == '0':
        launcher = [launcher]
        launcher.extend(launcher_args)
        returncode = runproc(argv=launcher)[1]
        sys.exit(returncode)

    # turn on verbosity
    val = os.environ.get('SCR_DEBUG')
    if val is not None and int(val) > 0:
        verbose = True

        # turn on python function tracing
        if config.TRACE_FUNC:
            sys.settrace(tracefunction)

    # make a record of start time
    timestamp = datetime.now()
    start_secs = int(time())
    if verbose:
        print(prog + ': Started: ' + str(timestamp))

    # env contains general environment infos independent of resmgr/launcher
    jobenv = JobEnv(launcher=launcher)

    # this may be used by a launcher to store a list of hosts
    jobenv.launcher.hostfile = os.path.join(jobenv.dir_scr(), 'hostfile')

    # get prefix directory
    prefix = jobenv.dir_prefix()

    # jobid will come from resource manager.
    jobid = jobenv.resmgr.job_id()
    user = jobenv.user()

    # We need the jobid for logging, and need to be running within an allocation
    # for operations such as scavenge.  This test serves both purposes.
    if jobid is None:
        raise RuntimeError(f'No valid job ID or not in an allocation.')

    # create object to write log messages
    log = SCRLog(prefix, jobid, user=user, jobstart=start_secs)

    # get the nodeset of this job
    nodelist = jobenv.node_list()
    if not nodelist:
        nodelist = jobenv.resmgr.job_nodes()
    if not nodelist:
        raise RuntimeError(f'Could not identify nodeset for job {jobid}')

    watchdog = None
    val = os.environ.get('SCR_WATCHDOG')
    if val == '1':
        watchdog = Watchdog(prefix, jobenv)

    # make a record of time prerun is started
    timestamp = datetime.now()
    if verbose:
        print(prog + ': prerun: ' + str(timestamp))

    # test runtime, ensure filepath exists
    prerun(jobenv=jobenv, verbose=verbose)

    # look up allocation end time, record in SCR_END_TIME
    endtime = jobenv.resmgr.end_time()
    if endtime == 0:
        if verbose:
            print(prog + ': WARNING: Unable to get end time.')
    os.environ['SCR_END_TIME'] = str(endtime)

    # determine number of times to run application
    runs = os.environ.get('SCR_RUNS')
    if runs is None:
        runs = os.environ.get('SCR_RETRIES')
        if runs is None:
            runs = 0
        else:
            runs = int(runs)
        runs += 1
    else:
        runs = int(runs)

    # totalruns printed when runs are exhausted
    totalruns = str(runs)

    # the run loop breaks when runs hits zero (or some other conditions)
    # we can protect against an invalid input for number of runs here
    if runs < 1:
        runs = 1

    # enter the run loop
    finished = False
    down_nodes = []
    attempts = 0
    while True:
        # Once we mark a node as bad, leave it as bad (even if it comes back healthy).
        # This hacks around the problem of accidentally deleting a checkpoint set during distribute
        # when a relaunch lands on previously down nodes, which are healthy again.
        # A better way would be to remember the last set used, or to provide a utility to run on *all*
        # nodes to distribute files (also useful for defragging the machine) -- for now this works.
        keep_down = down_nodes

        # set "first run" flag, let individual tests decide how to handle that
        first_run = (attempts == 0)

        # check for any down nodes
        reasons = list_down_nodes(jobenv,
                                  nodes_down=keep_down,
                                  free=first_run,
                                  reason=True,
                                  log=log,
                                  secs='0')
        if verbose:
            for node in sorted(list(reasons.keys())):
                print(prog + ": FAILED: " + node + ': ' + reasons[node])

        down_nodes = sorted(list(reasons.keys()))

        # if this is the first run, we hit down nodes right off the bat, make a record of them
        if down_nodes and first_run and verbose:
            start_secs = int(time())
            down_str = ','.join(down_nodes)
            print('SCR: Failed node detected: JOBID=' + jobid +
                  ' ATTEMPT=0 TIME=' + str(start_secs) +
                  ' NNODES=-1 RUNTIME=0 FAILED=' + down_str)

        # check that we have enough nodes
        if should_exit(jobenv, down_nodes, verbose=verbose):
            if verbose:
                print(prog + ': Halt condition detected, ending run.')
            break

        launch_cmd = launcher_args.copy()
        if run_cmd != '':
            launch_cmd.append(run_cmd)

        # If restarting and restart_cmd is defined,
        # Run scr_have_restart on all nodes to rebuild checkpoint
        # and identify most recent checkpoint name.
        # Then replace SCR_CKPT_NAME with restart name in user's restart command.
        if restart_cmd != '':
            argv = [launcher]
            argv.extend(launcher_args)
            argv.append(os.path.join(bindir, 'scr_have_restart'))
            restart_name = runproc(argv=argv, getstdout=True)[0]
            if restart_name is not None and restart_name != '':
                restart_name = restart_name.strip()
                my_restart_cmd = re.sub('SCR_CKPT_NAME', restart_name,
                                        restart_cmd)
                launch_cmd = launcher_args.copy()
                launch_cmd.extend(my_restart_cmd.split(' '))

        # make a record of when each run is started
        attempts += 1
        timestamp = datetime.now()
        start_secs = int(time())
        log.event('RUN_START', note='run=' + str(attempts))
        if verbose:
            print(prog + ': RUN ' + str(attempts) + ': ' + str(timestamp))

        # launch the job, make sure we include the script node and exclude down nodes
        if verbose:
            print(prog + ': Launching ' + str(launch_cmd))
        proc, jobstep = jobenv.launcher.launch_run(launch_cmd,
                                                   down_nodes=down_nodes)

        if watchdog is None:
            finished, success = jobenv.launcher.wait_run(proc)
        else:
            if verbose:
                print(prog + ': Entering watchdog method')
            if watchdog.watchproc(proc, jobstep) != 0:
                # watchdog returned error or a watcher process was launched
                if verbose:
                    print(prog + ': Error launching watchdog')
                finished, success = jobenv.launcher.wait_run(proc)
            else:
                # watchdog returned because the process has finished/been killed
                #TODO: verify this really works for case where watchproc() returns 0 / succeeds
                finished, success = jobenv.launcher.wait_run(proc)

        # log stats on the latest run attempt
        end_secs = int(time())
        run_secs = end_secs - start_secs
        log.event('RUN_END', note='run=' + str(attempts), secs=str(run_secs))

        # run ended, check whether we should exit right away
        if should_exit(jobenv, down_nodes, verbose=verbose):
            if verbose:
                print(prog + ': Halt condition detected, ending run.')
            break

        # decrement retry counter
        runs -= 1
        if runs == 0:
            if verbose:
                print(prog + ': ' + totalruns +
                      ' launches exhausted, ending run.')
            break

        ##### Reduce this sleep time when testing scripts #####
        # give nodes a chance to clean up
        sleep_secs = 60
        if verbose:
            print(
                f'{prog} : Sleeping {sleep_secs} secs before relaunch to let system settle.'
            )
        sleep(sleep_secs)

    # make a record of time postrun is started
    timestamp = datetime.now()
    if verbose:
        print(prog + ': postrun: ' + str(timestamp))

    # scavenge files from cache to parallel file system
    postrun(jobenv=jobenv, verbose=verbose, log=log)

    # make a record of end time
    timestamp = datetime.now()
    if verbose:
        print(prog + ': Ended: ' + str(timestamp))


def print_usage(launcher=''):
    available_launchers = '[' + '/'.join(valid_launchers) + ']'
    print('USAGE:')
    # the original parsing in SLURM/scr_run.in combines all [launcher args]
    # (no matter whether they appear in the front or at the end)
    # the usage printing looks like you could define/usr different launcher args
    if launcher == '':
        launcher = available_launchers
        print(
            'scr_run <launcher> [' + launcher +
            ' args] [-rc|--run-cmd=<run_command>] [-rs|--restart-cmd=<restart_command>] ['
            + launcher + ' args]')
        print('<launcher>: The job launcher to use, one of ' + launcher)
    else:
        print(
            'scr_' + launcher + ' [' + launcher +
            ' args] [-rc|--run-cmd=<run_command>] [-rs|--restart-cmd=<restart_command>] ['
            + launcher + ' args]')
    print('<run_command>: The command to run when no restart file is present')
    print(
        '<restart_command>: The command to run when a restart file is present')
    print('')
    print('The invoked command will be \'' + launcher + ' [' + launcher +
          ' args] [run_command]\' when no restart file is present')
    print('The invoked command will be \'' + launcher + ' [' + launcher +
          ' args] [restart_command]\' when a restart file is present')
    print(
        'If the string \"SCR_CKPT_NAME\" appears in the restart command, it will be replaced by the name'
    )
    print('presented to SCR when the most recent checkpoint was written.')
    print('')
    print(
        'If no restart command is specified, the run command will always be used'
    )
    print('If no commands are specified, the ' + launcher +
          ' arguments will be passed directly to ' + launcher +
          ' in all circumstances')
    print(
        'If no run command is specified, but a restart command is specified,')
    print('then the restart command will be appended to the ' + launcher +
          ' arguments when a restart file is present.')


valid_launchers = ['aprun', 'flux', 'jsrun', 'lrun', 'mpirun', 'srun']


def validate_launcher(launcher):
    if launcher not in valid_launchers:
        print(f'invalid launcher {launcher} not in {valid_launchers}')
        print_usage()
        sys.exit(1)


def parseargs(argv, launcher=''):
    starti = 0
    if launcher == '':
        launcher = argv[0]
        starti = 1
    validate_launcher(launcher)
    launcher_args = []
    run_cmd = ''
    restart_cmd = ''
    restart_args = []
    # position starts at zero to parse args
    position = 0
    for i in range(starti, len(argv)):
        # pos ->       0                           1                                   2                     3
        # ['+launcher+' args] [-rc|--run-cmd=<run_command>] [-rs|--restart-cmd=<restart_command>] ['+launcher+' args]')
        # haven't gotten to run command yet
        if position == 0:
            # run command is next (or here if we have an '=')
            if argv[i].startswith('-rc') or argv[i].startswith('--run-cmd'):
                if '=' in argv[i]:
                    run_cmd = argv[i][argv[i].find('=') +
                                      1:]  # take after the equals
                else:
                    position = 1
            # no run command but got to the restart command
            elif argv[i].startswith('-rs') or argv[i].startswith(
                    '--restart-cmd'):
                if '=' in argv[i]:
                    restart_cmd = argv[i][argv[i].find('=') +
                                          1:]  # take after the equals
                    position = 3  # final args
                else:
                    position = 2
            # these are launcher args before the run command
            else:
                launcher_args.append(argv[i])
        # position is 1, we are expecting the run command
        elif position == 1:
            run_cmd = argv[i]
            position = 0
        # expecting the restart command
        elif position == 2:
            restart_cmd = argv[i]
            position = 3
        # final launcher args
        elif position == 3:
            if argv[i].startswith('-rc') or argv[i].startswith('--run-cmd'):
                if '=' in argv[i]:
                    run_cmd = argv[i][argv[i].find('=') +
                                      1:]  # take after the equals
                    position = 0
                else:
                    position = 1
            # these are trailing restart args
            else:
                restart_args.append(argv[i])
    return launcher, launcher_args, run_cmd, restart_cmd, restart_args
