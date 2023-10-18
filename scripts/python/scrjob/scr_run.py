#! /usr/bin/env python3

# scr_run.py

# the general launcher for the scripts
# if called directly the launcher to use (srun/jsrun/mpirun) should be specified as an argument
# scr_{srun,jsrun,mpirun} scripts call this script with the launcher specified

import os, sys

if 'scrjob' not in sys.path:
    sys.path.insert(0, '/'.join(os.path.realpath(__file__).split('/')[:-2]))
    import scrjob

from datetime import datetime
from time import time, sleep

from scrjob import scr_const, scr_common
from scrjob.postrun import postrun
from scrjob.list_dir import list_dir
from scrjob.list_down_nodes import list_down_nodes
from scrjob.scr_common import tracefunction, runproc, scr_prefix
from scrjob.scr_prerun import scr_prerun
from scrjob.scr_watchdog import SCR_Watchdog
from scrjob.scr_environment import SCR_Env
from scrjob.launchers import AutoJobLauncher
from scrjob.resmgrs import AutoResourceManager
from scrjob.scr_param import SCR_Param
from scrjob.scr_glob_hosts import scr_glob_hosts
from scrjob.cli import SCRLog


# determine how many nodes are needed
def nodes_needed(scr_env, nodelist):
    # if SCR_MIN_NODES is set, use that
    num_needed = os.environ.get('SCR_MIN_NODES')
    if num_needed is None or int(num_needed) <= 0:
        # otherwise, use value in nodes file if one exists
        num_needed = scr_env.get_runnode_count()
        if num_needed <= 0:
            # otherwise, assume we need all nodes in the allocation
            num_needed = scr_glob_hosts(count=True,
                                        hosts=nodelist,
                                        resmgr=scr_env.resmgr)
            if num_needed is None:
                # failed all methods to estimate the minimum number of nodes
                return 0
    return int(num_needed)


# return number of nodes left in allocation after excluding down nodes
def nodes_remaining(resmgr, nodelist, down_nodes):
    num_left = scr_glob_hosts(count=True,
                              minus=nodelist + ':' + down_nodes,
                              resmgr=resmgr)
    if num_left is None:
        return 0
    return int(num_left)


# is there a halt condition instructing us to stop?
def should_halt(bindir, prefix):
    argv = [os.path.join(bindir, 'scr_retries_halt'), '--dir', prefix]
    returncode = runproc(argv=argv)[1]
    return (returncode == 0)


def scr_run(launcher='',
            launcher_args=[],
            run_cmd='',
            restart_cmd='',
            restart_args=[]):
    prog = 'scr_' + launcher

    bindir = scr_const.X_BINDIR

    val = os.environ.get('SCR_ENABLE')
    if val == '0':
        launcher = [launcher]
        launcher.extend(launcher_args)
        returncode = runproc(argv=launcher)[1]
        sys.exit(returncode)

    # turn on verbosity
    verbose = False
    val = os.environ.get('SCR_DEBUG')
    if val is not None and int(val) > 0:
        verbose = True

    # turn on python function tracing
    if scr_const.PYFE_TRACE_FUNC == '1' or os.environ.get(
            'PYFE_TRACE_FUNC') == '1':
        sys.settrace(tracefunction)

    # make a record of start time
    timestamp = datetime.now()
    start_secs = int(time())
    print(prog + ': Started: ' + str(timestamp))

    # get prefix directory
    prefix = scr_prefix()

    param = SCR_Param()

    # resource manager (SLURM/LSF/ ...) set by argument or compile constant
    resmgr = AutoResourceManager()

    # launcher contains attributes unique to launcher (srun/jsrun/ ...)
    launcher = AutoJobLauncher(launcher)

    # env contains general environment infos independent of resmgr/launcher
    scr_env = SCR_Env(prefix=prefix)

    # give scr_env a pointer to the objects for calling other methods
    scr_env.param = param
    scr_env.resmgr = resmgr
    scr_env.launcher = launcher

    # this may be used by a launcher to store a list of hosts
    launcher.hostfile = os.path.join(scr_env.dir_scr(), 'hostfile')

    # jobid will come from resource manager.
    jobid = resmgr.get_job_id()
    user = scr_env.get_user()

    # We need the jobid for logging, and need to be running within an allocation
    # for operations such as scavenge.  This test serves both purposes.
    if jobid is None:
        print(prog + f': ERROR: No valid job ID or not in an allocation.')
        sys.exit(1)

    # create object to write log messages
    log = SCRLog(prefix, jobid, user=user, jobstart=start_secs)

    # get the nodeset of this job
    nodelist = scr_env.get_scr_nodelist()
    if nodelist is None:
        nodelist = resmgr.get_job_nodes()
        if nodelist is None:
            print(prog +
                  f': ERROR: Could not identify nodeset for job {jobid}')
            sys.exit(1)
    nodelist = ','.join(resmgr.expand_hosts(nodelist))

    watchdog = None
    val = os.environ.get('SCR_WATCHDOG')
    if val != '1':
        resmgr.usewatchdog(False)
    else:
        resmgr.usewatchdog(True)
        watchdog = SCR_Watchdog(prefix, scr_env)

    # TODO: define resmgr.prerun() and launcher.prerun() hooks, call from scr_prerun?
    # run a NOP with srun, other launchers could do any preamble work here
    launcher.prepareforprerun()

    # make a record of time prerun is started
    timestamp = datetime.now()
    print(prog + ': prerun: ' + str(timestamp))

    # test runtime, ensure filepath exists
    if scr_prerun(scr_env=scr_env) != 0:
        print(prog + ': ERROR: Command failed: scr_prerun -p ' + prefix)
        sys.exit(1)

    # look up allocation end time, record in SCR_END_TIME
    endtime = resmgr.get_end_time()
    if endtime == 0:
        if verbose == True:
            print(prog + ': WARNING: Unable to get end time.')
    elif endtime == -1:  # no end time / limit
        pass
    #else:
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
    down_nodes = ''
    attempts = 0
    while True:
        # once we mark a node as bad, leave it as bad (even if it comes back healthy)
        # TODO: This hacks around the problem of accidentally deleting a checkpoint set during distribute
        #       when a relaunch lands on previously down nodes, which are healthy again.
        #       A better way would be to remember the last set used, or to provide a utility to run on *all*
        #       nodes to distribute files (also useful for defragging the machine) -- for now this works.
        keep_down = down_nodes

        # set "first run" flag, let individual tests decide how to handle that
        first_run = (attempts == 0)

        # are there enough nodes to continue?
        down_nodes = list_down_nodes(free=first_run,
                                     nodeset_down=down_nodes,
                                     scr_env=scr_env)

        # list_down_nodes returns 0 for none, 1 for error
        # could handle an error here, or just continue
        if type(down_nodes) is int:
            down_nodes = ''
        # a comma separated string of down nodes is returned otherwise
        else:  #if down_nodes != '':
            # print the reason for the down nodes, and log them
            # when reason == True a string formatted for printing will be returned
            printstring = list_down_nodes(reason=True,
                                          free=first_run,
                                          nodeset_down=down_nodes,
                                          runtime_secs='0',
                                          scr_env=scr_env,
                                          log=log)
            print(printstring)

            # if this is the first run, we hit down nodes right off the bat, make a record of them
            if first_run:
                start_secs = int(time())
                print('SCR: Failed node detected: JOBID=' + jobid +
                      ' ATTEMPT=0 TIME=' + str(start_secs) +
                      ' NNODES=-1 RUNTIME=0 FAILED=' + down_nodes)

            # determine how many nodes are needed
            num_needed = nodes_needed(scr_env, nodelist)
            if num_needed <= 0:
                print(prog +
                      ': ERROR: Unable to determine number of nodes needed')
                break

            # determine number of nodes remaining in allocation
            num_left = nodes_remaining(resmgr, nodelist, down_nodes)

            # check that we have enough nodes after excluding down nodes
            if num_left < num_needed:
                print(prog + ': (Nodes remaining=' + str(num_left) +
                      ') < (Nodes needed=' + str(num_needed) +
                      '), ending run.')
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
        print(prog + ': RUN ' + str(attempts) + ': ' + str(timestamp))

        # launch the job, make sure we include the script node and exclude down nodes
        print(prog + ': Launching ' + str(launch_cmd))
        proc, jobstep = launcher.launchruncmd(up_nodes=nodelist,
                                              down_nodes=down_nodes,
                                              launcher_args=launch_cmd)
        if watchdog is None:
            (finished, success) = launcher.waitonprocess(proc)
        else:
            print(prog + ': Entering watchdog method')
            # watchdog returned error or a watcher process was launched
            if watchdog.watchproc(proc, jobstep) != 0:
                print(prog + ': Error launching watchdog')
                (finished, success) = launcher.waitonprocess(proc)
            # else the watchdog returned because the process has finished/been killed
            else:
                #TODO: verify this really works for case where watchproc() returns 0 / succeeds
                (finished, success) = launcher.waitonprocess(proc)

        #print('Process has finished or has been terminated.')

        end_secs = int(time())
        run_secs = end_secs - start_secs

        # log stats on the latest run attempt
        log.event('RUN_END', note='run=' + str(attempts), secs=str(run_secs))

        # check for and log any down nodes
        # logging happens within list_down_nodes
        # a string formatted for printing is returned when reason == True
        printstring = list_down_nodes(reason=True,
                                      nodeset_down=keep_down,
                                      runtime_secs=str(run_secs),
                                      scr_env=scr_env,
                                      log=log)
        print(printstring)

        # decrement retry counter
        runs -= 1
        if runs == 0:
            print(prog + ': ' + totalruns + ' launches exhausted, ending run.')
            break

        # is there a halt condition instructing us to stop?
        if should_halt(bindir, prefix):
            print(prog + ': Halt condition detected, ending run.')
            break

        ##### Reduce this sleep time when testing scripts #####
        # give nodes a chance to clean up
        sleep(60)

        # check for halt condition again after sleep
        if should_halt(bindir, prefix):
            print(prog + ': Halt condition detected, ending run.')
            break

    # make a record of time postrun is started
    timestamp = datetime.now()
    print(prog + ': postrun: ' + str(timestamp))

    # scavenge files from cache to parallel file system
    if postrun(prefix_dir=prefix, scr_env=scr_env, verbose=verbose,
               log=log) != 0:
        print(prog + ': ERROR: Command failed: scr_postrun -p ' + prefix)

    # make a record of end time
    timestamp = datetime.now()
    print(prog + ': Ended: ' + str(timestamp))

    if finished == True and success == True:
        returncode = 0
    else:
        returncode = 1

    sys.exit(returncode)


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
        scr_run(launcher, launcher_args=sys.argv[2:])
    else:
        launcher, launcher_args, run_cmd, restart_cmd, restart_args = parseargs(
            sys.argv[1:])
        #if launcher=='flux', remove 'mini' 'submit' 'run' from front of args
        scr_run(launcher_args=launcher_args,
                run_cmd=run_cmd,
                restart_cmd=restart_cmd,
                restart_args=restart_args)
