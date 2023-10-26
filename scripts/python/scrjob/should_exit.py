# Determine whether the job script should exit or launch another run.
# Checks that:
#   - a halt condition has not been set
#   - there is sufficient time remaining in the allocation
#   - there are sufficient healthy nodes in the allocation
# Returns True if job should exit, False otherwise
# Raises RuntimeError on failure

import os

from scrjob.list_down_nodes import list_down_nodes
from scrjob.cli import SCRLog, SCRRetriesHalt


# determine how many nodes are needed
def nodes_needed(scr_env, nodelist):
    # if SCR_MIN_NODES is set, use that
    num_needed = os.environ.get('SCR_MIN_NODES')
    if num_needed is None or int(num_needed) <= 0:
        # otherwise, use value in nodes file if one exists
        num_needed = scr_env.runnode_count()
        if num_needed <= 0:
            # otherwise, assume we need all nodes in the allocation
            num_needed = len(nodelist)
            if num_needed == 0:
                # failed all methods to estimate the minimum number of nodes
                return 0
    return int(num_needed)

# return number of nodes left in allocation after excluding down nodes
def nodes_subtract(nodes, nodes_remove):
    temp = [n for n in nodes if n not in nodes_remove]
    return temp

# return number of nodes left in allocation after excluding down nodes
def nodes_remaining(nodelist, down_nodes):
    temp = nodes_subtract(nodelist, down_nodes)
    return len(temp)

def should_exit(scr_env, keep_down=[], first_run=False, verbose=False):
    prefix = scr_env.dir_prefix()

    # We need the jobid for logging, and need to be running within an allocation
    # for operations such as scavenge.  This test serves both purposes.
    jobid = scr_env.resmgr.job_id()
    if jobid is None:
        raise RuntimeError('No valid job ID or not in an allocation.')

    # get the nodeset of this job
    nodelist = scr_env.node_list()
    if not nodelist:
        nodelist = scr_env.resmgr.job_nodes()
    if not nodelist:
        raise RuntimeError(f'Could not identify nodeset for job {jobid}')

    # TODO: read SCR_HALT_SECONDS and current time
    # This should be handled indirectly by the library,
    # since it will record a halt condition.
    # look up allocation end time, record in SCR_END_TIME
    #endtime = scr_env.resmgr.end_time()

    # is there a halt condition instructing us to stop?
    halt = SCRRetriesHalt(prefix)
    halt_cond = halt.check():
    if halt_cond:
        if verbose:
            print(f'Halt condition detected: {halt_cond}')
        return True

    # create object to write log messages
    user = scr_env.user()
    start_secs = 0
    log = SCRLog(prefix, jobid, user=user, jobstart=start_secs)

    # check for any down nodes
    reasons = list_down_nodes(reason=True,
                              free=first_run,
                              nodes_down=keep_down,
                              runtime_secs='0',
                              scr_env=scr_env,
                              log=log)

    down_nodes = sorted(list(reasons.keys()))

    # print list of failed nodes and reasons
    if verbose:
        for node in down_nodes:
            print("FAILED: " + node + ': ' + reasons[node])

    # bail out if we don't have enough nodes to continue
    if down_nodes:
        # determine how many nodes are needed
        num_needed = nodes_needed(scr_env, nodelist)
        if num_needed <= 0:
            raise RuntimeError('Unable to determine number of nodes needed')

        # determine number of nodes remaining in allocation
        num_left = nodes_remaining(nodelist, down_nodes)

        # check that we have enough nodes after excluding down nodes
        if num_left < num_needed:
            if verbose:
                print(f'nodes_remaining={num_left} < nodes_needed={num_needed}')
            return True

    # everything checks out, the job can keep going
    return False
