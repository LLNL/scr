# this method takes an scr_env, the contained resource manager will determine which methods above to use
def list_down_nodes(reason=False,
                    free=False,
                    nodes_down=[],
                    runtime_secs=None,
                    nodes=None,
                    scr_env=None,
                    log=None):

    if scr_env is None or scr_env.resmgr is None or scr_env.param is None:
        raise RuntimeError(
            'scr_list_down_nodes: ERROR: environment, resmgr, or param not set'
        )

    # check that we have a list of nodes before going any further
    if not nodes:
        nodes = scr_env.node_list()
    if not nodes:
        nodes = scr_env.resmgr.job_nodes()
    if not nodes:
        raise RuntimeError(
            'scr_list_down_nodes: ERROR: Nodeset must be specified or script must be run from within a job allocation.'
        )

    # drop any nodes that we are told are down
    for node in nodes_down:
        if node in nodes:
            nodes.remove(node)

    # get a dictionary of all unavailable (down or excluded) nodes and reason
    # keys are nodes and the values are the reasons
    unavailable = scr_env.resmgr.list_down_nodes_with_reason(nodes=nodes,
                                                             scr_env=scr_env)

    # TODO: read exclude list from a file, as well?

    # log each newly failed node, along with the reason
    if log:
        for node, reason in unavailable.items():
            note = node + ": " + reason
            log.event('NODE_FAIL', note=note, secs=runtime_secs)

    if not reason:
        return sorted(list(unavailable.keys()))

    return unavailable
