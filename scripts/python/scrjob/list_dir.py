import os

from scrjob import scr_const


def list_dir(user=None,
             jobid=None,
             base=False,
             runcmd=None,
             scr_env=None):
    """This method returns info on the SCR control/cache/prefix directories for
    the current user and jobid.

    Required Parameters
    ----------
    runcmd     string, 'control' or 'cache'
    scr_env    class, an instance of SCR_Env with valid references to
               scr_env.resmgr and scr_env.param

    Returns
    -------
    string
      A space separated list of directories

    Error
    -----
      This method raises a RuntimeError
    """

    # check that user specified "control" or "cache"
    if runcmd != 'control' and runcmd != 'cache':
        raise RuntimeError('list_dir: INVALID: \'control\' or \'cache\' must be specified.')

    # ensure scr_env is set
    if scr_env is None or scr_env.resmgr is None or scr_env.param is None:
        raise RuntimeError('list_dir: INVALID: Unknown environment.')

    # get the base directory
    bases = []
    if runcmd == 'cache':
        # lookup cache base
        cachedesc = scr_env.param.get_hash('CACHE')
        if type(cachedesc) is dict:
            bases = list(cachedesc.keys())
        elif cachedesc is not None:
            bases = [cachedesc]
        else:
            raise RuntimeError('list_dir: INVALID: Unable to get parameter CACHE.')
    else:
        # lookup cntl base
        bases = scr_env.param.get('SCR_CNTL_BASE')
        if type(bases) is dict:
            bases = list(bases.keys())
        elif type(bases) is not None:
            bases = [bases]
        else:
            raise RuntimeError('list_dir: INVALID: Unable to get parameter SCR_CNTL_BASE.')

    if len(bases) == 0:
        raise RuntimeError('list_dir: INVALID: Length of bases [] is zero.')

    # get the user/job directory
    suffix = ''
    if base == False:
        # if not specified, read username from environment
        if user is None:
            user = scr_env.get_user()

        # if not specified, read jobid from environment
        if jobid is None:
            jobid = scr_env.resmgr.job_id()

        # check that the required environment variables are set
        if user is None or jobid is None:
            # something is missing, print invalid dir and exit with error
            raise RuntimeError('list_dir: INVALID: Unable to determine user or jobid.')

        suffix = os.path.join(user, 'scr.' + jobid)

    # ok, all values are here, print out the directory name and exit with success
    dirs = []
    for abase in bases:
        if suffix != '':
            dirs.append(os.path.join(abase, suffix))
        else:
            dirs.append(abase)
    return dirs
