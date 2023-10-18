#! /usr/bin/env python3

from scrjob import scr_const


def list_dir(user=None,
             jobid=None,
             base=False,
             runcmd=None,
             scr_env=None,
             bindir=''):
    """This method returns info on the SCR control/cache/prefix directories
  for the current user and jobid

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
    This method will print 'list_dir: INVALID: %s', where %s is an error
    string representing the error.
    The method will then return the integer 1
  """
    # check that user specified "control" or "cache"
    if runcmd != 'control' and runcmd != 'cache':
        print('list_dir: INVALID: \'control\' or \'cache\' must be specified.')
        return 1

    # TODO: read cache directory from config file
    bindir = scr_const.X_BINDIR

    # ensure scr_env is set
    if scr_env is None or scr_env.resmgr is None or scr_env.param is None:
        print('list_dir: INVALID: Unknown environment.')
        return 1

    # get the base directory
    bases = []
    if runcmd == 'cache':
        # lookup cache base
        cachedesc = scr_env.param.get_hash('CACHE')
        if type(cachedesc) is dict:
            bases = list(cachedesc.keys())
            #foreach my $index (keys %$cachedesc) {
            #  push @bases, $index;
        elif cachedesc is not None:
            bases = [cachedesc]
        else:
            print('list_dir: INVALID: Unable to get parameter CACHE.')
            return 1
    else:
        # lookup cntl base
        bases = scr_env.param.get('SCR_CNTL_BASE')
        if type(bases) is dict:
            bases = list(bases.keys())
        elif type(bases) is not None:
            bases = [bases]
        else:
            print('list_dir: INVALID: Unable to get parameter SCR_CNTL_BASE.')
            return 1
    if len(bases) == 0:
        print('list_dir: INVALID: Length of bases [] is zero.')
        return 1

    # get the user/job directory
    suffix = ''
    if base == False:
        # if not specified, read username from environment
        if user is None:
            user = scr_env.get_user()
        # if not specified, read jobid from environment
        if jobid is None:
            jobid = scr_env.resmgr.get_job_id()
        # check that the required environment variables are set
        if user is None or jobid is None:
            # something is missing, print invalid dir and exit with error
            print('list_dir: INVALID: Unable to determine user or jobid.')
            return 1
        suffix = user + '/scr.' + jobid

    # ok, all values are here, print out the directory name and exit with success
    dirs = []
    for abase in bases:
        if suffix != '':
            dirs.append(abase + '/' + suffix)
        else:
            dirs.append(abase)
    dirs = ' '.join(dirs)
    return dirs
