import os

from scrjob import config, hostlist
from scrjob.common import scr_prefix
from scrjob.param import Param
from scrjob.resmgrs import AutoResourceManager
from scrjob.launchers import AutoJobLauncher
from scrjob.nodetests import NodeTests
from scrjob.remoteexec import Pdsh, ClusterShell
from scrjob.cli.scr_nodes_file import SCRNodesFile


class JobEnv:
    """The JobEnv class tracks information relating to the SCR job environment.

    This class retrieves information from the environment.
    This class contains pointers to the active Joblauncher, ResourceManager, and Param.

    References to these other classes should be assigned following instantiation of this class.

    Attributes
    ----------
    prefix     - string, SCR_PREFIX value, initialized upon init or through scr_prefix()
    param      - class, a reference to Param to read SCR parameter values
    resmgr     - class, a reference to ResourceManager to query resource manager
    launcher   - class, a reference to Joblauncher for MPI job launcher
    """

    def __init__(self, prefix=None, param=None, resmgr=None, launcher=None):
        # record SCR_PREFIX directory, default to scr_prefix if not provided
        self.prefix = prefix
        if prefix is None:
            self.prefix = scr_prefix()

        # used to read SCR parameter values,
        # which may be from environment or config files
        self.param = param
        if param is None:
            self.param = Param()

        # resource manager to query job id and node list
        self.resmgr = resmgr
        if resmgr is None:
            self.resmgr = AutoResourceManager()

        # job launcher for MPI jobs
        if launcher is None:
            self.launcher = AutoJobLauncher()
        else:
            self.launcher = AutoJobLauncher(launcher)

        self.nodetests = NodeTests()

        if config.USE_CLUSTERSHELL:
            self.rexec = ClusterShell()
        else:
            self.rexec = Pdsh(launcher=launcher)

    def user(self):
        """Return the username from the environment."""
        return os.environ.get('USER')

    def node_list(self):
        """Return the SCR_NODELIST, if set, or None."""
        nodelist = os.environ.get('SCR_NODELIST')
        return hostlist.expand_hosts(nodelist)

    def dir_prefix(self):
        """Return the scr prefix."""
        return self.prefix

    def dir_scr(self):
        """Return the prefix/.scr directory."""
        return os.path.join(self.prefix, '.scr')

    def dir_dset(self, d):
        """Given a dataset id, return the dataset directory within the prefix
        prefix/.scr/scr.dataset.<id>"""
        return os.path.join(self.dir_scr(), 'scr.dataset.' + str(d))

    def _append_userjob(self, dirs):
        user = self.user()
        jobid = self.resmgr.job_id()
        return [os.path.join(d, user, 'scr.' + jobid) for d in dirs]

    def dir_cache(self, base=False):
        # lookup cache base directories
        desc = self.param.get_hash('CACHE')
        if type(desc) is dict:
            dirs = list(desc.keys())
        elif desc is not None:
            dirs = [desc]
        else:
            raise RuntimeError('Unable to get parameter CACHE.')

        if not base:
            dirs = self._append_userjob(dirs)
        return dirs

    def dir_control(self, base=False):
        # lookup cntl base directories
        desc = self.param.get('SCR_CNTL_BASE')
        if type(desc) is dict:
            dirs = list(desc.keys())
        elif type(desc) is not None:
            dirs = [desc]
        else:
            raise RuntimeError('Unable to get parameter SCR_CNTL_BASE.')

        if not base:
            dirs = self._append_userjob(dirs)
        return dirs

    def runnode_count(self):
        """Return the number of nodes used in the last run, if known."""
        nodes_file = SCRNodesFile(prefix=self.prefix)
        num_nodes = nodes_file.last_num_nodes()
        return num_nodes if num_nodes is not None else 0
