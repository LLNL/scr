import os
from scrjob import scr_const
from scrjob.scr_common import scr_prefix
from scrjob.cli.scr_nodes_file import SCRNodesFile


class SCR_Env:
    """The SCR_Env class tracks information relating to the environment.

    This class retrieves information from the environment.
    This class contains pointers to the active Joblauncher, ResourceManager, and SCR_Param.

    References to these other classes should be assigned following instantiation of this class.

    Attributes
    ----------
    param      - class, a reference to SCR_Param
    launcher   - class, a reference to Joblauncher
    resmgr     - class, a reference to ResourceManager
    prefix     - string, initialized upon init or through scr_prefix()
    """

    def __init__(self, prefix=None):
        # we can keep a reference to the other objects
        self.param = None
        self.launcher = None
        self.resmgr = None

        # record SCR_PREFIX directory, default to scr_prefix if not provided
        if prefix is None:
            prefix = scr_prefix()
        self.prefix = prefix

    def user(self):
        """Return the username from the environment."""
        return os.environ.get('USER')

    def node_list(self):
        """Return the SCR_NODELIST, if set, or None."""
        nodelist = os.environ.get('SCR_NODELIST')
        return self.resmgr.expand_hosts(nodelist)

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

    def runnode_count(self):
        """Return the number of nodes used in the last run, if known."""
        nodes_file = SCRNodesFile(prefix=self.prefix)
        num_nodes = nodes_file.last_num_nodes()
        return num_nodes if num_nodes is not None else 0
