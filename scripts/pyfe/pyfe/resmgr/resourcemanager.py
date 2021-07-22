#! /usr/bin/env python3

# resourcemanager
# ResourceManager is the super class of the specific resource manager classes
# the ResourceManager itself is missing its environment type (unknown)
# common methods shared between subclasses can be used from here
# (all environments currently also call the super init)

import os
from pyfe import scr_const, scr_hostlist
from pyfe.scr_common import scr_prefix, runproc

class ResourceManager(object):
  def __init__(self,resmgr='unknown'):
    self.prefix = scr_prefix()
    self.resmgr = resmgr
    self.use_watchdog = False
    self.nodes = self.get_job_nodes()
    self.clustershell_nodeset = None
    if scr_const.USE_CLUSTERSHELL != '0':
      try:
        import ClusterShell.NodeSet as MyCSNodeSet
        self.clustershell_nodeset = MyCSNodeSet
      except:
        pass

  # no arg -> usewatchdog will return True or False for whether or not watchdog is enabled
  # boolean arg -> default value is to set self.use_watchdog = argument
  # override this method for a specific resource manager to disable use of scr_watchdog
  def usewatchdog(self,use_scr_watchdog=None):
    if use_scr_watchdog is None:
      return self.use_watchdog
    self.use_watchdog = use_scr_watchdog

  def get_jobstep_id(self,user='',pid=-1):
    return None

  def getjobid(self):
    """Return current job allocation id.

    Returns
    -------
    str
        job allocation id
    """
    # failed to read jobid from environment,
    # assume user is running in test mode
    return None

  # get node list
  def get_job_nodes(self):
    """Return compute nodes in allocation.

    Each node should be specified once and in order
    if node order matters, e.g., for job launchers.

    Returns
    -------
    str
        list of allocation compute nodes in string format
    """
    return None

  def get_downnodes(self):
    """Return allocation compute nodes the resource manager identifies as down.

    Some resource managers can report nodes it has determined to be down.
    The returned list should be a subset of the allocation nodes.

    Returns
    -------
    str
        list of down compute nodes in string format
        or None if there are no down nodes
    """
    return None

  def scr_kill_jobstep(self,jobid=-1):
    return 1

  def get_scr_end_time(self):
    """Return expected allocation end time.

    The end time must be expressed as seconds since
    the Unix epoch.

    Returns
    -------
    int
        end time as secs since Unix epoch
        or None if there is no end time
    """
    return 0

  # Returns a hostlist string given a list of hostnames
  def compress_hosts(self,hostnames=[]):
    if hostnames is None or len(hostnames)==0:
      return ''
    if type(hostnames) is str:
      hostnames = hostnames.split(',')
    if self.clustershell_nodeset is not None:
      nodeset = self.clustershell_nodeset.NodeSet.fromlist(hostnames)
      return str(nodeset)
    return scr_hostlist.compress_range(hostnames)

  # Returns a list of hostnames given a hostlist string
  def expand_hosts(self,hostnames=''):
    if hostnames is None or hostnames=='':
      return []
    if type(hostnames) is list:
      hostnames = ','.join(hostnames)
    if self.clustershell_nodeset is not None:
      nodeset = self.clustershell_nodeset.NodeSet(hostnames)
      nodeset = [node for node in nodeset]
      return nodeset
    return scr_hostlist.expand(hostnames)

  # Given references to two lists, subtract elements in list 2 from list 1 and return remainder
  def diff_hosts(self,set1=[],set2=[]):
    if type(set1) is str:
      set1 = set1.split(',')
    if type(set2) is str:
      set2 = set2.split(',')
    if set1 is None or set1==[]:
      return set2 if set2 is not None else []
    if set2 is None or set2==[]:
      return set1
    if self.clustershell_nodeset is not None:
      set1 = self.clustershell_nodeset.NodeSet.fromlist(set1)
      set2 = self.clustershell_nodeset.NodeSet.fromlist(set2)
      # strict=False is default
      # if strict true then raises error if something in set2 not in set1
      set1.difference_update(set2, strict=False)
      # ( this should also work like set1 -= set2 )
      set1 = [node for node in set1]
      return set1
    return scr_hostlist.diff(set1=set1,set2=set2)

  # Return the intersection of two host lists
  def intersect_hosts(self,set1=[],set2=[]):
    if type(set1) is str:
      set1 = set1.split(',')
    if type(set2) is str:
      set2 = set2.split(',')
    if set1 is None or set1==[]:
      return []
    if set2 is None or set2==[]:
      return []
    if self.clustershell_nodeset is not None:
      set1 = self.clustershell_nodeset.NodeSet.fromlist(set1)
      set2 = self.clustershell_nodeset.NodeSet.fromlist(set2)
      set1.intersection_update(set2)
      set1 = [node for node in set1]
      return set1
    return scr_hostlist.intersect(set1,set2)

  # return a hash to define all unavailable (down or excluded) nodes and reason
  def list_down_nodes_with_reason(self,nodes=[], scr_env=None, free=False, cntldir_string=None, cachedir_string=None):
    return {}

  # each scavenge operation needs upnodes and downnodes_spaced
  def get_scavenge_nodelists(self,upnodes='',downnodes=''):
    # get nodesets
    jobnodes = self.get_job_nodes()
    if jobnodes is None:
      ### error handling
      print('scr_scavenge: ERROR: Could not determine nodeset.')
      return '', ''
    jobnodes = self.expand_hosts(jobnodes)
    if downnodes != '':
      downnodes = self.expand_hosts(downnodes)
      upnodes = self.diff_hosts(jobnodes, downnodes)
    elif upnodes != '':
      upnodes = self.expand_hosts(upnodes)
      downnodes = self.diff_hosts(jobnodes, upnodes)
    else:
      upnodes = jobnodes
    ##############################
    # format up and down node sets for scavenge command
    #################
    upnodes = self.compress_hosts(upnodes)
    downnodes_spaced = ' '.join(downnodes)
    return upnodes, downnodes_spaced

if __name__=='__main__':
  resmgr = ResourceManager()
  print(type(resmgr))
