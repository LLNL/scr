#! /usr/bin/env python3

# scr_list_down_nodes.py
# this is a launcher script for list_down_nodes.py

import argparse
from pyfe import list_down_nodes
from pyfe.scr_environment import SCR_Env
from pyfe.scr_param import SCR_Param
from pyfe.resmgr import AutoResourceManager

if __name__=='__main__':
  parser = argparse.ArgumentParser(add_help=False, argument_default=argparse.SUPPRESS, prog='scr_list_down_nodes')
  parser.add_argument('--help', action='store_true', help='Show this help message and exit.')
  parser.add_argument('-r','--reason', action='store_true', default=False, help='Print reason node is down.')
  parser.add_argument('-f','--free', action='store_true', default=False, help='Test required drive space based on free amount, rather than capacity.')
  parser.add_argument('-d','--down', metavar='<nodeset>', type=str, default=None, help='Force nodes to be down without testing.')
  parser.add_argument('-l','--log', action='store_true', default=False, help='Add entry to SCR log for each down node.')
  parser.add_argument('-s','--secs', metavar='N', type=str, default=None, help='Specify the job\'s runtime seconds for SCR log.')
  parser.add_argument('[nodeset]', nargs='*', default=None, help='Specify the complete set of nodes to check within.')
  args = vars(parser.parse_args())
  if 'help' in args:
    parser.print_help()
  else:
    scr_env = SCR_Env()
    scr_env.resmgr = AutoResourceManager()
    scr_env.param = SCR_Param()
    ret = list_down_nodes(reason=args['reason'], free=args['free'], nodeset_down=args['down'], log_nodes=args['log'], runtime_secs=args['secs'], nodeset=args['[nodeset]'], scr_env=scr_env)
    print(str(ret),end='') ### remove trailing newlines?
