#! /usr/bin/env python3

# scr_poststage.py

#
# SCR allows you to spawn off dataset transfers "in the background"
# that will finish some time after a job completes.  This saves you from
# using your compute time to transfer datasets.  You can do this by
# specifying SCR_FLUSH_POSTAGE=1 in your SCR config.  This currently is
# only supported on on IBM burst buffer nodes, and only when FLUSH=BBAPI is
# specified in the burst buffer's storage descriptor.
#
# This script is to be run as a 2nd-half post-stage script on an IBM
# system.  A 2nd-half post-stage script will run after all the job's burst
# buffer transfers have finished (which could be hours later after the job
# finishes).  This script will finalize any completed burst buffer dataset
# transfers so that they're visible to SCR.
#

# Pass the prefix as an argument
# e.g., python3 scr_poststage.py /tmp
# or scr_poststage('/tmp')

import os, sys

if 'pyfe' not in sys.path:
  sys.path.insert(0,'/'.join(os.path.realpath(__file__).split('/')[:-2]))
  import pyfe

import argparse, subprocess
from datetime import datetime
from pyfe import scr_const
from pyfe.scr_common import runproc

# do_poststage is called from scr_poststage below
# not intended to be directly called
# can hardcode the bindir and logfile in scr_poststage
def do_poststage(bindir=None,prefix=None,logfile=None):
  logfile.write(str(datetime.now())+' Begin post_stage\n')
  logfile.write('Current index before finalizing:\n')
  argv = [bindir+'/scr_index','-l','-p',prefix]
  out = runproc(argv=argv,getstdout=True)[0]
  logfile.write(out)

  # If we fail to finalize any dataset, set this to the ID of that dataset.
  # We later then only attempt to finalize checkpoints up to the first
  # failed output dataset ID.
  failed_id = 0
  
  logfile.write('--- Processing output datasets ---\n')
  argv = [bindir+'/scr_flush_file','--dir',prefix,'--list-output']
  out = runproc(argv=argv,getstdout=True,getstderr=True)[0]
  logfile.write(out[1])
  for cid in out[0].split('\n'):
    if len(cid)==0:
      continue
    # Get name of this dataset id
    argv = [bindir+'/scr_flush_file','--dir',prefix,'--name',cid]
    dset = runproc(argv=argv,getstdout=True)[0]
    logfile.write('Looking at output dataset '+str(cid)+' ('+str(dset)+')\n')

    argv = [bindir+'/scr_flush_file','--dir',prefix,'--need-flush',cid]
    tempout, returncode = runproc(argv=argv,getstdout=True,getstderr=True)
    if len(tempout[1])>0:
      logfile.write(tempout[1])
    if len(tempout[0])>0:
      logfile.write(tempout[0])
    if returncode==0:
      logfile.write('Output dataset '+str(cid)+' ('+str(dset)+') is already flushed, skip it\n')
      # Dataset is already flushed, skip it
      continue

    logfile.write('Finalizing transfer for dataset '+str(cid)+' ('+str(dset)+')\n')
    argv = [bindir+'/scr_flush_file','-r','-s',cid,'--dir',prefix]
    tempout, returncode = runproc(argv=argv,getstdout=True,getstderr=True)
    if len(tempout[1])>0:
      logfile.write(tempout[1])
    if len(tempout[0])>0:
      logfile.write(tempout[0])
    if returncode==0:
      logfile.write('Error: Can\'t resume output dataset '+str(cid)+' ('+str(dset)+')\n')
      failed_id = cid
      break

    logfile.write('Writing summary for dataset '+str(cid)+' ('+str(dset)+')\n')
    argv = [bindir+'/scr_flush_file','-s',cid,'-S','--dir',prefix]
    tempout, returncode = runproc(argv=argv,getstdout=True,getstderr=True)
    if len(tempout[1])>0:
      logfile.write(tempout[1])
    if len(tempout[0])>0:
      logfile.write(tempout[0])
    if returncode==0:
      logfile.write('ERROR: can\'t write summary for output dataset '+str(cid)+' ('+str(dset)+')\n')
      failed_id = cid
      break

    logfile.write('Adding dataset '+str(cid)+' ('+str(dset)+') to index\n')
    argv = [bindir+'/scr_index','-p',prefix,'--add='+str(dset)]
    tempout, returncode = runproc(argv=argv,getstdout=True,getstderr=True)
    if len(tempout[1])>0:
      logfile.write(tempout[1])
    if len(tempout[0])>0:
      logfile.write(tempout[0])
    if returncode==0:
      logfile.write('Couldn\'t add output dataset '+str(cid)+' ('+str(dset)+') to index\n')
      failed_id = cid
      break

	# Finalize each checkpoint listed in the flush file.  If there are any
	# failed output files (FAILED_ID > 0) then only finalize checkpoints
	# up to the last good output file.  If there are no failures
	# (FAILED_ID = 0) then all checkpoints are iterated over.
  logfile.write('--- Processing checkpoints ---\n')
  argv = [bindir+'/scr_flush_file','--dir',prefix,'--before',str(failed_id),'--list-ckpt']
  out = runproc(argv=argv,getstdout=True)[0]
  for cid in out.split('\n'):
    if len(cid)==0:
      continue
    # Get name of this dataset id
    argv = [bindir+'/scr_flush_file','--dir',prefix,'--name',cid]
    dset = runproc(argv=argv,getstdout=True)[0]
    logfile.write('Looking at checkpoint dataset '+str(cid)+' ('+str(dset)+')\n')

    argv = [bindir+'/scr_flush_file','--dir',prefix,'--need-flush',cid]
    tempout, returncode = runproc(argv=argv,getstdout=True,getstderr=True)
    if len(tempout[1])>0:
      logfile.write(tempout[1])
    if len(tempout[0])>0:
      logfile.write(tempout[0])
    if returncode==0:
      # Dataset is already flushed, skip it
      continue

    logfile.write('Finalizing transfer for checkpoint dataset '+str(cid)+' ('+str(dset)+')\n')
    argv = [bindir+'/scr_flush_file','-r','-s',cid,'--dir',prefix]
    tempout, returncode = runproc(argv=argv,getstdout=True,getstderr=True)
    if len(tempout[1])>0:
      logfile.write(tempout[1])
    if len(tempout[0])>0:
      logfile.write(tempout[0])
    if returncode==0:
      logfile.write('Error: Can\'t resume checkpoint dataset '+str(cid)+' ('+str(dset)+')\n')
      continue

    logfile.write('Writing summary for checkpoint dataset '+str(cid)+' ('+str(dset)+')\n')
    argv = [bindir+'/scr_flush_file','-s',cid,'-S','--dir',prefix]
    tempout, returncode = runproc(argv=argv,getstdout=True,getstderr=True)
    if len(tempout[1])>0:
      logfile.write(tempout[1])
    if len(tempout[0])>0:
      logfile.write(tempout[0])
    if returncode==0:
      logfile.write('ERROR: can\'t write summary for checkpoint dataset '+str(cid)+' ('+str(dset)+')\n')
      continue

    logfile.write('Adding checkpoint dataset '+str(cid)+' ('+str(dset)+') to index\n')
    argv = [bindir+'/scr_index','-p',prefix,'--add='+str(dset)]
    tempout, returncode = runproc(argv=argv,getstdout=True,getstderr=True)
    if len(tempout[1])>0:
      logfile.write(tempout[1])
    if len(tempout[0])>0:
      logfile.write(tempout[0])
    if returncode==0:
      logfile.write('Couldn\'t add checkpoint dataset '+str(cid)+' ('+str(dset)+') to index\n')
      continue

    logfile.write('Setting current checkpoint dataset to '+str(cid)+' ('+str(dset)+')\n')
    argv = [bindir+'/scr_index','-p',prefix,'--current='+str(dset)]
    tempout, returncode = runproc(argv=argv,getstdout=True,getstderr=True)
    if len(tempout[1])>0:
      logfile.write(tempout[1])
    if len(tempout[0])>0:
      logfile.write(tempout[0])
    if returncode==0:
      logfile.write('Couldn\'t set current checkpoint dataset to '+str(cid)+' ('+str(dset)+')\n')

  logfile.write('All done, index now:\n')
  argv = [bindir+'/scr_index','-l','-p',prefix]
  out, returncode = runproc(argv=argv,getstdout=True,getstderr=True)
  if len(out[1])>0:
    logfile.write(out[1])
  if len(out[0])>0:
    logfile.write(out[0])
  if returncode==0:
    logfile.write('Couldn\'t add checkpoint dataset '+str(cid)+' ('+str(dset)+') to index\n')

def scr_poststage(prefix=None):
  if prefix is None:
    return

  # The full path to the dir containing the SCR binaries (scr_index and scr_flush_file)
  bindir=scr_const.X_BINDIR

  # Path to where you want the scr_poststage log
  logfile=prefix+'/scr_poststage.log'
  try:
    with open(logfile,'a') as logfile:
      do_poststage(bindir,prefix,logfile)
  except:
    pass

if __name__=='__main__':
  parser = argparse.ArgumentParser(add_help=False, argument_default=argparse.SUPPRESS, prog='scr_poststage')
  parser.add_argument('-h','--help', action='store_true', help='Show this help message and exit.')
  parser.add_argument('-p','--prefix', metavar='<dir>', type=str, default=None, help='Specify the prefix directory.')
  parser.add_argument('rest', nargs=argparse.REMAINDER)
  args = vars(parser.parse_args())
  if 'help' in args:
    parser.print_help()
  elif args['prefix'] is None and (args['rest'] is None or len(args['rest'])==0):
    print('The prefix directory must be specified.')
  elif args['prefix'] is None:
    scr_poststage(prefix=args['rest'][0])
  else:
    scr_poststage(prefix=args['prefix'])
