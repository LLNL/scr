#! /usr/bin/env python

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

import subprocess, sys
import scr_const
from datetime import datetime

def do_poststage(bindir,prefix,logfile):
  logfile.write(str(datetime.now())+' Begin post_stage\n')
  logfile.write('Current index before finalizing:\n')
  argv = [bindir+'/scr_index','-l','-p',prefix]
  runproc = subprocess.Popen(argv, bufsize=1, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=False)
  out = runproc.communicate()[0]
  logfile.write(out)

  # If we fail to finalize any dataset, set this to the ID of that dataset.
  # We later then only attempt to finalize checkpoints up to the first
  # failed output dataset ID.
  failed_id = 0
  
  logfile.write('--- Processing output datasets ---\n')
  argv = [bindir+'/scr_flush_file','--dir',prefix,'--list-output']
  runproc = subprocess.Popen(argv, bufsize=1, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=False)
  out = runproc.communicate()
  if len(out[1])>0:
    logfile.write(out[1])
  for cid in out[0].split('\n'):
    if len(cid)==0:
      continue
    # Get name of this dataset id
    argv = [bindir+'/scr_flush_file','--dir',prefix,'--name',cid]
    runproc = subprocess.Popen(argv, bufsize=1, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=False)
    dset = runproc.communicate()[0]
    logfile.write('Looking at output dataset '+str(cid)+' ('+str(dset)+')\n')

    argv = [bindir+'/scr_flush_file','--dir',prefix,'--need-flush',cid]
    runproc = subprocess.Popen(argv, bufsize=1, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=False)
    tempout = runproc.communicate()
    if len(tempout[1])>0:
      logfile.write(tempout[1])
    if len(tempout[0])>0:
      logfile.write(tempout[0])
    if runproc.returncode==0:
      logfile.write('Output dataset '+str(cid)+' ('+str(dset)+') is already flushed, skip it\n')
      # Dataset is already flushed, skip it
      continue

    logfile.write('Finalizing transfer for dataset '+str(cid)+' ('+str(dset)+')\n')
    argv = [bindir+'/scr_flush_file','-r','-s',cid,'--dir',prefix]
    runproc = subprocess.Popen(argv, bufsize=1, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=False)
    tempout = runproc.communicate()
    if len(tempout[1])>0:
      logfile.write(tempout[1])
    if len(tempout[0])>0:
      logfile.write(tempout[0])
    if runproc.returncode==0:
      logfile.write('Error: Can\'t resume output dataset '+str(cid)+' ('+str(dset)+')\n')
      failed_id = cid
      break

    logfile.write('Writing summary for dataset '+str(cid)+' ('+str(dset)+')\n')
    argv = [bindir+'/scr_flush_file','-s',cid,'-S','--dir',prefix]
    runproc = subprocess.Popen(argv,bufsize=1,stdin=None,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell = True, universal_newlines = False)
    tempout = runproc.communicate()
    if len(tempout[1])>0:
      logfile.write(tempout[1])
    if len(tempout[0])>0:
      logfile.write(tempout[0])
    if runproc.returncode==0:
      logfile.write('ERROR: can\'t write summary for output dataset '+str(cid)+' ('+str(dset)+')\n')
      failed_id = cid
      break

    logfile.write('Adding dataset '+str(cid)+' ('+str(dset)+') to index\n')
    argv = [bindir+'/scr_index','-p',prefix,'--add='+str(dset)]
    runproc = subprocess.Popen(argv,bufsize=1,stdin=None,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell = True, universal_newlines = False)
    tempout = runproc.communicate()
    if len(tempout[1])>0:
      logfile.write(tempout[1])
    if len(tempout[0])>0:
      logfile.write(tempout[0])
    if runproc.returncode==0:
      logfile.write('Couldn\'t add output dataset '+str(cid)+' ('+str(dset)+') to index\n')
      failed_id = cid
      break

	# Finalize each checkpoint listed in the flush file.  If there are any
	# failed output files (FAILED_ID > 0) then only finalize checkpoints
	# up to the last good output file.  If there are no failures
	# (FAILED_ID = 0) then all checkpoints are iterated over.
  logfile.write('--- Processing checkpoints ---\n')
  argv = [bindir+'/scr_flush_file','--dir',prefix,'--before',str(failed_id),'--list-ckpt']
  runproc = subprocess.Popen(argv, bufsize=1, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=False)
  out = runproc.communicate()
  for cid in out[0].split('\n'):
    if len(cid)==0:
      continue
    # Get name of this dataset id
    argv = [bindir+'/scr_flush_file','--dir',prefix,'--name',cid]
    runproc = subprocess.Popen(argv, bufsize=1, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=False)
    dset = runproc.communicate()[0]
    logfile.write('Looking at checkpoint dataset '+str(cid)+' ('+str(dset)+')\n')

    argv = [bindir+'/scr_flush_file','--dir',prefix,'--need-flush',cid]
    runproc = subprocess.Popen(argv, bufsize=1, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=False)
    tempout = runproc.communicate()
    if len(tempout[1])>0:
      logfile.write(tempout[1])
    if len(tempout[0])>0:
      logfile.write(tempout[0])
    if runproc.returncode==0:
      # Dataset is already flushed, skip it
      continue

    logfile.write('Finalizing transfer for checkpoint dataset '+str(cid)+' ('+str(dset)+')\n')
    argv = [bindir+'/scr_flush_file','-r','-s',cid,'--dir',prefix]
    runproc = subprocess.Popen(argv, bufsize=1, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=False)
    tempout = runproc.communicate()
    if len(tempout[1])>0:
      logfile.write(tempout[1])
    if len(tempout[0])>0:
      logfile.write(tempout[0])
    if runproc.returncode==0:
      logfile.write('Error: Can\'t resume checkpoint dataset '+str(cid)+' ('+str(dset)+')\n')
      continue

    logfile.write('Writing summary for checkpoint dataset '+str(cid)+' ('+str(dset)+')\n')
    argv = [bindir+'/scr_flush_file','-s',cid,'-S','--dir',prefix]
    runproc = subprocess.Popen(argv,bufsize=1,stdin=None,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell = True, universal_newlines = False)
    tempout = runproc.communicate()
    if len(tempout[1])>0:
      logfile.write(tempout[1])
    if len(tempout[0])>0:
      logfile.write(tempout[0])
    if runproc.returncode==0:
      logfile.write('ERROR: can\'t write summary for checkpoint dataset '+str(cid)+' ('+str(dset)+')\n')
      continue

    logfile.write('Adding checkpoint dataset '+str(cid)+' ('+str(dset)+') to index\n')
    argv = [bindir+'/scr_index','-p',prefix,'--add='+str(dset)]
    runproc = subprocess.Popen(argv,bufsize=1,stdin=None,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell = True, universal_newlines = False)
    tempout = runproc.communicate()
    if len(tempout[1])>0:
      logfile.write(tempout[1])
    if len(tempout[0])>0:
      logfile.write(tempout[0])
    if runproc.returncode==0:
      logfile.write('Couldn\'t add checkpoint dataset '+str(cid)+' ('+str(dset)+') to index\n')
      continue

    logfile.write('Setting current checkpoint dataset to '+str(cid)+' ('+str(dset)+')\n')
    argv = [bindir+'/scr_index','-p',prefix,'--current='+str(dset)]
    runproc = subprocess.Popen(argv,bufsize=1,stdin=None,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell = True, universal_newlines = False)
    tempout = runproc.communicate()
    if len(tempout[1])>0:
      logfile.write(tempout[1])
    if len(tempout[0])>0:
      logfile.write(tempout[0])
    if runproc.returncode==0:
      logfile.write('Couldn\'t set current checkpoint dataset to '+str(cid)+' ('+str(dset)+')\n')

  logfile.write('All done, index now:\n')
  argv = [bindir+'/scr_index','-l','-p',prefix]
  runproc = subprocess.Popen(argv,bufsize=1,stdin=None,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell = True, universal_newlines = False)
  out = runproc.communicate()
  if len(out[1])>0:
    logfile.write(out[1])
  if len(out[0])>0:
    logfile.write(out[0])
  if runproc.returncode==0:
    logfile.write('Couldn\'t add checkpoint dataset '+str(cid)+' ('+str(dset)+') to index\n')

def scr_poststage(myprefix):
  # The full path to the dir containing the SCR binaries (scr_index and scr_flush_file)
  bindir=scr_const.X_BINDIR

  prefix=myprefix
  # Path to where you want the scr_poststage log
  logfile=prefix+'/scr_poststage.log'
  try:
    with open(logfile,'a') as logfile:
      do_poststage(bindir,prefix,logfile)
  except:
    pass

if __name__=='__main__':
  if len(sys.argv)!=2:
    print('USAGE')
    print('scr_poststage <prefix>')
    sys.exit(0)
  scr_poststage(sys.argv[1])

