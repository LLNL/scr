"""Given an SCR log file, estimates optimum checkpoint interval.

Given an SCR log file, estimates optimum checkpoint interval
based on checkpoint cost and mean time to interrupt using traditional models.

The intent is for someone to include this in a job script like so:
  export SCR_CHECKPOINT_SECONDS=`python scr_ckpt_interval.py`
"""

import sys
import os
import math
import argparse

import scrjob.scrlog

# TODO: Return a reasonable default value when no log file exists.

# TODO: This treats the end of the allocation as a failure. Since SCR
# normally saves a checkpoint there, we can exclude those and just look
# for true failure cases. Perhaps count up number of starts minus number
# of SCR_FINALIZE_CALLED?

parser = argparse.ArgumentParser(
    description=
    "Given an SCR log file, estimate optimum checkpoint interval based on checkpoint cost and mean time to interrupt.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--stats',
                    help='print stats for checkpoint cost and failure rate',
                    action='store_true')
parser.add_argument('--model',
                    help='model to compute optimum checkpoint interval',
                    type=str,
                    choices=['young', 'daly'],
                    default='daly')
parser.add_argument(
    '--percent',
    help='express optimum checkpoint interval as percent overhead',
    action='store_true')
parser.add_argument('--prefix',
                    help='prefix directory to look for log file',
                    type=str)
parser.add_argument('--logfile', help='path to log file', type=str)
args = parser.parse_args(sys.argv[1:])

# count up number of times job has started
num_starts = 0.0

# total time spent during fetch and number of times fetch was executed
# this will count towards restart cost
fetch_secs = 0.0
fetch_count = 0.0

# total time spent during rebuild and number of times rebuild was executed
# this will count towards restart cost
rebuild_secs = 0.0
rebuild_count = 0.0

# total time spent during compute phases number of compute phases
# counts toward compute time
compute_secs = 0.0
compute_count = 0.0

# total time spent during checkpoint phases number of checkpoint phases
# we'll include this in the checkpoint cost
checkpoint_secs = 0.0
checkpoint_count = 0.0

# total time spent flushing checkpoints and number of flushed checkpoints
# we'll include this in the checkpoint cost
flush_ckpt_secs = 0.0
flush_ckpt_count = 0.0

# total time spent flushing output and number of flushed outputs (non-checkpoints)
# this will be part of the "compute" time
flush_output_secs = 0.0
flush_output_count = 0.0

# tracks whether we are in a compute or checkpoint phase
state = None

# parse log file and get list of entries
filename = os.path.join('.scr', 'log')
if args.prefix:
    filename = os.path.join(args.prefix, filename)
if args.logfile:
    filename = args.logfile
try:
    entries = scrlog.parse_file(filename)
except:
    if args.stats:
        print("ERROR: failed to parse log file:", filename)
        print("Run this command from within the prefix directory of the job,")
        print(
            "specify the prefix directory with --prefix, or provide the path to the log file with --logfile"
        )
    else:
        # TODO: check for .scr, if we find that assume this is the first run, if not, report an error
        # for the first run, let's go wtih 10% overhead until we accumulate some data
        print("10.0")
    quit()

# run over entries and add up time and number of times we executed different phases
for e in entries:
    dt = e['timestamp']

    # count number of times we see event=START signaling start of new run
    if e['label'] == 'START':
        num_starts += 1.0
        continue

    # fetch time and count
    if e['label'] == 'FETCH':
        secs = e['secs']
        fetch_secs += secs
        fetch_count += 1.0
        continue

    # rebuild time and count
    if e['label'] == 'RESTART_SUCCESS':
        secs = e['secs']
        rebuild_secs += secs
        rebuild_count += 1.0
        continue

    # rebuild time and count
    if e['label'] == 'RESTART_FAILURE':
        secs = e['secs']
        rebuild_secs += secs
        rebuild_count += 1.0
        continue

    # start of compute phase
    if e['label'] == 'COMPUTE_START':
        state = 'compute'
        continue

    # end of compute phase
    if e['label'] == 'COMPUTE_END':
        secs = e['secs']
        compute_secs += secs
        compute_count += 1.0
        continue

    # start of checkpoint phase
    if e['label'] == 'CHECKPOINT_START':
        state = 'checkpoint'
        continue

    # end of checkpoint phase
    if e['label'] == 'CHECKPOINT_END':
        secs = e['secs']
        checkpoint_secs += secs
        checkpoint_count += 1.0
        continue

    # flush time and count
    # if in checkpoint, add to checkpoint time, if in output add to compute time
    if e['label'] == 'FLUSH_SYNC':
        secs = e['secs']
        if state == 'checkpoint':
            flush_ckpt_secs += secs
            flush_ckpt_count += 1.0
        else:
            flush_output_secs += secs
            flush_output_count += 1.0
        continue

#  print e

fetch_cost = fetch_secs
if fetch_count > 0.0:
    fetch_cost /= fetch_count

rebuild_cost = rebuild_secs
if rebuild_count > 0.0:
    rebuild_cost /= rebuild_count

compute_cost = compute_secs
if compute_count > 0.0:
    compute_cost /= compute_count

# We're not accounting for multi-level checkpointing right now.
# For an approximate single-level cost, sum time spent in checkpoints (write + encode)
# and all time flushing checkpoints (but not pure output) and divide by number of tmies we've checkpointed.
checkpoint_cost = checkpoint_secs + flush_ckpt_secs
if checkpoint_count > 0.0:
    checkpoint_cost /= checkpoint_count

flush_ckpt_cost = flush_ckpt_secs
if flush_ckpt_count > 0.0:
    flush_ckpt_cost /= flush_ckpt_count

flush_output_cost = flush_output_secs
if flush_output_count > 0.0:
    flush_output_cost /= flush_output_count

if args.stats:
    print("Fetch time total/avg (s): ", fetch_secs, fetch_cost)
    print("Rebuild time total/avg (s): ", rebuild_secs, rebuild_cost)
    print("Compute time total/avg (s): ", compute_secs, compute_cost)
    print("Checkpoint time total/avg (s): ", checkpoint_secs, checkpoint_cost)
    print("Flush checkpoint time total/avg (s): ", flush_ckpt_secs,
          flush_ckpt_cost)
    print("Flush output time total/avg (s): ", flush_output_secs,
          flush_output_cost)

# for average time before failure, we use total runtime
# divided by the number of job starts
total_secs = fetch_secs + rebuild_secs + compute_secs + checkpoint_secs + flush_ckpt_secs + flush_output_secs
avg_secs_before_failure = total_secs
if num_starts > 0.0:
    avg_secs_before_failure /= num_starts
if args.stats:
    print("Starts: ", num_starts)
    print("Total time (s): " + str(total_secs))
    print("Mean time to interrupt (s): " + str(avg_secs_before_failure))

if args.model == 'young':
    # "A First Order Approximation to the Optimum Checkpoint Interval",
    # John Young, 1976.
    opt_checkpoint_secs = math.sqrt(2.0 * checkpoint_cost *
                                    avg_secs_before_failure)
    opt_checkpoint_overhead = checkpoint_cost * 100.0 / opt_checkpoint_secs
    if args.stats:
        print("Model = Young")
        print("Checkpoint Interval (s) = " + str(opt_checkpoint_secs))
        print("Percent Overhead = " + str(opt_checkpoint_overhead))
    else:
        if args.percent:
            # print overhead percentage as float
            print(str(opt_checkpoint_overhead))
        else:
            # print seconds as int
            print(str(int(opt_checkpoint_secs)))

if args.model == 'daly':
    # "A Higher Order Estimate of the Optimum Checkpoint Interval for Restart Dumps",
    # John Daly, 2004
    # See equation 37 from above paper
    M2 = 2.0 * avg_secs_before_failure  # 2M
    opt_checkpoint_secs = avg_secs_before_failure  # t_opt = M
    if checkpoint_cost < M2:  # if delta < 2M
        f = checkpoint_cost / M2  # delta / 2M
        opt_checkpoint_secs = math.sqrt(checkpoint_cost * M2) * (
            1.0 + math.sqrt(f) / 3.0 + f / 9.0) - checkpoint_cost
    opt_checkpoint_overhead = checkpoint_cost * 100.0 / opt_checkpoint_secs
    if args.stats:
        print("Model = Daly")
        print("Checkpoint Interval (s) = " + str(opt_checkpoint_secs))
        print("Percent Overhead = " + str(opt_checkpoint_overhead))
    else:
        if args.percent:
            # print overhead percentage as float
            print(str(opt_checkpoint_overhead))
        else:
            # print seconds as int
            print(str(int(opt_checkpoint_secs)))
