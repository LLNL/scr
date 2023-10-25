"""Parse an SCR log file and return a list of entries, one per record.

import scrlog
entries = scrlog.parse_file(logfile)
for e in entries:
  print e

Each entry is a dictionary with fields (types)
depending on its 'type' and 'label' values:

  e['type']      (str)      one of {"event", "xfer"}
  e['label']     (str)      e.g., "FETCH", "CHECKPOINT_START"
  e['jobid']     (str)      jobid string from resource manager
  e['from']      (str)      source path
  e['to']        (str)      destination path
  e['dset']      (int)      SCR dataset id
  e['secs']      (float)    number of seconds
  e['bytes']     (float)    number of bytes in dataset
  e['files']     (float)    number of files in dataset (float for easier math)
  e['timestamp'] (datetime)
  e['note']      (str)
  e['name']      (str)
"""

import re
from datetime import datetime, timedelta
from dateutil import parser

# define regular expressions to pluck out field values
re_time = re.compile(r'^(\d\d\d\d\-\d\d\-\d\dT\d\d:\d\d:\d\d)')
re_jobid = re.compile(r'^jobid=(.*)$')
re_event = re.compile(r'^event=(.*)$')
re_xfer = re.compile(r'^xfer=(.*)$')
re_from = re.compile(r'^from=(.*)$')
re_to = re.compile(r'^to=(.*)$')
re_secs = re.compile(r'^secs=(\d+\.\d+)$')
re_dset = re.compile(r'^dset=(\d+)$')
re_bytes = re.compile(r'^bytes=(\d+\.\d+)$')
re_files = re.compile(r'^files=(\d+)$')
re_name = re.compile(r'.* name=\"(.*?)\"')
re_note = re.compile(r'.* note=\"(.*?)\"')


# given a line, parse into a dictionary of key=value pairs
def parse_line(l):
    e = dict()

    # most fields we can split out by comma's
    # though there are some fields whose values may have embedded commas

    # handle fields that we can get by splitting commas
    parts = l.split(", ")
    for p in parts:
        m = re_jobid.match(p)
        if m:
            jobid = m.groups(1)[0]
            e['jobid'] = jobid
            continue

        m = re_event.match(p)
        if m:
            label = m.groups(1)[0]
            e['type'] = 'event'
            e['label'] = label
            continue

        m = re_xfer.match(p)
        if m:
            label = m.groups(1)[0]
            e['type'] = 'xfer'
            e['label'] = label
            continue

        m = re_from.match(p)
        if m:
            path = m.groups(1)[0]
            e['from'] = path
            continue

        m = re_to.match(p)
        if m:
            path = m.groups(1)[0]
            e['to'] = path
            continue

        m = re_dset.match(p)
        if m:
            dset = int(m.groups(1)[0])
            e['dset'] = dset
            continue

        m = re_secs.match(p)
        if m:
            secs = float(m.groups(1)[0])
            e['secs'] = secs
            continue

        m = re_bytes.match(p)
        if m:
            byte_count = float(m.groups(1)[0])
            e['bytes'] = byte_count
            continue

        m = re_files.match(p)
        if m:
            files = float(m.groups(1)[0])
            e['files'] = files
            continue

    # handle fields that might have commas
    m = re_time.match(l)
    if m:
        timestamp = m.groups(1)[0]
        dt = parser.parse(timestamp)
        e['timestamp'] = dt

    m = re_note.match(l)
    if m:
        note = m.groups(1)[0]
        e['note'] = note

    m = re_name.match(l)
    if m:
        name = m.groups(1)[0]
        e['name'] = name

    return e


def parse_file(filename):
    entries = []
    with open(filename) as f:
        for l in f.readlines():
            e = parse_line(l)
            entries.append(e)
    return entries
