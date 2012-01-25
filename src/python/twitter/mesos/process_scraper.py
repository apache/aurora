"""Scans running processes and prints to stdout one line per process of the form:

[process id] [mesos task id] [comma-delimited list of listening ports]
"""

import os
import psutil
import re
import subprocess
from collections import defaultdict
from twitter.common import app
from twitter.common.process import ProcessProviderFactory

app.add_option('--mesos_task_id_regex',
               dest = 'mesos_task_id_regex',
               default = '/var/run/nexus/(?P<task_id>[^/]*)',
               help = 'the regular expression to extract the mesos task ID from a path. ' +
                      'A capture group named "task_id" must be present in the expression. ')
app.add_option('--ignore_user_name',
               dest = 'ignore_user_name',
               default = 'root',
               help = 'Process owner user names to avoid reporting.')

def main(args, opts):
  id_matcher = re.compile(opts.mesos_task_id_regex)
  ignore_users = opts.ignore_user_name.split(',')

  procs = [proc for proc in psutil.process_iter() if proc.username not in ignore_users]

  def get_taskid(cwd):
    match = id_matcher.match(cwd)
    return None if not match else match.group('task_id')

  procs_with_ids = [(proc, get_taskid(proc.getcwd())) for proc in procs]

  for proc_with_id in [p for p in procs_with_ids if p[1] is not None]:
    listen_ports = [conn.local_address[1] for conn in proc_with_id[0].get_connections()
                    if conn.status == 'LISTEN']
    print '%s %s %s' % (proc_with_id[0].pid, proc_with_id[1], ','.join(map(str, listen_ports)))


app.main()
