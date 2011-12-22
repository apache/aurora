import os
import re
import subprocess
from collections import defaultdict
from twitter.common import app
from twitter.common.process import ProcessProviderFactory

app.add_option("--mesos_task_id_regex", dest = "mesos_task_id_regex", default = "/var/run/nexus/(?P<task_id>[^/]*)",
               help = "the regular expression of getting the mesos task id from path. " +
                      "The group name in regex has to be task_id. For example:/var/run/nexus/(?P<task_id>[^/]*)")
app.add_option("--ignore_user_name", dest = "ignore_user_name", default = "root",
              help = "Ignore the username, split by comma")


def get_running_mesos_processes(mesos_task_id_regex, ignore_users):
  task_info = {}
  ps = ProcessProviderFactory.get()
  ps.collect_all()
  p = re.compile(mesos_task_id_regex)
  for pid in ps.pids():
    handle = ps.get_handle(pid)
    if handle.user() in ignore_users:
      continue

    path = handle.cwd()
    if not path:
      continue

    matches = p.search(path)
    if not matches:
      continue

    task_id = matches.group('task_id')
    if not task_id:
      continue

    task_info[str(pid)] = task_id
  return task_info


def get_open_ports():
  port_info = defaultdict(set)
  netstat_cmd = 'netstat -lnp --protocol=inet,inet6'
  netstat = subprocess.Popen(netstat_cmd.split(),
    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  stdout, stderr = netstat.communicate()
  for line in stdout.split('\n'):
    components = line.split()
    port = components[3].split(':')[-1]
    pid = components[-1].split('/')[0]
    port_info[pid].add(port)
  return port_info


def main(args, opts):
  task_info = get_running_mesos_processes(
    opts.mesos_task_id_regex, opts.ignore_user_name.split(","))
  port_info = get_open_ports()

  for pid, task_id in task_info.items():
    ports = ['-1'] if pid not in port_info else port_info[pid]
    print "%s %s %s" % (pid, task_id, ','.join(ports))


app.main()