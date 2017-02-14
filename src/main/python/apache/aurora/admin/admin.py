#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import json
import optparse
import sys

from twitter.common import app, log
from twitter.common.quantity import Data, Time
from twitter.common.quantity.parse_simple import parse_data, parse_time

from apache.aurora.admin.admin_util import make_admin_client
from apache.aurora.client.api.sla import JobUpTimeLimit
from apache.aurora.client.base import (
    GROUPING_OPTION,
    check_and_log_response,
    combine_messages,
    die,
    get_grouping_or_die,
    requires
)
from apache.aurora.common.aurora_job_key import AuroraJobKey
from apache.aurora.common.clusters import CLUSTERS
from apache.aurora.common.shellify import shellify
from apache.aurora.config.resource import ResourceManager, ResourceType

from .admin_util import (
    FILENAME_OPTION,
    HOSTS_OPTION,
    format_sla_results,
    parse_hostnames,
    parse_hostnames_optional,
    parse_sla_percentage,
    print_results
)

from gen.apache.aurora.api.constants import ACTIVE_STATES, TERMINAL_STATES
from gen.apache.aurora.api.ttypes import ResponseCode, ScheduleStatus, TaskQuery

"""Command-line client for managing admin-only interactions with the aurora scheduler."""


MIN_SLA_INSTANCE_COUNT = optparse.Option(
    '--min_job_instance_count',
    dest='min_instance_count',
    type=int,
    default=10,
    help='Min job instance count to consider for SLA purposes. Default 10.'
)


def make_admin_client_with_options(cluster):
  options = app.get_options()

  return make_admin_client(
      cluster=cluster,
      verbose=getattr(options, 'verbosity', 'normal') == 'verbose',
      bypass_leader_redirect=options.bypass_leader_redirect)


@app.command
@app.command_option('--states', dest='states', default=None,
    help='Only match tasks with given state(s).')
@app.command_option('--role', dest='role', default=None,
    help='Only match tasks with given role.')
@app.command_option('--env', dest='environment', default=None,
    help='Only match tasks with given environment.')
@app.command_option('--limit', dest='limit', default=None, type=int,
    help='Limit the number of total tasks to prune.')
def prune_tasks(args, options):
  if len(args) == 0:
    die('Must specify at least cluster.')
  cluster = args[0]

  t = TaskQuery()
  if options.states:
    t.statuses = set(map(ScheduleStatus._NAMES_TO_VALUES.get, options.states.split(',')))
  if options.role:
    t.role = options.role
  if options.environment:
    t.environment = options.environment
  if options.limit:
    t.limit = options.limit

  api = make_admin_client_with_options(cluster)
  rsp = api.prune_tasks(t)
  if rsp.responseCode != ResponseCode.OK:
    die('Failed to prune tasks: %s' % combine_messages(rsp))
  else:
    print("Tasks pruned.")


@app.command
@app.command_option('--force', dest='force', default=False, action='store_true',
    help='Force expensive queries to run.')
@app.command_option('--shards', dest='shards', default=None,
    help='Only match given shards of a job.')
@app.command_option('--states', dest='states', default='RUNNING',
    help='Only match tasks with given state(s).')
@app.command_option('-l', '--listformat', dest='listformat',
    default="%role%/%name%/%instanceId% %status%",
    help='Format string of job/task items to print out.')
# TODO(ksweeney): Allow query by environment here.
def query(args, options):
  """usage: query [--force]
                  [--listformat=FORMAT]
                  [--shards=N[,N,...]]
                  [--states=State[,State,...]]
                  cluster [role [job]]

  Query Mesos about jobs and tasks.
  """
  def _convert_fmt_string(fmtstr):
    import re
    def convert(match):
      return "%%(%s)s" % match.group(1)
    return re.sub(r'%(\w+)%', convert, fmtstr)

  def flatten_task(t, d={}):
    for key in t.__dict__.keys():
      val = getattr(t, key)
      try:
        val.__dict__.keys()
      except AttributeError:
        d[key] = val
      else:
        flatten_task(val, d)

    return d

  def map_values(d):
    default_value = lambda v: v
    mapping = {
      'status': lambda v: ScheduleStatus._VALUES_TO_NAMES[v],
    }
    return dict(
      (k, mapping.get(k, default_value)(v)) for (k, v) in d.items()
    )

  for state in options.states.split(','):
    if state not in ScheduleStatus._NAMES_TO_VALUES:
      msg = "Unknown state '%s' specified.  Valid states are:\n" % state
      msg += ','.join(ScheduleStatus._NAMES_TO_VALUES.keys())
      die(msg)

  # Role, Job, Instances, States, and the listformat
  if len(args) == 0:
    die('Must specify at least cluster.')

  cluster = args[0]
  role = args[1] if len(args) > 1 else None
  job = args[2] if len(args) > 2 else None
  instances = set(map(int, options.shards.split(','))) if options.shards else set()

  if options.states:
    states = set(map(ScheduleStatus._NAMES_TO_VALUES.get, options.states.split(',')))
  else:
    states = ACTIVE_STATES | TERMINAL_STATES
  listformat = _convert_fmt_string(options.listformat)

  #  Figure out "expensive" queries here and bone if they do not have --force
  #  - Does not specify role
  if not role and not options.force:
    die('--force is required for expensive queries (no role specified)')

  #  - Does not specify job
  if not job and not options.force:
    die('--force is required for expensive queries (no job specified)')

  #  - Specifies status outside of ACTIVE_STATES
  if not (states <= ACTIVE_STATES) and not options.force:
    die('--force is required for expensive queries (states outside ACTIVE states')

  api = make_admin_client_with_options(cluster)

  query_info = api.query(TaskQuery(role=role, jobName=job, instanceIds=instances, statuses=states))
  if query_info.responseCode != ResponseCode.OK:
    die('Failed to query scheduler: %s' % combine_messages(query_info))

  tasks = query_info.result.scheduleStatusResult.tasks
  if tasks is None:
    return

  try:
    for task in tasks:
      d = flatten_task(task)
      print(listformat % map_values(d))
  except KeyError:
    msg = "Unknown key in format string.  Valid keys are:\n"
    msg += ','.join(d.keys())
    die(msg)


@app.command
@requires.exactly('cluster', 'role', 'cpu', 'ram', 'disk')
def set_quota(cluster, role, cpu_str, ram, disk):
  """usage: set_quota cluster role cpu ram[MGT] disk[MGT]

  Alters the amount of production quota allocated to a user.
  """
  try:
    ram_size = parse_data(ram).as_(Data.MB)
    disk_size = parse_data(disk).as_(Data.MB)
  except ValueError as e:
    die(str(e))

  try:
    cpu = float(cpu_str)
    ram_mb = int(ram_size)
    disk_mb = int(disk_size)
  except ValueError as e:
    die(str(e))

  resp = make_admin_client_with_options(cluster).set_quota(role, cpu, ram_mb, disk_mb)
  check_and_log_response(resp)


@app.command
@requires.exactly('cluster', 'role', 'cpu', 'ram', 'disk')
def increase_quota(cluster, role, cpu_str, ram_str, disk_str):
  """usage: increase_quota cluster role cpu ram[unit] disk[unit]

  Increases the amount of production quota allocated to a user.
  """
  cpu = float(cpu_str)
  ram = parse_data(ram_str).as_(Data.MB)
  disk = parse_data(disk_str).as_(Data.MB)

  client = make_admin_client_with_options(cluster)
  resp = client.get_quota(role)
  quota = resp.result.getQuotaResult.quota
  resource_details = ResourceManager.resource_details_from_quota(quota)
  log.info('Current quota for %s:\n\t%s' % (
      role,
      '\n\t'.join('%s\t%s%s' % (
          r.resource_type.display_name,
          r.value,
          r.resource_type.display_unit) for r in resource_details)))

  new_cpu = ResourceType.CPUS.value_type(
    cpu + ResourceManager.quantity_of(resource_details, ResourceType.CPUS))
  new_ram = ResourceType.RAM_MB.value_type(
    ram + ResourceManager.quantity_of(resource_details, ResourceType.RAM_MB))
  new_disk = ResourceType.DISK_MB.value_type(
    disk + ResourceManager.quantity_of(resource_details, ResourceType.DISK_MB))

  log.info('Attempting to update quota for %s to\n\tCPU\t%s\n\tRAM\t%s MB\n\tDisk\t%s MB' %
           (role, new_cpu, new_ram, new_disk))

  resp = client.set_quota(role, new_cpu, new_ram, new_disk)
  check_and_log_response(resp)


@app.command
@requires.exactly('cluster')
def scheduler_backup_now(cluster):
  """usage: scheduler_backup_now cluster

  Immediately initiates a full storage backup.
  """
  check_and_log_response(make_admin_client_with_options(cluster).perform_backup())


@app.command
@requires.exactly('cluster')
def scheduler_list_backups(cluster):
  """usage: scheduler_list_backups cluster

  Lists backups available for recovery.
  """
  resp = make_admin_client_with_options(cluster).list_backups()
  check_and_log_response(resp)
  backups = resp.result.listBackupsResult.backups
  print('%s available backups:' % len(backups))
  for backup in backups:
    print(backup)


@app.command
@requires.exactly('cluster', 'backup_id')
def scheduler_stage_recovery(cluster, backup_id):
  """usage: scheduler_stage_recovery cluster backup_id

  Stages a backup for recovery.
  """
  check_and_log_response(make_admin_client_with_options(cluster).stage_recovery(backup_id))


@app.command
@requires.exactly('cluster')
def scheduler_print_recovery_tasks(cluster):
  """usage: scheduler_print_recovery_tasks cluster

  Prints all active tasks in a staged recovery.
  """
  resp = make_admin_client_with_options(cluster).query_recovery(
      TaskQuery(statuses=ACTIVE_STATES))
  check_and_log_response(resp)
  log.info('Role\tJob\tShard\tStatus\tTask ID')
  for task in resp.result.queryRecoveryResult.tasks:
    assigned = task.assignedTask
    conf = assigned.task
    log.info('\t'.join((conf.job.role,
                        conf.job.name,
                        str(assigned.instanceId),
                        ScheduleStatus._VALUES_TO_NAMES[task.status],
                        assigned.taskId)))


@app.command
@requires.exactly('cluster', 'task_ids')
def scheduler_delete_recovery_tasks(cluster, task_ids):
  """usage: scheduler_delete_recovery_tasks cluster task_ids

  Deletes a comma-separated list of task IDs from a staged recovery.
  """
  ids = set(task_ids.split(','))
  check_and_log_response(make_admin_client_with_options(cluster).delete_recovery_tasks(
      TaskQuery(taskIds=ids)))


@app.command
@requires.exactly('cluster')
def scheduler_commit_recovery(cluster):
  """usage: scheduler_commit_recovery cluster

  Commits a staged recovery.
  """
  check_and_log_response(make_admin_client_with_options(cluster).commit_recovery())


@app.command
@requires.exactly('cluster')
def scheduler_unload_recovery(cluster):
  """usage: scheduler_unload_recovery cluster

  Unloads a staged recovery.
  """
  check_and_log_response(make_admin_client_with_options(cluster).unload_recovery())


@app.command
@requires.exactly('cluster')
def scheduler_snapshot(cluster):
  """usage: scheduler_snapshot cluster

  Request that the scheduler perform a storage snapshot and block until complete.
  """
  check_and_log_response(make_admin_client_with_options(cluster).snapshot())


@app.command
@requires.exactly('cluster')
@app.command_option('-t', '--type', default='explicit', choices=['implicit', 'explicit'],
    help='Type of reconciliation to run - implicit or explicit')
@app.command_option('-b', '--batch_size', default=None, type=int,
    help='Batch size for explicit reconciliation')
def reconcile_tasks(cluster):
  """usage: reconcile_tasks
            [--type=RECONCILIATION_TYPE]
            [--batch_size=BATCHSIZE]
            cluster

  Reconcile the Mesos master and the scheduler. Default runs explicit
  reconciliation with a batch size set in reconciliation_explicit_batch_size
  scheduler configuration option.
  """
  options = app.get_options()
  client = make_admin_client_with_options(cluster)
  if options.type == 'implicit':
    resp = client.reconcile_implicit()
  elif options.type == 'explicit':
    resp = client.reconcile_explicit(options.batch_size)
  else:
    die('Unexpected value for --type: %s' % options.type)
  check_and_log_response(resp)


@app.command
@app.command_option('-X', '--exclude_file', dest='exclude_filename', default=None,
    help='Exclusion filter. An optional text file listing host names (one per line)'
         'to exclude from the result set if found.')
@app.command_option('-x', '--exclude_hosts', dest='exclude_hosts', default=None,
    help='Exclusion filter. An optional comma-separated list of host names'
         'to exclude from the result set if found.')
@app.command_option(GROUPING_OPTION)
@app.command_option('-I', '--include_file', dest='include_filename', default=None,
    help='Inclusion filter. An optional text file listing host names (one per line)'
         'to include into the result set if found.')
@app.command_option('-i', '--include_hosts', dest='include_hosts', default=None,
    help='Inclusion filter. An optional comma-separated list of host names'
         'to include into the result set if found.')
@app.command_option('-l', '--list_jobs', dest='list_jobs', default=False, action='store_true',
    help='Lists all affected job keys with projected new SLAs if their tasks get killed'
         'in the following column format:\n'
         'HOST  JOB  PREDICTED_SLA  DURATION_SECONDS')
@app.command_option(MIN_SLA_INSTANCE_COUNT)
@app.command_option('-o', '--override_file', dest='override_filename', default=None,
    help='An optional text file to load job specific SLAs that will override'
         'cluster-wide command line percentage and duration values.'
         'The file can have multiple lines in the following format:'
         '"cluster/role/env/job percentage duration". Example: cl/mesos/prod/labrat 95 2h')
@requires.exactly('cluster', 'percentage', 'duration')
def sla_list_safe_domain(cluster, percentage, duration):
  """usage: sla_list_safe_domain
            [--exclude_file=FILENAME]
            [--exclude_hosts=HOSTS]
            [--grouping=GROUPING]
            [--include_file=FILENAME]
            [--include_hosts=HOSTS]
            [--list_jobs]
            [--min_job_instance_count=COUNT]
            [--override_jobs=FILENAME]
            cluster percentage duration

  Returns a list of relevant hosts where it would be safe to kill
  tasks without violating their job SLA. The SLA is defined as a pair of
  percentage and duration, where:

  percentage - Percentage of tasks required to be up within the duration.
  Applied to all jobs except those listed in --override_jobs file;

  duration - Time interval (now - value) for the percentage of up tasks.
  Applied to all jobs except those listed in --override_jobs file.
  Format: XdYhZmWs (each field is optional but must be in that order.)
  Examples: 5m, 1d3h45m.

  NOTE: if --grouping option is specified and is set to anything other than
        default (by_host) the results will be processed and filtered based
        on the grouping function on a all-or-nothing basis. In other words,
        the group is 'safe' IFF it is safe to kill tasks on all hosts in the
        group at the same time.
  """
  def parse_jobs_file(filename):
    result = {}
    with open(filename, 'r') as overrides:
      for line in overrides:
        if not line.strip():
          continue

        tokens = line.split()
        if len(tokens) != 3:
          die('Invalid line in %s:%s' % (filename, line))
        job_key = AuroraJobKey.from_path(tokens[0])
        result[job_key] = JobUpTimeLimit(
            job=job_key,
            percentage=parse_sla_percentage(tokens[1]),
            duration_secs=parse_time(tokens[2]).as_(Time.SECONDS)
        )
    return result

  options = app.get_options()

  sla_percentage = parse_sla_percentage(percentage)
  sla_duration = parse_time(duration)

  exclude_hosts = parse_hostnames_optional(options.exclude_hosts, options.exclude_filename)
  include_hosts = parse_hostnames_optional(options.include_hosts, options.include_filename)
  override_jobs = parse_jobs_file(options.override_filename) if options.override_filename else {}
  get_grouping_or_die(options.grouping)

  vector = make_admin_client_with_options(cluster).sla_get_safe_domain_vector(
      options.min_instance_count,
      include_hosts)
  groups = vector.get_safe_hosts(sla_percentage, sla_duration.as_(Time.SECONDS),
      override_jobs, options.grouping)

  results = []
  for group in groups:
    for host in sorted(group.keys()):
      if exclude_hosts and host in exclude_hosts:
        continue

      if options.list_jobs:
        results.append('\n'.join(['%s\t%s\t%.2f\t%d' %
            (host, d.job.to_path(), d.percentage, d.duration_secs) for d in sorted(group[host])]))
      else:
        results.append('%s' % host)

  print_results(results)


@app.command
@app.command_option(FILENAME_OPTION)
@app.command_option(GROUPING_OPTION)
@app.command_option(HOSTS_OPTION)
@app.command_option(MIN_SLA_INSTANCE_COUNT)
@requires.exactly('cluster', 'percentage', 'duration')
def sla_probe_hosts(cluster, percentage, duration):
  """usage: sla_probe_hosts
            [--filename=FILENAME]
            [--grouping=GROUPING]
            [--hosts=HOSTS]
            [--min_job_instance_count=COUNT]
            cluster percentage duration

  Probes individual hosts with respect to their job SLA.
  Specifically, given a host, outputs all affected jobs with their projected SLAs
  if the host goes down. In addition, if a job's projected SLA does not clear
  the specified limits suggests the approximate time when that job reaches its SLA.

  Output format:
  HOST  JOB  PREDICTED_SLA  SAFE?  PREDICTED_SAFE_IN

  where:
  HOST - host being probed.
  JOB - job that has tasks running on the host being probed.
  PREDICTED_SLA - predicted effective percentage of up tasks if the host is shut down.
  SAFE? - PREDICTED_SLA >= percentage
  PREDICTED_SAFE_IN - expected wait time in seconds for the job to reach requested SLA threshold.
  """
  options = app.get_options()

  sla_percentage = parse_sla_percentage(percentage)
  sla_duration = parse_time(duration)
  hosts = parse_hostnames(options.filename, options.hosts)
  get_grouping_or_die(options.grouping)

  vector = make_admin_client_with_options(cluster).sla_get_safe_domain_vector(
      options.min_instance_count,
      hosts)
  groups = vector.probe_hosts(sla_percentage, sla_duration.as_(Time.SECONDS), options.grouping)

  output, _ = format_sla_results(groups)
  print_results(output)


@app.command
@app.command_option('--sh', default=False, action="store_true",
  help="Emit a shell script instead of JSON.")
@app.command_option('--export', default=False, action="store_true",
  help="Emit a shell script prefixed with 'export'.")
@requires.exactly('cluster')
def get_cluster_config(cluster):
  """usage: get_cluster_config [--sh] [--export] CLUSTER

  Dumps the configuration for CLUSTER. By default we emit a json blob to stdout equivalent to
  an entry in clusters.json. With --sh a shell script is written to stdout that can be used
  with eval in a script to load the cluster config. With --export the shell script is prefixed
  with 'export '."""
  options = app.get_options()
  cluster = CLUSTERS[cluster]
  if not options.sh:
    json.dump(cluster, sys.stdout)
  else:
    for line in shellify(cluster, options.export, prefix="AURORA_CLUSTER_"):
      print(line)


@app.command
@requires.exactly('cluster')
def get_scheduler(cluster):
  """usage: get_scheduler CLUSTER

  Dumps the leading scheduler endpoint URL.
  """
  print("Found leading scheduler at: %s" %
      make_admin_client_with_options(cluster).scheduler_proxy.scheduler_client().raw_url)
