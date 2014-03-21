#
# Copyright 2013 Apache Software Foundation
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

"""Command-line client for managing jobs with the Aurora scheduler.
"""

from __future__ import print_function

import collections
from datetime import datetime
import json
import os
import pprint
import subprocess
import sys
import time
from tempfile import NamedTemporaryFile

from apache.aurora.client.base import (
    check_and_log_response,
    deprecation_warning,
    die,
    handle_open,
    requires,
    synthesize_url)
from apache.aurora.client.api.disambiguator import LiveJobDisambiguator
from apache.aurora.client.api.job_monitor import JobMonitor
from apache.aurora.client.api.quota_check import print_quota
from apache.aurora.client.api.updater_util import UpdaterConfig
from apache.aurora.client.config import get_config, GlobalHookRegistry
from apache.aurora.client.factory import make_client, make_client_factory
from apache.aurora.client.options import (
    CLUSTER_CONFIG_OPTION,
    CLUSTER_INVOKE_OPTION,
    CLUSTER_NAME_OPTION,
    DISABLE_HOOKS_OPTION,
    ENV_CONFIG_OPTION,
    ENVIRONMENT_BIND_OPTION,
    FROM_JOBKEY_OPTION,
    HEALTH_CHECK_INTERVAL_SECONDS_OPTION,
    JSON_OPTION,
    OPEN_BROWSER_OPTION,
    SHARDS_OPTION,
    WAIT_UNTIL_OPTION)
from apache.aurora.common.aurora_job_key import AuroraJobKey

from gen.apache.aurora.constants import ACTIVE_STATES, CURRENT_API_VERSION, AURORA_EXECUTOR_NAME
from gen.apache.aurora.ttypes import ExecutorConfig, ResponseCode, ScheduleStatus

from twitter.common import app, log
from twitter.common.python.pex import PexInfo
from twitter.common.python.dirwrapper import PythonDirectoryWrapper


def get_job_config(job_spec, config_file, options):
  try:
    job_key = AuroraJobKey.from_path(job_spec)
    select_cluster = job_key.cluster
    select_env = job_key.env
    select_role = job_key.role
    jobname = job_key.name
  except AuroraJobKey.Error:
    deprecation_warning('Please refer to your job in CLUSTER/ROLE/ENV/NAME format.')
    select_cluster = options.cluster if options.cluster else None
    select_env = options.env
    select_role = None
    jobname = job_spec
  try:
    json_option = options.json
  except AttributeError:
    json_option = False
  try:
    bindings = options.bindings
  except AttributeError:
    bindings = ()
  return get_config(
      jobname,
      config_file,
      json_option,
      bindings,
      select_cluster=select_cluster,
      select_role=select_role,
      select_env=select_env)

@app.command
def version(args):
  """usage: version

  Prints information about the version of the aurora client being run.
  """
  try:
    pexpath = sys.argv[0]
    pex_info = PexInfo.from_pex(PythonDirectoryWrapper.get(pexpath))
    print("Aurora client build info:")
    print("\tsha: %s" % pex_info.build_properties['sha'])
    print("\tdate: %s" % pex_info.build_properties['date'])
  except (IOError, PythonDirectoryWrapper.Error):
    print("Aurora client build info not available")
  print("Aurora API version: %s" % CURRENT_API_VERSION)


def maybe_disable_hooks(options):
  """Checks the hooks disable option, and disables the hooks if required.
  This could be done with a callback in the option, but this is better for the way that
  we test clientv1.
  """
  if options.disable_all_hooks_reason is not None:
    GlobalHookRegistry.disable_hooks()
    log.info('Client hooks disabled; reason given by user: %s' % options.disable_all_hooks_reason)


@app.command
@app.command_option(ENVIRONMENT_BIND_OPTION)
@app.command_option(OPEN_BROWSER_OPTION)
@app.command_option(CLUSTER_CONFIG_OPTION)
@app.command_option(ENV_CONFIG_OPTION)
@app.command_option(JSON_OPTION)
@app.command_option(WAIT_UNTIL_OPTION)
@app.command_option(DISABLE_HOOKS_OPTION)
@requires.exactly('cluster/role/env/job', 'config')
def create(job_spec, config_file):
  """usage: create cluster/role/env/job config

  Creates a job based on a configuration file.
  """
  options = app.get_options()
  maybe_disable_hooks(options)
  try:
    config = get_job_config(job_spec, config_file, options)
  except ValueError as v:
    print("Error: %s" % v)
    sys.exit(1)
  api = make_client(config.cluster())
  monitor = JobMonitor(api, config.role(), config.environment(), config.name())
  resp = api.create_job(config)
  check_and_log_response(resp)
  handle_open(api.scheduler_proxy.scheduler_client().url, config.role(), config.environment(),
      config.name())
  if options.wait_until == 'RUNNING':
    monitor.wait_until(monitor.running_or_finished)
  elif options.wait_until == 'FINISHED':
    monitor.wait_until(monitor.terminal)


@app.command
@app.command_option(ENVIRONMENT_BIND_OPTION)
@app.command_option(CLUSTER_CONFIG_OPTION)
@app.command_option(ENV_CONFIG_OPTION)
@app.command_option(JSON_OPTION)
@app.command_option(FROM_JOBKEY_OPTION)
@requires.exactly('cluster/role/env/job', 'config')
def diff(job_spec, config_file):
  """usage: diff cluster/role/env/job config

  Compares a job configuration against a running job.
  By default the diff will be displayed using 'diff', though you may choose an alternate
  diff program by specifying the DIFF_VIEWER environment variable."""
  options = app.get_options()
  config = get_job_config(job_spec, config_file, options)
  if options.rename_from:
    cluster, role, env, name = options.rename_from
  else:
    cluster = config.cluster()
    role = config.role()
    env = config.environment()
    name = config.name()
  api = make_client(cluster)
  resp = api.query(api.build_query(role, name, statuses=ACTIVE_STATES, env=env))
  if resp.responseCode != ResponseCode.OK:
    die('Request failed, server responded with "%s"' % resp.message)
  remote_tasks = [t.assignedTask.task for t in resp.result.scheduleStatusResult.tasks]
  resp = api.populate_job_config(config)
  if resp.responseCode != ResponseCode.OK:
    die('Request failed, server responded with "%s"' % resp.message)
  local_tasks = resp.result.populateJobResult.populated

  pp = pprint.PrettyPrinter(indent=2)
  def pretty_print_task(task):
    # The raw configuration is not interesting - we only care about what gets parsed.
    task.configuration = None
    task.executorConfig = ExecutorConfig(
        name=AURORA_EXECUTOR_NAME,
        data=json.loads(task.executorConfig.data))
    return pp.pformat(vars(task))

  def pretty_print_tasks(tasks):
    return ',\n'.join([pretty_print_task(t) for t in tasks])

  def dump_tasks(tasks, out_file):
    out_file.write(pretty_print_tasks(tasks))
    out_file.write('\n')
    out_file.flush()

  diff_program = os.environ.get('DIFF_VIEWER', 'diff')
  with NamedTemporaryFile() as local:
    dump_tasks(local_tasks, local)
    with NamedTemporaryFile() as remote:
      dump_tasks(remote_tasks, remote)
      result = subprocess.call([diff_program, remote.name, local.name])
      # Unlike most commands, diff doesn't return zero on success; it returns
      # 1 when a successful diff is non-empty.
      if result != 0 and result != 1:
        return result
      else:
        return 0


@app.command(name='open')
def do_open(args, _):
  """usage: open cluster[/role[/env/job]]

  Opens the scheduler page for a cluster, role or job in the default web browser.
  """
  cluster_name = role = env = job = None
  args = args[0].split("/")
  if len(args) > 0:
    cluster_name = args[0]
    if len(args) > 1:
      role = args[1]
      if len(args) > 2:
        env = args[2]
        if len(args) > 3:
          job = args[3]
        else:
          # TODO(ksweeney): Remove this after MESOS-2945 is completed.
          die('env scheduler pages are not yet implemented, please specify job')

  if not cluster_name:
    die('cluster is required')

  api = make_client(cluster_name)

  import webbrowser
  webbrowser.open_new_tab(
      synthesize_url(api.scheduler_proxy.scheduler_client().url, role, env, job))


@app.command
@app.command_option('--local', dest='local', default=False, action='store_true',
    help='Inspect the configuration as would be created by the "spawn" command.')
@app.command_option('--raw', dest='raw', default=False, action='store_true',
    help='Show the raw configuration.')
@app.command_option(ENVIRONMENT_BIND_OPTION)
@app.command_option(CLUSTER_CONFIG_OPTION)
@app.command_option(ENV_CONFIG_OPTION)
@app.command_option(JSON_OPTION)
@requires.exactly('cluster/role/env/job', 'config')
def inspect(job_spec, config_file):
  """usage: inspect cluster/role/env/job config

  Verifies that a job can be parsed from a configuration file, and displays
  the parsed configuration.
  """
  options = app.get_options()
  config = get_job_config(job_spec, config_file, options)
  if options.raw:
    print('Parsed job config: %s' % config.job())
    return

  job_thrift = config.job()
  job = config.raw()
  job_thrift = config.job()
  print('Job level information')
  print('  name:       %s' % job.name())
  print('  role:       %s' % job.role())
  print('  contact:    %s' % job.contact())
  print('  cluster:    %s' % job.cluster())
  print('  instances:  %s' % job.instances())
  if job.has_cron_schedule():
    print('  cron:')
    print('     schedule: %s' % job.cron_schedule())
    print('     policy:   %s' % job.cron_collision_policy())
  if job.has_constraints():
    print('  constraints:')
    for constraint, value in job.constraints().get().items():
      print('    %s: %s' % (constraint, value))
  print('  service:    %s' % job_thrift.taskConfig.isService)
  print('  production: %s' % bool(job.production().get()))
  print()

  task = job.task()
  print('Task level information')
  print('  name: %s' % task.name())
  if len(task.constraints().get()) > 0:
    print('  constraints:')
    for constraint in task.constraints():
      print('    %s' % (' < '.join(st.get() for st in constraint.order())))
  print()

  processes = task.processes()
  for process in processes:
    print('Process %s:' % process.name())
    if process.daemon().get():
      print('  daemon')
    if process.ephemeral().get():
      print('  ephemeral')
    if process.final().get():
      print('  final')
    print('  cmdline:')
    for line in process.cmdline().get().splitlines():
      print('    ' + line)
    print()


@app.command
@app.command_option(CLUSTER_INVOKE_OPTION)
@app.command_option(OPEN_BROWSER_OPTION)
@app.command_option(DISABLE_HOOKS_OPTION)
def start_cron(args, options):
  """usage: start_cron cluster/role/env/job

  Invokes a cron job immediately, out of its normal cron cycle.
  This does not affect the cron cycle in any way.
  """
  maybe_disable_hooks(options)
  api, job_key, config_file = LiveJobDisambiguator.disambiguate_args_or_die(
      args, options, make_client_factory())
  config = get_job_config(job_key.to_path(), config_file, options) if config_file else None
  resp = api.start_cronjob(job_key, config=config)
  check_and_log_response(resp)
  handle_open(api.scheduler_proxy.scheduler_client().url, job_key.role, job_key.env, job_key.name)


@app.command
@app.command_option(
    '--pretty',
    dest='pretty',
    default=False,
    action='store_true',
    help='Show job information in prettyprinted format')
@app.command_option(
    '--show-cron',
    '-c',
    dest='show_cron_schedule',
    default=False,
    action='store_true',
    help='List jobs registered with the Aurora scheduler')
@requires.exactly('cluster/role')
def list_jobs(cluster_and_role):
  """usage: list_jobs [--show-cron] cluster/role/env/job

  Shows all jobs that match the job-spec known by the scheduler.
  If --show-cron is specified, then also shows the registered cron schedule.
  """
  def show_job_simple(job):
    if options.show_cron_schedule:
      print(('{0}/{1.key.role}/{1.key.environment}/{1.key.name}' +
          '\t\'{1.cronSchedule}\'\t{1.cronCollisionPolicy}').format(cluster, job))
    else:
      print('{0}/{1.key.role}/{1.key.environment}/{1.key.name}'.format(cluster, job))

  def show_job_pretty(job):
    print("Job %s/%s/%s/%s:" %
        (cluster, job.key.role, job.key.environment, job.key.name))
    print('\tcron schedule: %s' % job.cronSchedule)
    print('\tcron policy:   %s' % job.cronCollisionPolicy)

  options = app.get_options()
  if options.show_cron_schedule and options.pretty:
    print_fn = show_job_pretty
  else:
    print_fn = show_job_simple
  # Take the cluster_and_role parameter, and split it into its two components.
  if cluster_and_role.count('/') != 1:
    die('list_jobs parameter must be in cluster/role format')
  (cluster,role) = cluster_and_role.split('/')
  api = make_client(cluster)
  resp = api.get_jobs(role)
  check_and_log_response(resp)
  for job in resp.result.getJobsResult.configs:
    print_fn(job)


@app.command
@app.command_option(CLUSTER_INVOKE_OPTION)
@app.command_option(OPEN_BROWSER_OPTION)
@app.command_option(SHARDS_OPTION)
@app.command_option(DISABLE_HOOKS_OPTION)
def kill(args, options):
  """usage: kill --shards=shardspec cluster/role/env/job

  Kills a group of tasks in a running job, blocking until all specified tasks have terminated.

  """
  maybe_disable_hooks(options)
  if options.shards is None:
    print('Shards option is required for kill; use killall to kill all shards', file=sys.stderr)
    exit(1)
  api, job_key, config_file = LiveJobDisambiguator.disambiguate_args_or_die(
      args, options, make_client_factory())
  options = app.get_options()
  config = get_job_config(job_key.to_path(), config_file, options) if config_file else None
  resp = api.kill_job(job_key, options.shards, config=config)
  check_and_log_response(resp)
  handle_open(api.scheduler_proxy.scheduler_client().url, job_key.role, job_key.env, job_key.name)

@app.command
@app.command_option(CLUSTER_INVOKE_OPTION)
@app.command_option(OPEN_BROWSER_OPTION)
@app.command_option(DISABLE_HOOKS_OPTION)
def killall(args, options):
  """usage: killall cluster/role/env/job
  Kills all tasks in a running job, blocking until all specified tasks have been terminated.
  """
  maybe_disable_hooks(options)
  job_key = AuroraJobKey.from_path(args[0])
  config_file = args[1] if len(args) > 1 else None  # the config for hooks
  config = get_job_config(job_key.to_path(), config_file, options) if config_file else None
  api = make_client(job_key.cluster)
  resp = api.kill_job(job_key, None, config=config)
  check_and_log_response(resp)
  handle_open(api.scheduler_proxy.scheduler_client().url, job_key.role, job_key.env, job_key.name)


@app.command
@app.command_option(CLUSTER_INVOKE_OPTION)
def status(args, options):
  """usage: status cluster/role/env/job

  Fetches and prints information about the active tasks in a job.
  """
  def is_active(task):
    return task.status in ACTIVE_STATES

  def print_task(scheduled_task):
    assigned_task = scheduled_task.assignedTask
    taskInfo = assigned_task.task
    taskString = ''
    if taskInfo:
      taskString += '''cpus: %s, ram: %s MB, disk: %s MB''' % (taskInfo.numCpus,
                                                               taskInfo.ramMb,
                                                               taskInfo.diskMb)
    if assigned_task.assignedPorts:
      taskString += '\n\tports: %s' % assigned_task.assignedPorts
    taskString += '\n\tfailure count: %s (max %s)' % (scheduled_task.failureCount,
                                                      taskInfo.maxTaskFailures)
    taskString += '\n\tevents:'
    for event in scheduled_task.taskEvents:
      taskString += '\n\t\t %s %s: %s' % (datetime.fromtimestamp(event.timestamp / 1000),
                                          ScheduleStatus._VALUES_TO_NAMES[event.status],
                                          event.message)
    taskString += '\n\tpackages:'
    if assigned_task.task.packagesDEPRECATED is not None:
      for pkg in assigned_task.task.packagesDEPRECATED:
        taskString += ('\n\t\trole: %s, package: %s, version: %s' % (pkg.role, pkg.name, pkg.version))

    return taskString

  def print_tasks(tasks):
    for task in tasks:
      taskString = print_task(task)

      log.info('role: %s, env: %s, name: %s, shard: %s, status: %s on %s\n%s' %
             (task.assignedTask.task.owner.role,
              task.assignedTask.task.environment,
              task.assignedTask.task.jobName,
              task.assignedTask.instanceId,
              ScheduleStatus._VALUES_TO_NAMES[task.status],
              task.assignedTask.slaveHost,
              taskString))
      if task.assignedTask.task.packagesDEPRECATED is not None:
        for pkg in task.assignedTask.task.packagesDEPRECATED:
          log.info('\tpackage %s/%s/%s' % (pkg.role, pkg.name, pkg.version))

  api, job_key, _ = LiveJobDisambiguator.disambiguate_args_or_die(
      args, options, make_client_factory())
  resp = api.check_status(job_key)
  check_and_log_response(resp)

  tasks = resp.result.scheduleStatusResult.tasks
  if tasks:
    active_tasks = filter(is_active, tasks)
    log.info('Active Tasks (%s)' % len(active_tasks))
    print_tasks(active_tasks)
    inactive_tasks = filter(lambda x: not is_active(x), tasks)
    log.info('Inactive Tasks (%s)' % len(inactive_tasks))
    print_tasks(inactive_tasks)
  else:
    log.info('No tasks found.')


@app.command
@app.command_option(SHARDS_OPTION)
@app.command_option(ENVIRONMENT_BIND_OPTION)
@app.command_option(CLUSTER_CONFIG_OPTION)
@app.command_option(ENV_CONFIG_OPTION)
@app.command_option(JSON_OPTION)
@app.command_option(HEALTH_CHECK_INTERVAL_SECONDS_OPTION)
@app.command_option(DISABLE_HOOKS_OPTION)
@app.command_option(
    '--force',
    dest='force',
    default=True,  # TODO(maximk): Temporary bandaid for MESOS-4310 until a better fix is available.
    action='store_true',
    help='Turn off warning message that the update looks large enough to be disruptive.')
@requires.exactly('cluster/role/env/job', 'config')
def update(job_spec, config_file):
  """usage: update cluster/role/env/job config

  Performs a rolling upgrade on a running job, using the update configuration
  within the config file as a control for update velocity and failure tolerance.

  Updates are fully controlled client-side, so aborting an update halts the
  update and leaves the job in a 'locked' state on the scheduler.
  Subsequent update attempts will fail until the update is 'unlocked' using the
  'cancel_update' command.

  The updater only takes action on shards in a job that have changed, meaning
  that changing a single shard will only induce a restart on the changed shard.

  You may want to consider using the 'diff' subcommand before updating,
  to preview what changes will take effect.
  """
  def warn_if_dangerous_change(api, job_spec, config):
    # Get the current job status, so that we can check if there's anything
    # dangerous about this update.
    job_key = AuroraJobKey(config.cluster(), config.role(), config.environment(), config.name())
    resp = api.query(api.build_query(config.role(), config.name(),
        statuses=ACTIVE_STATES, env=config.environment()))
    if resp.responseCode != ResponseCode.OK:
      die('Could not get job status from server for comparison: %s' % resp.message)
    remote_tasks = [t.assignedTask.task for t in resp.result.scheduleStatusResult.tasks]
    resp = api.populate_job_config(config)
    if resp.responseCode != ResponseCode.OK:
      die('Server could not populate job config for comparison: %s' % resp.message)
    local_task_count = len(resp.result.populateJobResult.populated)
    remote_task_count = len(remote_tasks)
    if (local_task_count >= 4 * remote_task_count or local_task_count <= 4 * remote_task_count
        or local_task_count == 0):
      print('Warning: this update is a large change. Press ^c within 5 seconds to abort')
      time.sleep(5)

  options = app.get_options()
  maybe_disable_hooks(options)
  config = get_job_config(job_spec, config_file, options)
  api = make_client(config.cluster())
  if not options.force:
    warn_if_dangerous_change(api, job_spec, config)
  resp = api.update_job(config, options.health_check_interval_seconds, options.shards)
  check_and_log_response(resp)


@app.command
@app.command_option(CLUSTER_INVOKE_OPTION)
@app.command_option(HEALTH_CHECK_INTERVAL_SECONDS_OPTION)
@app.command_option(OPEN_BROWSER_OPTION)
@app.command_option(SHARDS_OPTION)
@app.command_option(
    '--batch_size',
    dest='batch_size',
    type=int,
    default=1,
    help='Number of shards to be restarted in one iteration.')
@app.command_option(
    '--max_per_shard_failures',
    dest='max_per_shard_failures',
    type=int,
    default=0,
    help='Maximum number of restarts per shard during restart. Increments total failure count when '
         'this limit is exceeded.')
@app.command_option(
    '--max_total_failures',
    dest='max_total_failures',
    type=int,
    default=0,
    help='Maximum number of shard failures to be tolerated in total during restart.')
@app.command_option(
    '--restart_threshold',
    dest='restart_threshold',
    type=int,
    default=60,
    help='Maximum number of seconds before a shard must move into the RUNNING state before '
         'considered a failure.')
@app.command_option(
    '--watch_secs',
    dest='watch_secs',
    type=int,
    default=30,
    help='Minimum number of seconds a shard must remain in RUNNING state before considered a '
         'success.')
@app.command_option(DISABLE_HOOKS_OPTION)
def restart(args, options):
  """usage: restart cluster/role/env/job
               [--shards=SHARDS]
               [--batch_size=INT]
               [--updater_health_check_interval_seconds=SECONDS]
               [--max_per_shard_failures=INT]
               [--max_total_failures=INT]
               [--restart_threshold=INT]
               [--watch_secs=SECONDS]

  Performs a rolling restart of shards within a job.

  Restarts are fully controlled client-side, so aborting halts the restart.
  """
  maybe_disable_hooks(options)
  api, job_key, config_file = LiveJobDisambiguator.disambiguate_args_or_die(
      args, options, make_client_factory())
  config = get_job_config(job_key.to_path(), config_file, options) if config_file else None
  updater_config = UpdaterConfig(
      options.batch_size,
      options.restart_threshold,
      options.watch_secs,
      options.max_per_shard_failures,
      options.max_total_failures)
  resp = api.restart(job_key, options.shards, updater_config,
      options.health_check_interval_seconds, config=config)
  check_and_log_response(resp)
  handle_open(api.scheduler_proxy.scheduler_client().url, job_key.role, job_key.env, job_key.name)


@app.command
@app.command_option(CLUSTER_INVOKE_OPTION)
def cancel_update(args, options):
  """usage: cancel_update cluster/role/env/job

  Unlocks a job for updates.
  A job may be locked if a client's update session terminated abnormally,
  or if another user is actively updating the job.  This command should only
  be used when the user is confident that they are not conflicting with another user.
  """
  api, job_key, config_file = LiveJobDisambiguator.disambiguate_args_or_die(
      args, options, make_client_factory())
  config = get_job_config(job_key.to_path(), config_file, options) if config_file else None
  resp = api.cancel_update(job_key, config=config)
  check_and_log_response(resp)


@app.command
@app.command_option(CLUSTER_NAME_OPTION)
@requires.exactly('role')
def get_quota(role):
  """usage: get_quota --cluster=CLUSTER role

  Prints the production quota that has been allocated to a user.
  """
  options = app.get_options()
  resp = make_client(options.cluster).get_quota(role)

  print_quota(resp.result.getQuotaResult.quota, 'Total allocated quota', role)

  if resp.result.getQuotaResult.consumed:
    print_quota(resp.result.getQuotaResult.consumed, 'Consumed quota', role)
