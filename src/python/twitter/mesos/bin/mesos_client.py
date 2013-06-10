"""Command-line client for managing jobs with the Twitter mesos scheduler.
"""

from __future__ import print_function

import collections
import copy
from datetime import datetime
import functools
import getpass
import optparse
from optparse import OptionValueError
import os
import pprint
import subprocess
import sys
from tempfile import NamedTemporaryFile
from time import gmtime, strftime
from urlparse import urljoin

from twitter.common import app, log
from twitter.common.dirutil import Fileset
from twitter.common.log.options import LogOptions
from twitter.common.quantity import Amount, Data
from twitter.common.quantity.parse_simple import parse_data_into
from twitter.mesos.client.api import MesosClientAPI
from twitter.mesos.client.base import check_and_log_response, die, requires
from twitter.mesos.client.command_runner import DistributedCommandRunner
from twitter.mesos.client.config import get_config
from twitter.mesos.client.disambiguator import LiveJobDisambiguator
from twitter.mesos.client.job_monitor import JobMonitor
from twitter.mesos.client.quickrun import Quickrun
from twitter.mesos.client.updater_util import UpdaterConfig
from twitter.mesos.common import (
    AuroraJobKey,
    Cluster,
    ClusterOption,
    Clusters)
# TODO(wickman) Split mesos_client / mesos_client_internal targets
from twitter.mesos.common_internal.clusters import TWITTER_CLUSTERS
from twitter.mesos.packer import sd_packer_client
from twitter.mesos.packer.packer_client import Packer
from twitter.thermos.base.options import add_binding_to

from gen.twitter.mesos.constants import ACTIVE_STATES, DEFAULT_ENVIRONMENT
from gen.twitter.mesos.ttypes import ScheduleStatus


try:
  from twitter.mesos.client.spawn_local import spawn_local
except ImportError:
  def spawn_local(*args, **kw):
    die('Local spawning not supported in this version of mesos client.')


DEFAULT_USAGE_BANNER = """
mesos client, used to interact with the aurora scheduler.

For questions contact mesos-team@twitter.com.
"""


def set_quiet(option, _1, _2, parser):
  setattr(parser.values, option.dest, 'quiet')
  LogOptions.set_stderr_log_level('NONE')


def set_verbose(option, _1, _2, parser):
  setattr(parser.values, option.dest, 'verbose')
  LogOptions.set_stderr_log_level('DEBUG')


app.add_option('-v',
               dest='verbosity',
               default='normal',
               action='callback',
               callback=set_verbose,
               help='Verbose logging. (default: %default)')
app.add_option('-q',
               dest='verbosity',
               default='normal',
               action='callback',
               callback=set_quiet,
               help='Quiet logging. (default: %default)')


# TODO(wickman) Add ClusterSetOption to twitter.mesos.common -- these should
# likely come in pairs for most tools.
MESOS_CLIENT_CLUSTER_SET = Clusters(TWITTER_CLUSTERS.values())
def set_cluster_config(option, _1, value, parser):
  if value.endswith('.json'):
    replacement = Clusters.from_json(value)
  elif value.endswith('.yml'):
    replacement = Clusters.from_yml(value)
  else:
    _, ext = os.path.splitext(value)
    raise TypeError('Unknown cluster configuration format: %s' % ext)
  MESOS_CLIENT_CLUSTER_SET.replace(replacement.values())


def mesos_client_cluster_provider(name):
  return MESOS_CLIENT_CLUSTER_SET[name]


app.add_option('--cluster_config',
               default=None,
               type='string',
               action='callback',
               callback=set_cluster_config,
               metavar='FILENAME',
               help='Use a different default cluster configuration.  WARNING: Only for testing.')


def make_client_factory():
  options = app.get_options()
  class TwitterMesosClientAPI(MesosClientAPI):
    def __init__(self, cluster, *args, **kw):
      super(TwitterMesosClientAPI, self).__init__(MESOS_CLIENT_CLUSTER_SET[cluster], *args, **kw)
  return functools.partial(TwitterMesosClientAPI, verbose=options.verbosity == 'verbose')


def make_client(cluster):
  factory = make_client_factory()
  return factory(cluster.name if isinstance(cluster, Cluster) else cluster)


def synthesize_url(scheduler_client, role=None, env=None, job=None):
  scheduler_url = scheduler_client.url
  if not scheduler_url:
    log.warning("Unable to find scheduler web UI!")
    return None

  if env and not role:
    die('If env specified, must specify role')
  if job and not (role and env):
    die('If job specified, must specify role and env')

  scheduler_url = urljoin(scheduler_url, 'scheduler')
  if role:
    scheduler_url += '/' + role
    if env:
      scheduler_url += '/' + env
      if job:
        scheduler_url += '/' + job
  return scheduler_url


def handle_open(scheduler_client, role, env, job):
  url = synthesize_url(scheduler_client, role, env, job)
  if url:
    log.info('Job url: %s' % url)
    if app.get_options().open_browser:
      import webbrowser
      webbrowser.open_new_tab(url)


OPEN_BROWSER_OPTION = optparse.Option(
    '-o',
    '--open_browser',
    dest='open_browser',
    action='store_true',
    default=False,
    help='Open a browser window to the job page after a job mutation.')


def shard_range_parser(shards):
  result = set()
  for part in shards.split(','):
    x = part.split('-')
    result.update(range(int(x[0]), int(x[-1])+1))
  return sorted(result)

def parse_shards_into(option, opt, value, parser):
  try:
    setattr(parser.values, option.dest, shard_range_parser(value))
  except ValueError as e:
    raise OptionValueError('Failed to parse: %s' % e)

SHARDS_OPTION = optparse.Option(
    '--shards',
    type='string',
    dest='shards',
    default=None,
    action='callback',
    callback=parse_shards_into,
    help='A list of shard ids to act on. Can either be a comma-separated list (e.g. 0,1,2) '
    'or a range (e.g. 0-2) or any combination of the two (e.g. 0-2,5,7-9). If not set, '
    'all shards will be acted on.')


JSON_OPTION = optparse.Option(
    '-j',
    '--json',
    dest='json',
    default=False,
    action='store_true',
    help='If specified, configuration is read in JSON format.')


CLUSTER_CONFIG_OPTION = ClusterOption(
  '--cluster',
  cluster_provider=mesos_client_cluster_provider,
  help='Cluster to match when selecting a job from a configuration. Optional if only one job '
       'matching the given job name exists in the config.')

CLUSTER_INVOKE_OPTION = ClusterOption(
  '--cluster',
  cluster_provider=mesos_client_cluster_provider,
  help='Cluster to invoke this command against. Deprecated in favor of the CLUSTER/ROLE/ENV/NAME '
       'syntax.')


def make_env_option(explanation):
  return optparse.Option(
    '--env',
    dest='env',
    default=None,
    help=explanation)

ENV_CONFIG_OPTION = make_env_option(
  'Environment to match when selecting a job from a configuration.')


# This is for binding arbitrary points in the Thermos namespace to specific strings, e.g.
# if a Thermos configuration has {{jvm.version}}, it can be bound explicitly from the
# command-line with, for example, -E jvm.version=7
ENVIRONMENT_BIND_OPTION = optparse.Option(
    '-E',
    type='string',
    nargs=1,
    action='callback',
    default=[],
    metavar='NAME=VALUE',
    callback=add_binding_to('bindings'),
    dest='bindings',
    help='Bind a thermos mustache variable name to a value. '
         'Multiple flags may be used to specify multiple values.')


EXECUTOR_SANDBOX_OPTION = optparse.Option(
    '-e',
    '--executor_sandbox',
    action='store_true',
    default=False,
    dest='executor_sandbox',
    help='Run the command in the executor sandbox instead of the task sandbox.')


# TODO(wickman) This is flawed -- we should ideally pass a config object
# directly to spawn_local and construct a Cluster object on the fly from e.g.
# command line parameters.
def make_spawn_options(options):
  return dict((name, getattr(options, name)) for name in (
      'json',
      'open_browser',
      'bindings',
      'cluster',
      'env'))


CREATE_STATES = (
  'PENDING',
  'RUNNING',
  'FINISHED'
)

WAIT_UNTIL_OPTION = optparse.Option(
    '--wait_until',
    default='PENDING',
    type='choice',
    choices=CREATE_STATES,
    metavar='STATE',
    dest='wait_until',
    help='Block the client until all the tasks have transitioned into the '
         'requested state.  Options: %s.  Default: %%default' % (', '.join(CREATE_STATES)))


@app.command
@app.command_option(ENVIRONMENT_BIND_OPTION)
@app.command_option(OPEN_BROWSER_OPTION)
@app.command_option(CLUSTER_CONFIG_OPTION)
@app.command_option(ENV_CONFIG_OPTION)
@app.command_option(JSON_OPTION)
@app.command_option(WAIT_UNTIL_OPTION)
@requires.exactly('job', 'config')
def create(jobname, config_file):
  """usage: create job config

  Creates a job based on a configuration file.
  """
  options = app.get_options()
  config = get_config(
      jobname,
      config_file,
      options.json,
      False,
      options.bindings,
      select_cluster=options.cluster.name if options.cluster else None,
      select_env=options.env)

  if config.cluster() == 'local':
    # TODO(wickman) Fix this terrible API.
    options.runner = 'build'
    return spawn_local('build', jobname, config_file, **make_spawn_options(options))

  api = make_client(config.cluster())
  monitor = JobMonitor(api, config.role(), config.environment(), config.name())
  resp = api.create_job(config)
  check_and_log_response(resp)
  handle_open(api.scheduler.scheduler(), config.role(), config.environment(), config.name())

  if options.wait_until == 'RUNNING':
    monitor.wait_until(monitor.running_or_finished)
  elif options.wait_until == 'FINISHED':
    monitor.wait_until(monitor.terminal)


@app.command
@app.command_option('--runner', dest='runner', default='build',
    help='The thermos_runner.pex to run the task.  If "build", build one automatically. '
         'This requires that you be running the spawn from within the root of a science repo.')
@app.command_option(ENVIRONMENT_BIND_OPTION)
@app.command_option(OPEN_BROWSER_OPTION)
@app.command_option(CLUSTER_CONFIG_OPTION)
@app.command_option(ENV_CONFIG_OPTION)
@app.command_option(JSON_OPTION)
@requires.exactly('job', 'config')
def spawn(jobname, config_file):
  """usage: spawn job config

  Spawns a local run of a task in the specified job.
  """
  options = app.get_options()
  return spawn_local(options.runner, jobname, config_file, **make_spawn_options(options))


def parse_package(option, _, value, parser):
  if value is None:
    return
  splits = value.split(':')
  if len(splits) != 3:
    raise OptionValueError('Expected package to be of the form role:name:version')
  if not getattr(parser.values, option.dest, None):
    setattr(parser.values, option.dest, [])
  getattr(parser.values, option.dest).append(tuple(splits))


def parse_filemap(option, _, value, parser):
  if value is None:
    return
  splits = value.split(':', 2)
  if len(splits) == 1:
    rel, glob = '.', splits[0]
  else:
    rel, glob = splits
  filemap = dict((os.path.relpath(filename, rel), filename) for filename in Fileset.zglobs(glob))
  if not getattr(parser.values, option.dest, None):
    setattr(parser.values, option.dest, {})
  getattr(parser.values, option.dest).update(filemap)


RUNTASK_INSTANCE_LIMIT = 25
RUNTASK_CPU_LIMIT = 50
RUNTASK_RAM_LIMIT = Amount(50, Data.GB)
RUNTASK_DISK_LIMIT = Amount(500, Data.GB)


def task_is_expensive(options):
  """A metric to determine whether or not we require the user to double-take."""
  if options.yes_i_really_want_to_run_an_expensive_job:
    return False

  errors = []
  if options.instances > RUNTASK_INSTANCE_LIMIT:
    errors.append('your task has more than %d instances (actual: %d)' % (
        RUNTASK_INSTANCE_LIMIT, options.instances))

  if options.instances * options.cpus > RUNTASK_CPU_LIMIT:
    errors.append('aggregate CPU is over %.1f cores (actual: %.1f)' % (
        RUNTASK_CPU_LIMIT, options.instances * options.cpus))

  if options.instances * options.ram > RUNTASK_RAM_LIMIT:
    errors.append('aggregate RAM is over %s (actual: %s)' % (
        RUNTASK_RAM_LIMIT, options.instances * options.ram))

  if options.instances * options.disk > RUNTASK_DISK_LIMIT:
    errors.append('aggregate disk is over %s (actual: %s)' % (
        RUNTASK_DISK_LIMIT, options.instances * options.disk))

  if errors:
    log.error('You must specify --yes_i_really_want_to_run_an_expensive_job because:')
    for op in errors:
      log.error('  - %s' % op)
    return True


@app.command
@app.command_option('-i', '--instances', type=int, default=1, dest='instances',
    help='The number of instances to create.')
@app.command_option('-c', '--cpu', type=float, default=1.0, dest='cpus',
    help='The amount of cpu (float) to allocate per instance.')
@app.command_option('-r', '--ram', nargs=1, action='callback', dest='ram',
    default=Amount(1, Data.GB), metavar='AMOUNT', type='string',
    callback=parse_data_into('ram', default='1G'),
    help='Amount of RAM per instance, e.g. "1512mb" or "5G".')
@app.command_option('-d', '--disk', nargs=1, action='callback', dest='disk',
    default=Amount(1, Data.GB), metavar='AMOUNT', type='string',
    callback=parse_data_into('disk'),
    help='Amount of disk per instance, e.g. "1512mb" or "5G"')
@app.command_option('-p', '--package', type='string', default=None, dest='packages',
    metavar='ROLE:NAME:VERSION', action='callback', callback=parse_package,
    help='Package to stage into sandbox prior to task invocation.  Package takes packer '
         'role, name, version strings.')
@app.command_option('-n', '--name', type='string', dest='name',
    default='quickrun-' + strftime('%Y%m%d-%H%M%S'),
    help='The job name to use.  By default an ephemeral name is generated automatically.')
@app.command_option('--announce', default=False, action='store_true', dest='announce',
    help='Announce a serverset for this job.')
@app.command_option('--role', type='string', default=getpass.getuser(), metavar='ROLE',
    help='Role in which to run the task.')
@app.command_option('-f', '--files', type='string', default=None, dest='filemap',
    metavar='[BASE:]GLOB', action='callback', callback=parse_filemap,
    help='Glob up files to be copied to the sandbox.  May be specified multiple times. '
         'The format is [BASE:]GLOB, e.g. --files=src/python/**/*.py to add all files '
         'beneath src/python ending in .py.  If you would like the paths to be relative '
         'to a base directory, use BASE, e.g. --files=src/python:src/python/**/*.py to '
         'strip away the leading src/python.')
@app.command_option('--yes_i_really_want_to_run_an_expensive_job', default=False,
    action='store_true', dest='yes_i_really_want_to_run_an_expensive_job',
    help='Yes, you really want to run a potentially resource intensive job.')
@app.command_option(ENVIRONMENT_BIND_OPTION)
@app.command_option(OPEN_BROWSER_OPTION)
def runtask(args, options):
  """usage: runtask cluster -- cmdline

  Run an Aurora task consisting of a simple command "cmdline" in a cluster.  Meant for
  one-off tasks or testing.
  """
  if len(args) < 2:
    app.error('Must specify cluster and command-line for runtask command!')
  cluster_name = args[0]
  cmdline = ' '.join(args[1:])
  if task_is_expensive(options):
    die('Task too expensive.')
  qr = Quickrun(
      cluster_name,
      cmdline,
      name=options.name,
      role=options.role,
      instances=options.instances,
      cpu=options.cpus,
      ram=options.ram,
      disk=options.disk,
      announce=options.announce,
      packages=options.packages,
      additional_files=options.filemap)
  api = make_client(cluster_name)
  qr.run(api)


@app.command
@app.command_option(ENVIRONMENT_BIND_OPTION)
@app.command_option(CLUSTER_CONFIG_OPTION)
@app.command_option(ENV_CONFIG_OPTION)
@app.command_option(JSON_OPTION)
@requires.exactly('job', 'config')
def diff(job, config_file):
  """usage: diff job config

  Compares a job configuration against a running job.
  By default the diff will be displayed using 'diff', though you may choose an alternate
  diff program by specifying the DIFF_VIEWER environment variable."""
  options = app.get_options()
  config = get_config(
      job,
      config_file,
      options.json,
      False,
      options.bindings,
      select_cluster=options.cluster.name if options.cluster else None,
      select_env=options.env)
  api = make_client(config.cluster())
  resp = api.query(api.build_query(config.role(), job, statuses=ACTIVE_STATES, env=options.env))
  if not resp.responseCode:
    die('Request failed, server responded with "%s"' % resp.message)
  remote_tasks = [t.assignedTask.task for t in resp.tasks]
  resp = api.populate_job_config(config)
  if not resp.responseCode:
    die('Request failed, server responded with "%s"' % resp.message)
  local_tasks = resp.populated

  pp = pprint.PrettyPrinter(indent=2)
  def pretty_print_task(task):
    # The raw configuration is not interesting - we only care about what gets parsed.
    task.configuration = None
    return pp.pformat(vars(task))

  def pretty_print_tasks(tasks):
    shard_ordered = sorted(tasks, key=lambda t: t.shardId)
    return ',\n'.join([pretty_print_task(t) for t in shard_ordered])

  def dump_tasks(tasks, out_file):
    out_file.write(pretty_print_tasks(tasks))
    out_file.write('\n')
    out_file.flush()

  diff_program = os.environ.get('DIFF_VIEWER', 'diff')
  with NamedTemporaryFile() as local:
    dump_tasks(local_tasks, local)
    with NamedTemporaryFile() as remote:
      dump_tasks(remote_tasks, remote)
      subprocess.call([diff_program, remote.name, local.name])


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
  webbrowser.open_new_tab(synthesize_url(api.scheduler.scheduler(), role, env, job))


@app.command
@app.command_option('--local', dest='local', default=False, action='store_true',
    help='Inspect the configuration as would be created by the "spawn" command.')
@app.command_option('--raw', dest='raw', default=False, action='store_true',
    help='Show the raw configuration.')
@app.command_option(ENVIRONMENT_BIND_OPTION)
@app.command_option(CLUSTER_CONFIG_OPTION)
@app.command_option(ENV_CONFIG_OPTION)
@app.command_option(JSON_OPTION)
@requires.exactly('job', 'config')
def inspect(jobname, config_file):
  """usage: inspect job config

  Verifies that a job can be parsed from a configuration file, and displays
  the parsed configuration.
  """
  options = app.get_options()
  config = get_config(
      jobname,
      config_file,
      options.json,
      False,
      options.bindings,
      select_cluster=options.cluster.name if options.cluster else None,
      select_env=options.env)
  if options.raw:
    print('Parsed job config: %s' % config.job())
    return

  job_thrift = config.job()
  job = config.raw()
  job_thrift = config.job()
  print('Job level information')
  print('  name:       %s' % job.name())
  print('  role:       %s' % job.role())
  print('  contact:    %s' % job.role())
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
def start_cron(args, options):
  """usage: start_cron cluster/role/env/job

  Invokes a cron job immediately, out of its normal cron cycle.
  This does not affect the cron cycle in any way.
  """

  api, job_key = LiveJobDisambiguator.disambiguate_args_or_die(args, options, make_client_factory())
  resp = api.start_cronjob(job_key)
  check_and_log_response(resp)
  handle_open(api.scheduler.scheduler(), job_key.role, job_key.env, job_key.name)


@app.command
@app.command_option(CLUSTER_INVOKE_OPTION)
@app.command_option(OPEN_BROWSER_OPTION)
@app.command_option(SHARDS_OPTION)
def kill(args, options):
  """usage: kill cluster/role/env/job

  Kills a running job, blocking until all tasks have terminated.

  Default behaviour is to kill all shards in the job, but the kill
  can be limited to specific shards with the --shards option
  """
  api, job_key = LiveJobDisambiguator.disambiguate_args_or_die(args, options, make_client_factory())
  resp = api.kill_job(job_key, options.shards)
  check_and_log_response(resp)
  handle_open(api.scheduler.scheduler(), job_key.role, job_key.env, job_key.name)


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
    return taskString

  def print_tasks(tasks):
    for task in tasks:
      taskString = print_task(task)

      log.info('role: %s, env: %s, name: %s, shard: %s, status: %s on %s\n%s' %
             (task.assignedTask.task.owner.role,
              task.assignedTask.task.environment,
              task.assignedTask.task.jobName,
              task.assignedTask.task.shardId,
              ScheduleStatus._VALUES_TO_NAMES[task.status],
              task.assignedTask.slaveHost,
              taskString))

  api, job_key = LiveJobDisambiguator.disambiguate_args_or_die(args, options, make_client_factory())
  resp = api.check_status(job_key)
  check_and_log_response(resp)

  if resp.tasks:
    active_tasks = filter(is_active, resp.tasks)
    log.info('Active Tasks (%s)' % len(active_tasks))
    print_tasks(active_tasks)
    inactive_tasks = filter(lambda x: not is_active(x), resp.tasks)
    log.info('Inactive Tasks (%s)' % len(inactive_tasks))
    print_tasks(inactive_tasks)
  else:
    log.info('No tasks found.')


@app.command
@app.command_option('--updater_health_check_interval_seconds',
                    dest='health_check_interval_seconds',
                    type=int,
                    default=3,
                    help='Time interval between subsequent shard status checks.')
@app.command_option(SHARDS_OPTION)
@app.command_option(ENVIRONMENT_BIND_OPTION)
@app.command_option(CLUSTER_CONFIG_OPTION)
@app.command_option(ENV_CONFIG_OPTION)
@app.command_option(JSON_OPTION)
@requires.exactly('job', 'config')
def update(jobname, config_file):
  """usage: update job config

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
  options = app.get_options()
  config = get_config(
      jobname,
      config_file,
      options.json,
      force_local=False,
      bindings=options.bindings,
      select_cluster=options.cluster.name if options.cluster else None,
      select_env=options.env)
  api = make_client(config.cluster())
  resp = api.update_job(config, options.health_check_interval_seconds, options.shards)
  check_and_log_response(resp)


@app.command
@app.command_option(CLUSTER_INVOKE_OPTION)
@app.command_option(OPEN_BROWSER_OPTION)
@app.command_option(SHARDS_OPTION)
@app.command_option(
    '--batch_size',
    dest='batch_size',
    type=int,
    default=1,
    help='Number of shards to be restarted in one iteration.')
@app.command_option(
    '--health_check_interval_seconds',
    dest='health_check_interval_seconds',
    type=int,
    default=3,
    help='Time interval between subsequent shard status checks.')
@app.command_option(
    '--max_per_shard_failures',
    dest='max_per_shard_failures',
    type=int,
    default=0,
    help='Maximum number of restarts per shard during restart. Increments total failure count when this limit is exceeded.')
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
    help='Maximum number of seconds before a shard must move into the RUNNING state before considered a failure.')
@app.command_option(
    '--watch_secs',
    dest='watch_secs',
    type=int,
    default=30,
    help='Minimum number of seconds a shard must remain in RUNNING state before considered a success.')
def restart(args, options):
  """usage: restart cluster/role/env/job [--shards=SHARDS] [--batch_size=BATCH_SIZE]
               [--health_check_interval_seconds=HEALTH_CHECK_INTERVAL_SECONDS]
               [--max_per_shard_failures=MAX_PER_SHARD_FAILURES]
               [--max_total_failures=MAX_TOTAL_FAILURES]
               [--restart_threshold=RESTART_THRESHOLD] [--watch_secs=WATCH_SECS]

  Performs a rolling restart of shards within a job.

  Restarts are fully controlled client-side, so aborting halts the restart.
  """
  api, job_key = LiveJobDisambiguator.disambiguate_args_or_die(args, options, make_client_factory())
  updater_config = UpdaterConfig(
      options.batch_size,
      options.restart_threshold,
      options.watch_secs,
      options.max_per_shard_failures,
      options.max_total_failures)
  resp = api.restart(job_key, options.shards, updater_config, options.health_check_interval_seconds)
  check_and_log_response(resp)
  handle_open(api.scheduler.scheduler(), job_key.role, job_key.env, job_key.name)


@app.command
@app.command_option(CLUSTER_INVOKE_OPTION)
def cancel_update(args, options):
  """usage: cancel_update cluster/role/env/job

  Unlocks a job for updates.
  A job may be locked if a client's update session terminated abnormally,
  or if another user is actively updating the job.  This command should only
  be used when the user is confident that they are not conflicting with another user.
  """
  api, job_key = LiveJobDisambiguator.disambiguate_args_or_die(args, options, make_client_factory())
  resp = api.cancel_update(job_key)
  check_and_log_response(resp)


@app.command
@app.command_option(CLUSTER_INVOKE_OPTION)
@requires.exactly('role')
def get_quota(role):
  """usage: get_quota --cluster=CLUSTER role

  Prints the production quota that has been allocated to a user.
  """
  options = app.get_options()
  resp = make_client(options.cluster).get_quota(role)
  quota = resp.quota

  quota_fields = [
    ('CPU', quota.numCpus),
    ('RAM', '%f GB' % (float(quota.ramMb) / 1024)),
    ('Disk', '%f GB' % (float(quota.diskMb) / 1024))
  ]
  log.info('Quota for %s:\n\t%s' %
           (role, '\n\t'.join(['%s\t%s' % (k, v) for (k, v) in quota_fields])))


@app.command
@app.command_option(EXECUTOR_SANDBOX_OPTION)
@app.command_option('--user', dest='ssh_user', default=None,
                    help="ssh as this user instead of the role.")
@app.command_option('-L', dest='tunnels', action='append', metavar='PORT:NAME',
                    default=[],
                    help="Add tunnel from local port PORT to remote named port NAME.")
def ssh(args, options):
  """usage: ssh cluster/role/env/job shard [args...]

  Initiate an SSH session on the machine that a shard is running on.
  """
  if not args:
    die('Job path is required')
  job_path = args.pop(0)
  try:
    cluster_name, role, env, name = AuroraJobKey.from_path(job_path)
  except AuroraJobKey.Error as e:
    die('Invalid job path "%s": %s' % (job_path, e))
  if not args:
    die('Shard is required')
  try:
    shard = int(args.pop(0))
  except ValueError:
    die('Shard must be an integer')
  api = make_client(cluster_name)
  resp = api.query(api.build_query(role, name, set([int(shard)]), env=env))
  check_and_log_response(resp)

  remote_cmd = 'bash' if not args else ' '.join(args)
  command = DistributedCommandRunner.substitute(remote_cmd, resp.tasks[0],
      api.cluster, executor_sandbox=options.executor_sandbox)

  ssh_command = ['ssh', '-t']

  role = resp.tasks[0].assignedTask.task.owner.role
  slave_host = resp.tasks[0].assignedTask.slaveHost

  for tunnel in options.tunnels:
    try:
      port, name = tunnel.split(':')
      port = int(port)
    except ValueError:
      die('Could not parse tunnel: %s.  Must be of form PORT:NAME' % tunnel)
    if name not in resp.tasks[0].assignedTask.assignedPorts:
      die('Task %s has no port named %s' % (resp.tasks[0].assignedTask.taskId, name))
    ssh_command += [
        '-L', '%d:%s:%d' % (port, slave_host, resp.tasks[0].assignedTask.assignedPorts[name])]

  ssh_command += ['%s@%s' % (options.ssh_user or role, slave_host), command]
  return subprocess.call(ssh_command)


@app.command
@app.command_option('-t', '--threads', type=int, default=1, dest='num_threads',
    help='The number of threads to use.')
@app.command_option(EXECUTOR_SANDBOX_OPTION)
def run(args, options):
  """usage: run cluster/role/env/job cmd

  Runs a shell command on all machines currently hosting shards of a single job.

  This feature supports the same command line wildcards that are used to
  populate a job's commands.

  This means anything in the {{mesos.*}} and {{thermos.*}} namespaces.
  """
  # TODO(William Farner): Add support for invoking on individual shards.
  # TODO(Kevin Sweeney): Restore the ability to run across jobs with globs (See MESOS-3010).
  if not args:
    die('job path is required')
  job_path = args.pop(0)
  try:
    cluster_name, role, env, name = AuroraJobKey.from_path(job_path)
  except AuroraJobKey.Error as e:
    die('Invalid job path "%s": %s' % (job_path, e))

  command = ' '.join(args)
  try:
    cluster = MESOS_CLIENT_CLUSTER_SET[cluster_name]
  except KeyError:
    die('Unknown cluster %s' % cluster_name)

  dcr = DistributedCommandRunner(cluster, role, [name])
  dcr.run(command, parallelism=options.num_threads, executor_sandbox=options.executor_sandbox)


def _get_packer(cluster, verbosity='normal'):
  # TODO(wickman) sd_packer_client(cluster.name) => TwitterPacker(cluster)
  return sd_packer_client.create_packer(cluster.name, verbose=verbosity in ('normal', 'verbose'))


def trap_packer_error(fn):
  @functools.wraps(fn)
  def wrap(args):
    try:
      return fn(args)
    except Packer.Error as e:
      print('Request failed: %s' % e)
  return wrap


@app.command
@trap_packer_error
@app.command_option(CLUSTER_INVOKE_OPTION)
@app.command_option('--all', '-a', dest='list_all_packages', default=False, action='store_true',
                    help='List hidden packages (packages prefixed with "__") as well.')
@requires.exactly('role')
def package_list(role):
  """usage: package_list --cluster=CLUSTER role

  Prints the names of packages owned by a user.
  """
  options = app.get_options()
  cluster, verbosity = options.cluster, options.verbosity
  def filter_packages(packages):
    if options.list_all_packages:
      return packages
    return [package for package in packages if not package.startswith('__')]
  print('\n'.join(filter_packages(_get_packer(cluster, verbosity).list_packages(role))))


def _print_package(pkg):
  print('Version: %s' % pkg['id'])
  if 'metadata' in pkg and pkg['metadata']:
    print('User metadata:\n  %s' % pkg['metadata'])
  for audit in sorted(pkg['auditLog'], key=lambda k: int(k['timestamp'])):
    gmtime_str = strftime('%m/%d/%Y %H:%M:%S UTC', gmtime(int(audit['timestamp']) / 1000))
    print('  moved to state %s by %s on %s' % (audit['state'], audit['user'], gmtime_str))


@app.command
@trap_packer_error
@app.command_option(CLUSTER_INVOKE_OPTION)
@requires.exactly('role', 'package')
def package_versions(role, package):
  """usage: package_versions --cluster=CLUSTER role package

  Prints metadata about all of the versions of a package.
  """
  options = app.get_options()
  for version in _get_packer(options.cluster, options.verbosity).list_versions(role, package):
    _print_package(version)


@app.command
@trap_packer_error
@app.command_option(CLUSTER_INVOKE_OPTION)
@requires.exactly('role', 'package', 'version')
def package_delete_version(role, package, version):
  """usage: package_delete_version --cluster=CLUSTER role package version

  Deletes a version of a package.
  """
  options = app.get_options()
  _get_packer(options.cluster, options.verbosity).delete(role, package, version)
  print('Version deleted')


@app.command
@trap_packer_error
@app.command_option(CLUSTER_INVOKE_OPTION)
@app.command_option(
    '--metadata',
    default='',
    help='Metadata to be applied to a package when invoking package_add_version.')
@requires.exactly('role', 'package', 'file_path')
def package_add_version(role, package, file_path):
  """usage: package_add_version --cluster=CLUSTER role package file_path

  Uploads a new version of a package.
  The file will be stored with a name equal to the basename of the supplied file.
  Likewise, when the package is fetched prior to running a task, it will
  be stored as the original basename.
  """
  options = app.get_options()
  cluster, verbosity = options.cluster, options.verbosity
  pkg = _get_packer(cluster, verbosity).add(role, package, file_path, options.metadata)
  print('Package added:')
  _print_package(pkg)


@app.command
@trap_packer_error
@app.command_option(CLUSTER_INVOKE_OPTION)
@app.command_option(
    '--metadata_only',
    default=False,
    action='store_true',
    help='When fetching a package version, return only the metadata.')
@requires.exactly('role', 'package', 'version')
def package_get_version(role, package, version):
  """usage: package_get_version --cluster=CLUSTER role package version

  Prints the metadata associated with a specific version of a package.
  This supports version labels 'latest' and 'live'.
  """
  options = app.get_options()
  pkg = _get_packer(options.cluster, options.verbosity).get_version(role, package, version)
  if options.metadata_only:
    if not 'metadata' in pkg:
      die('Package does not contain any user-specified metadata.')
    else:
      print(pkg['metadata'])
  else:
    _print_package(pkg)


@app.command
@trap_packer_error
@app.command_option(CLUSTER_INVOKE_OPTION)
@requires.exactly('role', 'package', 'version')
def package_set_live(role, package, version):
  """usage: package_set_live --cluster=CLUSTER role package version

  Updates the 'live' label of a package to point to a specific version.
  """
  options = app.get_options()
  _get_packer(options.cluster, options.verbosity).set_live(role, package, version)
  print('Version %s is now the LIVE version' % version)

@app.command
@trap_packer_error
@app.command_option(CLUSTER_INVOKE_OPTION)
@requires.exactly('role', 'package')
def package_unset_live(role, package):
  """usage: package_unset_live --cluster=CLUSTER role package

  Removes the 'live' label of a package if it exists.
  """
  options = app.get_options()
  _get_packer(options.cluster, options.verbosity).unset_live(role, package)
  print('LIVE label unset')


@app.command
@trap_packer_error
@app.command_option(CLUSTER_INVOKE_OPTION)
@requires.exactly('role', 'package')
def package_unlock(role, package):
  """usage: package_unlock --cluster=CLUSTER role package

  Unlocks a package.
  Under normal circumstances, this should not be necessary.  However,
  in the event of a packer server crash it is possible.  If you find
  a need to use this, please inform mesos-team so they can investigate.
  """
  options = app.get_options()
  _get_packer(options.cluster, options.verbosity).unlock(role, package)
  print('Package unlocked')


def make_commands_str(commands):
  commands.sort()
  if len(commands) == 1:
    return str(commands[0])
  elif len(commands) == 2:
    return '%s (or %s)' % (str(commands[0]), str(commands[1]))
  else:
    return '%s (or any of: %s)' % (str(commands[0]), ' '.join(map(str, commands[1:])))


def generate_full_usage():
  docs_to_commands = collections.defaultdict(list)
  for (command, doc) in app.get_commands_and_docstrings():
    docs_to_commands[doc].append(command)
  def make_docstring(item):
    (doc_text, commands) = item
    def format_line(line):
      return '    %s\n' % line.lstrip()
    stripped = ''.join(map(format_line, doc_text.splitlines()))
    return '%s\n%s' % (make_commands_str(commands), stripped)
  usage = sorted(map(make_docstring, docs_to_commands.items()))
  return 'Available commands:\n\n' + '\n'.join(usage)


def generate_terse_usage():
  docs_to_commands = collections.defaultdict(list)
  for (command, doc) in app.get_commands_and_docstrings():
    docs_to_commands[doc].append(command)
  usage = '\n    '.join(sorted(map(make_commands_str, docs_to_commands.values())))
  return """
Available commands:
    %s

For more help on an individual command:
    %s help <command>
""" % (usage, app.name())


@app.command
def help(args):
  """usage: help [subcommand]

  Displays help for using mesos client, or a specific subcommand.
  """
  if not args:
    print(generate_full_usage())
    sys.exit(0)

  if len(args) > 1:
    die('Please specify at most one subcommand.')

  subcmd = args[0]
  if subcmd in globals():
    app.command_parser(subcmd).print_help()
  else:
    print('Subcommand %s not found.' % subcmd)
    sys.exit(1)


def main():
  app.help()


if __name__ == '__main__':
  app.interspersed_args(True)
  LogOptions.set_stderr_log_level('INFO')
  LogOptions.disable_disk_logging()
  app.set_name('mesos-client')
  app.set_usage(DEFAULT_USAGE_BANNER + generate_terse_usage())
  app.main()
