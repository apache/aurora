"""Command-line client for managing jobs with the Twitter mesos scheduler.
"""

import collections
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
import zookeeper

from twitter.common import app, log
from twitter.common.log.options import LogOptions
from twitter.common.quantity import Amount, Data
from twitter.common.quantity.parse_simple import parse_data_into
from twitter.mesos.client import client_util
from twitter.mesos.client.client_wrapper import MesosClientAPI
from twitter.mesos.client.client_util import requires
from twitter.mesos.client.quickrun import Quickrun
from twitter.mesos.client.spawn_local import spawn_local
from twitter.mesos.clusters import Cluster
from twitter.mesos.command_runner import DistributedCommandRunner
from twitter.mesos.packer import sd_packer_client
from twitter.mesos.packer.packer_client import Packer
from twitter.thermos.base.options import add_binding_to

from gen.twitter.mesos.constants import ACTIVE_STATES, LIVE_STATES
from gen.twitter.mesos.ttypes import (
    Identity,
    ScheduleStatus,
    TaskQuery)


DEFAULT_USAGE_BANNER = """
mesos client, used to interact with the aurora scheduler.

For questions contact mesos-team@twitter.com.
"""

def synthesize_url(scheduler_client, role=None, job=None):
  scheduler_url = scheduler_client.url
  if not scheduler_url:
    log.warning("Unable to find scheduler web UI!")
    return None

  if job and not role:
    client_util.die('If job specified, must specify role!')

  scheduler_url = urljoin(scheduler_url, 'scheduler')
  if role:
    scheduler_url = urljoin(scheduler_url, role)
    if job:
      scheduler_url = urljoin(scheduler_url, job)
  return scheduler_url


def handle_open(scheduler_client, role, job):
  url = synthesize_url(scheduler_client, role, job)
  if url:
    log.info('Job url: %s' % url)
    if app.get_options().open_browser:
      import webbrowser
      webbrowser.open_new_tab(url)


COPY_APP_FROM_OPTION = optparse.Option(
    '--copy_app_from',
    dest='copy_app_from',
    default=None,
    help='Local path to use for the app (will copy to the cluster)  (default: %default)')


OPEN_BROWSER_OPTION = optparse.Option(
    '-o',
    '--open_browser',
    dest='open_browser',
    action='store_true',
    default=False,
    help='Open a browser window to the job page after a job mutation.')

SHARDS_OPTION = optparse.Option(
    '--shards',
    dest='shards',
    default=None,
    help='A comma separated list of shard ids to act on.')

CONFIG_TYPE_OPTION = optparse.Option(
    '-c',
    '--config_type',
    type='choice',
    action='store',
    dest='config_type',
    choices=['mesos', 'thermos', 'auto'],
    default='mesos',
    help='The type of the configuration file supplied.  Options are `mesos`, the '
         'new `thermos` format, or `auto`, which automatically converts from '
         'mesos to thermos configs.')


JSON_OPTION = optparse.Option(
    '-j',
    '--json',
    dest='json',
    default=False,
    action='store_true',
    help='If specified, configuration is read in JSON format.')


CLUSTER_OPTION = optparse.Option(
    '--cluster',
    dest='cluster',
    default=None,
    help='Cluster to invoke the command against.')


# This is for binding arbitrary points in the Thermos namespace to specific strings, e.g.
# if a Thermos configuration has {{jvm.version}}, it can be bound explicitly from the
# command-line with, for example, -E jvm.version:7
ENVIRONMENT_BIND_OPTION = optparse.Option(
    '-E',
    type='string',
    nargs=1,
    action='callback',
    default=[],
    metavar='NAME:VALUE',
    callback=add_binding_to('bindings'),
    dest='bindings',
    help='Bind a thermos environment name to a value.')


EXECUTOR_SANDBOX_OPTION = optparse.Option(
    '-e',
    '--executor_sandbox',
    action='store_true',
    default=False,
    dest='executor_sandbox',
    help='Run the command in the executor sandbox instead of the task sandbox.')


def make_spawn_options(options):
  return dict((name, getattr(options, name)) for name in (
      'copy_app_from',
      'config_type',
      'json',
      'shard',
      'bindings'))


@app.command
@app.command_option(ENVIRONMENT_BIND_OPTION)
@app.command_option(COPY_APP_FROM_OPTION)
@app.command_option(OPEN_BROWSER_OPTION)
@app.command_option(CONFIG_TYPE_OPTION)
@app.command_option(JSON_OPTION)
@requires.exactly('job', 'config')
def create(jobname, config_file):
  """usage: create job config

  Creates a job based on a configuration file.
  """
  options = app.get_options()
  config = client_util.get_config(jobname, config_file, options.copy_app_from, options.config_type,
      options.json)
  if config.cluster() == 'local':
    options.shard = 0
    options.runner = 'build'
    print('Detected cluster=local, spawning local run.')
    return spawn_local('build', jobname, config_file, **make_spawn_options(options))

  api = MesosClientAPI(cluster=config.cluster(), verbose=app.get_options().verbose)
  resp = api.create_job(config, app.get_options().copy_app_from)
  client_util.check_and_log_response(resp)
  handle_open(api.scheduler.scheduler(), config.role(), config.name())


@app.command
@app.command_option('--shard', dest='shard', type=int, default=0,
    help='The shard number to spawn.')
@app.command_option('--runner', dest='runner', default='build',
    help='The thermos_runner.pex to run the task.  If "build", build one automatically. '
         'This requires that you be running the spawn from within the root of a science repo.')
@app.command_option(ENVIRONMENT_BIND_OPTION)
@app.command_option(COPY_APP_FROM_OPTION)
@app.command_option(OPEN_BROWSER_OPTION)
@app.command_option(CONFIG_TYPE_OPTION)
@app.command_option(JSON_OPTION)
@requires.exactly('job', 'config')
def spawn(jobname, config_file):
  """usage: spawn [--shard=number] job config

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
  setattr(parser.values, option.dest, tuple(splits))


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
@app.command_option('-p', '--package', type='string', default=None,
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
  cluster = args[0]
  Cluster.assert_exists(cluster)
  cmdline = ' '.join(args[1:])
  if task_is_expensive(options):
    client_util.die('Task too expensive.')
  qr = Quickrun(cluster, cmdline, options)
  api = MesosClientAPI(cluster=cluster)
  qr.run(api)


@app.command
@app.command_option(ENVIRONMENT_BIND_OPTION)
@app.command_option(COPY_APP_FROM_OPTION)
@app.command_option(CONFIG_TYPE_OPTION)
@app.command_option(JSON_OPTION)
@requires.exactly('job', 'config')
def diff(job, config_file):
  """usage: diff job config

  Compares a job configuration against a running job.
  By default the diff will be displayed using 'diff', though you may choose an alternate
  diff program by specifying the DIFF_VIEWER environment variable."""
  options = app.get_options()
  config = client_util.get_config(job, config_file, options.copy_app_from, options.config_type,
      options.json, False, options.bindings)
  api = MesosClientAPI(cluster=config.cluster(), verbose=app.get_options().verbose)
  resp = query(config.role(), job, api=api, statuses=ACTIVE_STATES)
  if not resp.responseCode:
    client_util.die('Request failed, server responded with "%s"' % resp.message)
  remote_tasks = [t.assignedTask.task for t in resp.tasks]
  resp = api.populate_job_config(config)
  if not resp.responseCode:
    client_util.die('Request failed, server responded with "%s"' % resp.message)
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
@app.command_option(CLUSTER_OPTION)
@requires.nothing
def do_open(*args):
  """usage: open --cluster=CLUSTER [role [job]]

  Opens the scheduler page for a role or job in the default web browser.
  """
  options = app.get_options()
  role = job = None
  if len(args) > 0:
    role = args[0]
  if len(args) > 1:
    job = args[1]

  if not options.cluster:
    client_util.die('--cluster is required')

  api = MesosClientAPI(cluster=options.cluster, verbose=options.verbose)
  open_url(synthesize_url(api.scheduler.scheduler(), role, job))


@app.command
@app.command_option('--local', dest='local', default=False, action='store_true',
    help='Inspect the configuration as would be created by the "spawn" command.')
@app.command_option(COPY_APP_FROM_OPTION)
@app.command_option(ENVIRONMENT_BIND_OPTION)
@app.command_option(CONFIG_TYPE_OPTION)
@app.command_option(JSON_OPTION)
@requires.exactly('job', 'config')
def inspect(jobname, config_file):
  """usage: inspect job config

  Verifies that a job can be parsed from a configuration file, and displays
  the parsed configuration.
  """
  options = app.get_options()
  config = client_util.get_config(jobname, config_file, options.copy_app_from,
      options.config_type, options.json, False, options.bindings)
  log.info('Parsed job config: %s' % config.job())


@app.command
@app.command_option(CLUSTER_OPTION)
@app.command_option(OPEN_BROWSER_OPTION)
@requires.exactly('role', 'job')
def start_cron(role, jobname):
  """usage: start_cron --cluster=CLUSTER role job

  Invokes a cron job immediately, out of its normal cron cycle.
  This does not affect the cron cycle in any way.
  """
  api = MesosClientAPI(cluster=app.get_options().cluster, verbose=app.get_options().verbose)
  resp = api.start_cronjob(role, jobname)
  client_util.check_and_log_response(resp)
  handle_open(api.scheduler.scheduler(), role, jobname)


@app.command
@app.command_option(CLUSTER_OPTION)
@app.command_option(OPEN_BROWSER_OPTION)
@app.command_option(SHARDS_OPTION)
@requires.exactly('role', 'job')
def kill(role, jobname):
  """usage: kill --cluster=CLUSTER --shards=shards role job

  Kills a running job, blocking until all tasks have terminated.
  Kills all shards if no shard-ids are specified.

  """
  options = app.get_options()
  api = MesosClientAPI(cluster=options.cluster, verbose=options.verbose)
  resp = api.kill_job(role, jobname, _getshards(options.shards))
  client_util.check_and_log_response(resp)
  handle_open(api.scheduler.scheduler(), role, jobname)


@app.command
@app.command_option(CLUSTER_OPTION)
@requires.exactly('role', 'job')
def status(role, jobname):
  """usage: status --cluster=CLUSTER role job

  Fetches and prints information about the active tasks in a job.
  """

  def is_active(task):
    return task.status in ACTIVE_STATES

  def print_task(scheduled_task):
    assigned_task = scheduled_task.assignedTask
    taskInfo = assigned_task.task
    taskString = ''
    if taskInfo:
      taskString += '''\tHDFS path: %s
\tcpus: %s, ram: %s MB, disk: %s MB''' % (taskInfo.hdfsPath,
                                        taskInfo.numCpus,
                                        taskInfo.ramMb,
                                        taskInfo.diskMb)
    if assigned_task.assignedPorts:
      taskString += '\n\tports: %s' % assigned_task.assignedPorts
    taskString += '\n\tfailure count: %s (max %s)' % (scheduled_task.failureCount,
                                                      taskInfo.maxTaskFailures)
    return taskString

  def print_tasks(tasks):
    for task in tasks:
      taskString = print_task(task)

      log.info('shard: %s, status: %s on %s\n%s' %
             (task.assignedTask.task.shardId,
              ScheduleStatus._VALUES_TO_NAMES[task.status],
              task.assignedTask.slaveHost,
              taskString))

  api = MesosClientAPI(cluster=app.get_options().cluster, verbose=app.get_options().verbose)
  resp = api.check_status(role, jobname)
  client_util.check_and_log_response(resp)

  if resp.tasks:
    active_tasks = filter(is_active, resp.tasks)
    log.info('Active Tasks (%s)' % len(active_tasks))
    print_tasks(active_tasks)
    inactive_tasks = filter(lambda x: not is_active(x), resp.tasks)
    log.info('Inactive Tasks (%s)' % len(inactive_tasks))
    print_tasks(inactive_tasks)
  else:
    log.info('No tasks found.')


def _getshards(shards):
  if not shards:
    return None

  try:
    return map(int, shards.split(','))
  except ValueError:
    client_util.die('Invalid shards list: %r' % shards)


@app.command
@app.command_option(SHARDS_OPTION)
@app.command_option(ENVIRONMENT_BIND_OPTION)
@app.command_option(COPY_APP_FROM_OPTION)
@app.command_option(CONFIG_TYPE_OPTION)
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
  config = client_util.get_config(jobname, config_file, options.copy_app_from,
      options.config_type, options.json, force_local=False, bindings=options.bindings)
  api = MesosClientAPI(cluster=config.cluster(), verbose=options.verbose)
  resp = api.update_job(config, _getshards(options.shards), options.copy_app_from)
  client_util.check_and_log_response(resp)


@app.command
@app.command_option(CLUSTER_OPTION)
@requires.exactly('role', 'job')
def cancel_update(role, jobname):
  """usage: cancel_update --cluster=CLUSTER role job

  Unlocks an job for updates.
  A job may be locked if a client's update session terminated abnormally,
  or if another user is actively updating the job.  This command should only
  be used when the user is confident that they are not conflicting with another user.
  """
  api = MesosClientAPI(cluster=app.get_options().cluster, verbose=app.get_options().verbose)
  resp = api.cancel_update(role, jobname)
  client_util.check_and_log_response(resp)


@app.command
@app.command_option(CLUSTER_OPTION)
@requires.exactly('role')
def get_quota(role):
  """usage: get_quota --cluster=CLUSTER role

  Prints the production quota that has been allocated to a user.
  """
  api = MesosClientAPI(cluster=app.get_options().cluster, verbose=app.get_options().verbose)
  resp = api.get_quota(role)
  quota = resp.quota

  quota_fields = [
    ('CPU', quota.numCpus),
    ('RAM', '%f GB' % (float(quota.ramMb) / 1024)),
    ('Disk', '%f GB' % (float(quota.diskMb) / 1024))
  ]
  log.info('Quota for %s:\n\t%s' %
           (role, '\n\t'.join(['%s\t%s' % (k, v) for (k, v) in quota_fields])))


@app.command
@app.command_option(CLUSTER_OPTION)
@requires.exactly('role', 'cpu', 'ramMb', 'diskMb')
def set_quota(role, cpu_str, ram_mb_str, disk_mb_str):
  """usage: set_quota --cluster=CLUSTER role cpu ramMb diskMb

  Admin-only command.
  Alters the amount of production quota allocated to a user.
  """
  try:
    cpu = float(cpu_str)
    ram_mb = int(ram_mb_str)
    disk_mb = int(disk_mb_str)
  except ValueError:
    log.error('Invalid value')

  api = MesosClientAPI(cluster=app.get_options().cluster, verbose=app.get_options().verbose)
  resp = api.set_quota(role, cpu, ram_mb, disk_mb)
  client_util.check_and_log_response(resp)


@app.command
@app.command_option(CLUSTER_OPTION)
@requires.exactly('task_id', 'state')
def force_task_state(task_id, state):
  """usage: force_task_state --cluster=CLUSTER task_id state

  Admin-only command.
  Forcibly induces a state transition on a task.
  This should never be used under normal circumstances, and any use of this command
  should be accompanied by a bug indicating why it was necessary.
  """
  status = ScheduleStatus._NAMES_TO_VALUES.get(state)
  if status is None:
    log.error('Unrecognized status "%s", must be one of [%s]'
              % (state, ', '.join(ScheduleStatus._NAMES_TO_VALUES.keys())))
    sys.exit(1)

  api = MesosClientAPI(cluster=app.get_options().cluster, verbose=app.get_options().verbose)
  resp = api.force_task_state(task_id, status)
  client_util.check_and_log_response(resp)


def query(role, job, shards=None, statuses=LIVE_STATES, api=None):
  query = TaskQuery()
  query.statuses = statuses
  query.owner = Identity(role=role)
  query.jobName = job
  query.shardIds = shards
  api = api or MesosClientAPI(cluster=app.get_options().cluster, verbose=app.get_options().verbose)
  return api.query(query)


@app.command
@app.command_option(CLUSTER_OPTION)
@app.command_option(EXECUTOR_SANDBOX_OPTION)
@requires.exactly('role', 'job', 'shard')
def ssh(role, job, shard):
  """usage: ssh --cluster=CLUSTER role job shard

  Initiate an SSH session on the machine that a shard is running on.
  """
  resp = query(role, job, set([int(shard)]))
  if not resp.responseCode:
    client_util.die('Request failed, server responded with "%s"' % resp.message)

  command = DistributedCommandRunner.substitute('bash',
      resp.tasks[0], executor_sandbox=app.get_options().executor_sandbox)
  return subprocess.call(['ssh', '-t', resp.tasks[0].assignedTask.slaveHost,
      command])


@app.command
@app.command_option('-t', '--threads', type=int, default=1, dest='num_threads',
    help='The number of threads to use.')
@app.command_option(EXECUTOR_SANDBOX_OPTION)
@app.command_option(CLUSTER_OPTION)
@requires.at_least('role', 'job', 'cmd')
def run(*line):
  """usage: run --cluster=CLUSTER role job(s) cmd

  Runs a shell command on all machines currently hosting shards of a
  comma-separated list of jobs.

  This feature supports the same command line wildcards that are used to
  populate a job's commands.

  For Aurora jobs this means %port:PORT_NAME%, %shard_id%, %task_id%.
  For Thermos jobs this means anything in the {{mesos.*}} and {{thermos.*}} namespaces.
  """
  # TODO(William Farner): Add support for invoking on individual shards.
  options = app.get_options()

  (role, jobs) = line[:2]
  command = ' '.join(line[2:])

  if not options.cluster:
    client_util.die('--cluster is required')

  dcr = DistributedCommandRunner(options.cluster, role, jobs.split(','))
  dcr.run(command, parallelism=options.num_threads, executor_sandbox=options.executor_sandbox)


def _get_packer(cluster=None):
  cluster = cluster or app.get_options().cluster
  if not cluster:
    client_util.die('--cluster must be specified')
  return sd_packer_client.create_packer(cluster)


def trap_packer_error(fn):
  @functools.wraps(fn)
  def wrap(args):
    try:
      return fn(args)
    except Packer.Error as e:
      print 'Request failed: %s' % e
  return wrap


@app.command
@trap_packer_error
@app.command_option(CLUSTER_OPTION)
@requires.exactly('role')
def package_list(role):
  """usage: package_list --cluster=CLUSTER role

  Prints the names of packages owned by a user.
  """
  print '\n'.join(_get_packer().list_packages(role))


def _print_package(pkg):
  print 'Version: %s' % pkg['id']
  if 'metadata' in pkg and pkg['metadata']:
    print 'User metadata:\n  %s' % pkg['metadata']
  for audit in sorted(pkg['auditLog'], key=lambda k: int(k['timestamp'])):
    gmtime = strftime('%m/%d/%Y %H:%M:%S UTC', gmtime(int(audit['timestamp']) / 1000))
    print '  moved to state %s by %s on %s' % (audit['state'], audit['user'], gmtime)


@app.command
@trap_packer_error
@app.command_option(CLUSTER_OPTION)
@requires.exactly('role', 'package')
def package_versions(role, package):
  """usage: package_versions --cluster=CLUSTER role package

  Prints metadata about all of the versions of a package.
  """
  for version in _get_packer().list_versions(role, package):
    _print_package(version)


@app.command
@trap_packer_error
@app.command_option(CLUSTER_OPTION)
@requires.exactly('role', 'package', 'version')
def package_delete_version(role, package, version):
  """usage: package_delete_version --cluster=CLUSTER role package version

  Deletes a version of a package.
  """
  _get_packer().delete(role, package, version)
  print 'Version deleted'


@app.command
@trap_packer_error
@app.command_option(CLUSTER_OPTION)
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
  pkg = _get_packer().add(role, package, file_path, app.get_options().metadata)
  print 'Package added:'
  _print_package(pkg)


@app.command
@trap_packer_error
@app.command_option(CLUSTER_OPTION)
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
  pkg = _get_packer().get_version(role, package, version)
  if app.get_options().metadata_only:
    if not 'metadata' in pkg:
      client_util.die('Package does not contain any user-specified metadata.')
    else:
      print pkg['metadata']
  else:
    _print_package(pkg)


@app.command
@trap_packer_error
@app.command_option(CLUSTER_OPTION)
@requires.exactly('role', 'package', 'version')
def package_set_live(role, package, version):
  """usage: package_set_live --cluster=CLUSTER role package version

  Updates the 'live' label of a package to point to a specific version.
  """
  _get_packer().set_live(role, package, version)
  print 'Version %s is now the LIVE vesion' % version


@app.command
@trap_packer_error
@app.command_option(CLUSTER_OPTION)
@requires.exactly('role', 'package')
def package_unlock(role, package):
  """usage: package_unlock --cluster=CLUSTER role package

  Unlocks a package.
  Under normal circumstances, this should not be necessary.  However,
  in the event of a packer server crash it is possible.  If you find
  a need to use this, please inform mesos-team so they can investigate.
  """
  _get_packer().unlock(role, package)
  print 'Package unlocked'


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
    print generate_full_usage()
    sys.exit(0)

  if len(args) > 1:
    client_util.die('Please specify at most one subcommand.')

  subcmd = args[0]
  if subcmd in globals():
    app.command_parser(subcmd).print_help()
  else:
    print 'Subcommand %s not found.' % subcmd
    sys.exit(1)


def set_quiet(option, _1, _2, parser):
  setattr(parser.values, option.dest, True)
  LogOptions.set_stderr_log_level('NONE')


def set_verbose(option, _1, _2, parser):
  setattr(parser.values, option.dest, False)
  LogOptions.set_stderr_log_level('DEBUG')


def main():
  app.help()


if __name__ == '__main__':
  app.interspersed_args(True)
  app.add_option('-v',
                 dest='verbose',
                 default=False,
                 action='callback',
                 callback=set_verbose,
                 help='Verbose logging. (default: %default)')
  app.add_option('-q',
                 dest='verbose',
                 default=False,
                 action='callback',
                 callback=set_quiet,
                 help='Quiet logging. (default: %default)')
  LogOptions.set_stderr_log_level('INFO')
  LogOptions.disable_disk_logging()
  app.set_name('mesos-client')
  app.set_usage(DEFAULT_USAGE_BANNER + generate_terse_usage())
  app.main()

