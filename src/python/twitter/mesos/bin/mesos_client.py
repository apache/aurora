"""Command-line client for managing jobs with the Twitter mesos scheduler.
"""

# TODO(vinod): Use twitter.common.app subcommands instead of cmd
import cmd
import functools
import json
import os
import subprocess
import sys
import time
from urlparse import urljoin
import zookeeper

from pystachio import Ref

from twitter.common import app, log
from twitter.common.log.options import LogOptions
from twitter.mesos.clusters import Cluster
from twitter.mesos.client_wrapper import MesosClientAPI
from twitter.mesos.packer import sd_packer_client
from twitter.mesos.parsers.mesos_config import MesosConfig
from twitter.mesos.parsers.pystachio_config import PystachioConfig
from twitter.mesos.parsers.pystachio_codec import PystachioCodec
from twitter.thermos.base.options import add_binding_to

from gen.twitter.mesos.ttypes import *

__authors__ = ['William Farner',
               'Travis Crawford',
               'Alex Roetter']

LIVE_TASK_STATES = set([ScheduleStatus.KILLING,
                        ScheduleStatus.PREEMPTING,
                        ScheduleStatus.RUNNING,
                        ScheduleStatus.STARTING,
                        ScheduleStatus.UPDATING])


def _die(msg):
  log.fatal(msg)
  sys.exit(1)


def open_url(url):
  import webbrowser
  webbrowser.open_new_tab(url)


def synthesize_url(cluster, scheduler=None, role=None, job=None):
  if 'angrybird-local' in cluster:
    # TODO(vinod): Get the correct web port of the leading scheduler from zk!
    scheduler_url = 'http://%s:5051' % scheduler.real_host
  elif 'localhost' in cluster:
    scheduler_url = 'http://localhost:8081'
  else :
    cluster = Cluster.get(cluster)
    scheduler_url = cluster.proxy_url if cluster.is_service_proxied else \
      ('http://%s:8081' % scheduler.real_host)

  if job and not role:
    _die('If job specified, must specify role!')

  if not role and not job:
    return urljoin(scheduler_url, 'scheduler')
  elif role and not job:
    return urljoin(scheduler_url, 'scheduler/%s' % role)
  else:
    return urljoin(scheduler_url, 'scheduler/%s/%s' % (role, job))


def get_config(jobname, config_file, packer_factory):
  """Creates and returns a config object contained in the provided file."""

  options = app.get_options()
  config_type, is_json = options.config_type, options.json
  bindings = getattr(options, 'bindings', [])
  if options.copy_app_from:
    bindings.append({'mesos': {'package': os.path.basename(options.copy_app_from)}})

  if is_json:
    assert config_type == 'thermos', "--json only supported with thermos jobs"

  if config_type == 'mesos':
    config = MesosConfig(config_file, jobname)
    package = config.package()
    if package:
      if options.copy_app_from:
        _die('copy_app_from may not be used when a package spec is used in the configuration')
      if not isinstance(package, tuple) or len(package) is not 3:
        _die('package must be a tuple of exactly three elements')

      role, name, version = package
      log.info('Fetching metadata for package %s/%s version %s.' % package)
      metadata = packer_factory(config.cluster()).get_version(role, name, version)
      latest_audit = sorted(metadata['auditLog'], key=lambda a: a['timestamp'])[-1]
      if latest_audit['state'] == 'DELETED':
        _die('The requested package version has been deleted.')
      config.set_hdfs_path(metadata['uri'])
    elif config.hdfs_path():
      log.warning('''
*******************************************************************************
    hdfs_path in job configurations has been deprecated and will soon be
    disabled altogether.
    Please switch to using the package option as soon as possible!
    For details on how to do this, please consult
    http://go/mesostutorial
    and
    http://confluence.local.twitter.com/display/ENG/Mesos+Configuration+Reference
*******************************************************************************''')
    return config
  elif config_type == 'thermos':
    loader = PystachioConfig.load_json if is_json else PystachioConfig.load
    return loader(config_file, jobname, bindings)
  elif config_type == 'auto':
    return PystachioCodec(config_file, jobname)
  else:
    raise ValueError('Unknown config type %s!' % config_type)


def check_and_log_response(resp):
  log.info('Response from scheduler: %s (message: %s)'
      % (ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
  if resp.responseCode != ResponseCode.OK:
    sys.exit(1)


class requires(object):
  @staticmethod
  def wrap_function(fn, args, comparator):
    @functools.wraps(fn)
    def wrapped_function(self, line):
      sline = line.split()
      if not comparator(sline, args):
        _die('Incorrect parameters for %s: %s' % (fn.__name__, ' '.join(sline)))
      return fn(self, *sline)
    return wrapped_function

  @staticmethod
  def exactly(*args):
    def wrap(fn):
      return requires.wrap_function(fn, args, (lambda want, got: len(want) == len(got)))
    return wrap

  @staticmethod
  def at_least(*args):
    def wrap(fn):
      return requires.wrap_function(fn, args, (lambda want, got: len(want) >= len(got)))
    return wrap

  @staticmethod
  def nothing(fn):
    @functools.wraps(fn)
    def real_fn(self, line):
      return fn(self, *line.split())
    return real_fn


class MesosCLI(cmd.Cmd):
  def __init__(self, opts):
    cmd.Cmd.__init__(self)
    self.options = opts

  def handle_open(self, cluster, scheduler, role, job):
    url = synthesize_url(cluster, scheduler, role, job)
    log.info('Job url: %s' % url)
    if self.options.open_browser:
      open_url(url)

  @requires.exactly('job', 'config')
  def do_create(self, *line):
    """create job config"""
    (jobname, config_file) = line
    config = get_config(jobname, config_file, self._get_packer)
    if self.options.cluster is not None:
      log.warning('Deprecation warning: --cluster command line option is no longer supported.'
                  ' Cluster should be specified in the job config file.')
      assert self.options.cluster == config.cluster(), (
          'Command line --cluster and config file cluster do not match.')
    api = MesosClientAPI(cluster=config.cluster(), verbose=self.options.verbose)
    resp = api.create_job(config, self.options.copy_app_from)
    check_and_log_response(resp)
    self.handle_open(config.cluster(), api.scheduler(), config.role(), config.name())

  @requires.nothing
  def do_open(self, *args):
    """open --cluster=CLUSTER [role [job]]"""

    role = job = None
    if len(args) > 0:
      role = args[0]
    if len(args) > 1:
      job = args[1]

    if not self.options.cluster:
      _die('--cluster is required')

    api = MesosClientAPI(cluster=self.options.cluster, verbose=self.options.verbose)
    open_url(synthesize_url(self.options.cluster, api.scheduler(), role, job))

  @requires.exactly('job', 'config')
  def do_inspect(self, *line):
    """inspect job config"""
    (jobname, config_file) = line
    config = get_config(jobname, config_file, self._get_packer)
    cluster = Cluster.get(config.cluster())
    log.info('Parsed job config: %s' % config.job())

  @requires.exactly('role', 'job')
  def do_start_cron(self, *line):
    """start_cron role job"""
    (role, jobname) = line

    api = MesosClientAPI(cluster=self.options.cluster, verbose=self.options.verbose)
    resp = api.start_cronjob(role, jobname)
    check_and_log_response(resp)
    self.handle_open(self.options.cluster, api.scheduler(), role, jobname)

  @requires.exactly('role', 'job')
  def do_kill(self, *line):
    """kill role job"""
    (role, jobname) = line

    api = MesosClientAPI(cluster=self.options.cluster, verbose=self.options.verbose)
    resp = api.kill_job(role, jobname)
    check_and_log_response(resp)
    self.handle_open(self.options.cluster, api.scheduler(), role, jobname)

  @requires.exactly('role', 'job')
  def do_status(self, *line):
    """status role job"""

    ACTIVE_STATES = set([ScheduleStatus.PENDING, ScheduleStatus.STARTING, ScheduleStatus.RUNNING])
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

    (role, jobname) = line

    api = MesosClientAPI(cluster=self.options.cluster, verbose=self.options.verbose)
    resp = api.check_status(role, jobname)
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

  @requires.exactly('job', 'config')
  def do_update(self, *line):
    """update job config"""
    (jobname, config_file) = line
    config = get_config(jobname, config_file, self._get_packer)
    api = MesosClientAPI(cluster=config.cluster(), verbose=self.options.verbose)
    resp = api.update_job(config, self.options.copy_app_from)
    check_and_log_response(resp)

  @requires.exactly('role', 'job')
  def do_cancel_update(self, *line):
    """cancel_update role job"""
    (role, jobname) = line

    api = MesosClientAPI(cluster=self.options.cluster, verbose=self.options.verbose)
    resp = api.cancel_update(role, jobname)
    check_and_log_response(resp)

  @requires.exactly('role')
  def do_get_quota(self, *line):
    """get_quota role"""
    role = line[0]
    api = MesosClientAPI(cluster=self.options.cluster, verbose=self.options.verbose)
    resp = api.get_quota(role)
    quota = resp.quota

    quota_fields = [
      ('CPU', quota.numCpus),
      ('RAM', '%f GB' % (float(quota.ramMb) / 1024)),
      ('Disk', '%f GB' % (float(quota.diskMb) / 1024))
    ]
    log.info('Quota for %s:\n\t%s' %
             (role, '\n\t'.join(['%s\t%s' % (k, v) for (k, v) in quota_fields])))

  # TODO(wfarner): Revisit this once we have a better way to add named args per sub-cmmand
  @requires.exactly('role', 'cpu', 'ramMb', 'diskMb')
  def do_set_quota(self, *line):
    """set_quota role cpu ramMb diskMb"""
    (role, cpu_str, ram_mb_str, disk_mb_str) = line

    try:
      cpu = float(cpu_str)
      ram_mb = int(ram_mb_str)
      disk_mb = int(disk_mb_str)
    except ValueError:
      log.error('Invalid value')

    api = MesosClientAPI(cluster=self.options.cluster, verbose=self.options.verbose)
    resp = api.set_quota(role, cpu, ram_mb, disk_mb)
    check_and_log_response(resp)

  @requires.exactly('task_id', 'state')
  def do_force_task_state(self, *line):
    """force_task_state task_id state"""
    (task_id, state) = line
    status = ScheduleStatus._NAMES_TO_VALUES.get(state)
    if status is None:
      log.error('Unrecognized status "%s", must be one of [%s]'
                % (state, ', '.join(ScheduleStatus._NAMES_TO_VALUES.keys())))
      sys.exit(1)

    api = MesosClientAPI(cluster=self.options.cluster, verbose=self.options.verbose)
    resp = api.force_task_state(task_id, status)
    check_and_log_response(resp)

  def query(self, role, job, shards=None, statuses=LIVE_TASK_STATES, api=None):
    query = TaskQuery()
    query.statuses = statuses
    query.owner = Identity(role=role)
    query.jobName = job
    query.shardIds = shards
    api = api or MesosClientAPI(cluster=self.options.cluster, verbose=self.options.verbose)
    return api.client().getTasksStatus(query)

  @requires.exactly('role', 'job', 'shard')
  def do_ssh(self, *line):
    (role, job, shard) = line
    resp = self.query(role, job, set([int(shard)]))
    if not resp.responseCode:
      _die('Request failed, server responded with "%s"' % resp.message)
    return subprocess.call(['ssh', resp.tasks[0].assignedTask.slaveHost])

  @requires.at_least('role', 'job', 'cmd')
  def do_run(self, *line):
    """run role job cmd"""
    # TODO(William Farner): Add support for invoking on individual shards.
    # This will require some arg refactoring in the client.
    (role, jobs) = line[:2]
    cmd = list(line[2:])

    if not self.options.cluster:
      _die('--cluster is required')

    tasks = []
    api = MesosClientAPI(cluster=self.options.cluster, verbose=self.options.verbose)
    for job in jobs.split(','):
      resp = self.query(role, job, api=api)
      if not resp.responseCode:
        log.error('Failed to query job: %s' % job)
        continue
      tasks.extend(resp.tasks)

    def replace(match, replace_val):
      def _replace(value):
        return value.replace(match, str(replace_val))
      return _replace

    # TODO(William Farner): Support parallelization of this process with -t ala loony.
    for task in tasks:
      host = task.assignedTask.slaveHost
      full_cmd = ['ssh', '-n', '-q', host, 'cd', '/var/run/nexus/%task_id%/sandbox;']
      full_cmd += cmd

      # Populate command line wildcards.
      full_cmd = map(replace('%shard_id%', task.assignedTask.task.shardId), full_cmd)
      full_cmd = map(replace('%task_id%', task.assignedTask.taskId), full_cmd)
      for name, port in task.assignedTask.assignedPorts.items():
        full_cmd = map(replace('%port:' + name + '%', port), full_cmd)

      proc = subprocess.Popen(full_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
      output = proc.communicate()
      print '\n'.join(['%s:  %s' % (host, line) for line in output[0].splitlines()])


  def _get_packer(self, cluster=None):
    cluster = cluster or self.options.cluster
    if not cluster:
      _die('--cluster must be specified')
    return sd_packer_client.create_packer(Cluster.get(cluster))

  @requires.exactly('role')
  def do_package_list(self, role):
    print '\n'.join(self._get_packer().list_packages(role))

  @staticmethod
  def _print_package(pkg):
    print 'Version: %s' % pkg['id']
    for audit in sorted(pkg['auditLog'], key=lambda k: int(k['timestamp'])):
      gmtime = time.strftime('%m/%d/%Y %H:%M:%S UTC',
                             time.gmtime(int(audit['timestamp']) / 1000))
      print '  moved to state %s by %s on %s' % (audit['state'], audit['user'], gmtime)

  @requires.exactly('role', 'package')
  def do_package_versions(self, role, package):
    for version in self._get_packer().list_versions(role, package):
      MesosCLI._print_package(version)

  @requires.exactly('role', 'package', 'version')
  def do_package_delete_version(self, role, package, version):
    self._get_packer().delete(role, package, version)
    print 'Version deleted'

  @requires.exactly('role', 'package', 'file_path')
  def do_package_add_version(self, role, package, file_path):
    print 'Package added:'
    MesosCLI._print_package(self._get_packer().add(role, package, file_path))

  @requires.exactly('role', 'package')
  def do_package_unlock(self, role, package):
    self._get_packer().unlock(role, package)
    print 'Package unlocked'


def initialize_options():
  usage = """Mesos command-line interface.

For more information contact mesos-users@twitter.com, or explore the
documentation at https://sites.google.com/a/twitter.com/mesos/

Usage generally involves running a subcommand with positional arguments.
The subcommands and their arguments are:
"""

  for method in sorted(dir(MesosCLI)):
    if method.startswith('do_') and getattr(MesosCLI, method).__doc__:
      usage = usage + '\n    ' + getattr(MesosCLI, method).__doc__

  # TODO(William Farner): Split args out on a per-subcommand basis.
  app.set_usage(usage)
  app.interspersed_args(True)
  app.add_option('-v', dest='verbose', default=False, action='store_true',
                 help='Verbose logging. (default: %default)')
  app.add_option('--cluster', dest='cluster', default=None,
                  help='Cluster to use for commands that do not take configuration, '
                       'e.g. kill, start_cron, get_quota.')
  app.add_option('-o', '--open_browser', dest='open_browser', action='store_true', default=False,
                 help="Open a browser window to the job page after a job mutation.")
  app.add_option('--copy_app_from', dest='copy_app_from', default=None,
                  help="Local path to use for the app (will copy to the cluster)"
                       " (default: %default)")
  app.add_option('-c', '--config_type', type='choice', action='store', dest='config_type',
                 choices=['mesos', 'thermos', 'auto'], default='mesos',
                 help='The type of the configuration file supplied.  Options are `mesos`, the '
                      'new `thermos` format, or `auto`, which automatically converts from '
                      'mesos to thermos configs.')
  app.add_option('-j', '--json', default=False, action='store_true',
                 help="If specified, configuration is read in JSON format.")
  app.add_option('-E', type='string', nargs=1, action='callback', default=[], metavar='NAME:VALUE',
                 callback=add_binding_to('bindings'), dest='bindings',
                 help='bind an environment name to a value.')


def main(args, options):
  if not args:
    app.help()
    sys.exit(1)

  if options.verbose:
    LogOptions.set_stderr_log_level('DEBUG')
    zookeeper.set_debug_level(zookeeper.LOG_LEVEL_DEBUG)
  else:
    LogOptions.set_stderr_log_level('INFO')
    zookeeper.set_debug_level(zookeeper.LOG_LEVEL_ERROR)

  cli = MesosCLI(options)

  try:
    cli.onecmd(' '.join(args))
  except AssertionError as e:
    _die('\n%s' % e)


LogOptions.disable_disk_logging()
initialize_options()
app.main()
