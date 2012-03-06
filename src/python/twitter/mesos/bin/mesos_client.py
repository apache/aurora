#!/usr/bin/env python2.6

"""Command-line client for managing jobs with the Twitter mesos scheduler.
"""

# TODO(vinod): Use twitter.common.app subcommands instead of cmd
import cmd
import functools
import getpass
import sys
from urlparse import urljoin
import zookeeper
from pystachio import Ref

from twitter.common import app, log
from twitter.common.log.options import LogOptions
from twitter.mesos.clusters import Cluster
from twitter.mesos.client_wrapper import MesosClientAPI, MesosHelper
from twitter.mesos.parsers.mesos_config import MesosConfig
from twitter.mesos.parsers.pystachio_config import PystachioConfig
from twitter.thermos.base.options import add_binding_to

from gen.twitter.mesos.ttypes import (
  ResponseCode,
  ScheduleStatus,
  UpdateResponseCode
)

__authors__ = ['William Farner',
               'Travis Crawford',
               'Alex Roetter']


def _die(msg):
  log.fatal(msg)
  sys.exit(1)


def open_url(url):
  import webbrowser
  webbrowser.open_new_tab(url)


def synthesize_url(cluster, scheduler=None, role=None, job=None):
  cluster = Cluster.get(cluster)
  scheduler_url = cluster.proxy_url if cluster.is_service_proxied else (
    'http://%s:8081' % scheduler.real_host)

  if job and not role:
    _die('If job specified, must specify role!')

  if not role and not job:
    return urljoin(scheduler_url, 'scheduler')
  elif role and not job:
    return urljoin(scheduler_url, 'scheduler/role?role=%s' % role)
  else:
    return urljoin(scheduler_url, 'scheduler/job?role=%s&job=%s' % (role, job))


def choose_cluster(config):
  cmdline_cluster = app.get_options().cluster
  if config.cluster() and cmdline_cluster and config.cluster() != cmdline_cluster:
    log.error(
      """Warning: --cluster and the cluster in your configuration do not match.
         Using the cluster specified on the command line. (cluster = %s)""" % cmdline_cluster)
  return cmdline_cluster or config.cluster()


def is_admin():
  try:
    MesosHelper.acquire_session_key_or_die('mesos')
  except:
    return False
  return True


def get_config(jobname, config_file, new_mesos, is_json, bindings=None):
  """Returns the proxy config."""
  if new_mesos:
    assert is_admin(), ("--new_mesos is currently only allowed"
                        " for users in the mesos group.")
    loader = PystachioConfig.load_json if is_json else PystachioConfig.load
    config = loader(config_file, jobname, bindings)
  else:
    assert not is_json, "--json only supported with pystachio jobs"
    config = MesosConfig(config_file, jobname)
  return config


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
    config = get_config(jobname, config_file, self.options.new_mesos, self.options.json,
                        getattr(self.options, 'bindings', None))
    api = MesosClientAPI(cluster=choose_cluster(config), verbose=self.options.verbose)
    resp = api.create_job(config, self.options.copy_app_from)
    check_and_log_response(resp)
    self.handle_open(choose_cluster(config), api.scheduler(), config.role(), config.name())

  @requires.nothing
  def do_open(self, *args):
    """open --cluster=CLUSTER [role [job]]"""

    role = job = None
    if len(args) > 0:
      role = args[0]
    if len(args) > 1:
      job = args[1]

    if not self.options.cluster:
      _die('--cluster required!')

    api = MesosClientAPI(cluster=self.options.cluster, verbose=self.options.verbose)
    open_url(synthesize_url(self.options.cluster, api.scheduler(), role, job))

  @requires.exactly('job', 'config')
  def do_inspect(self, *line):
    """inspect job config"""
    (jobname, config_file) = line
    config = get_config(jobname, config_file, self.options.new_mesos, self.options.json,
                         getattr(self.options, 'bindings', None))
    if self.options.copy_app_from and config.hdfs_path():
      log.info('Detected HDFS package: %s' % config.hdfs_path())
      log.info('Would copy to %s via %s.' % (self.cluster(), self.proxy()))
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
    config = get_config(jobname, config_file, self.options.new_mesos, self.options.json,
                        getattr(self.options, 'bindings', None))

    api = MesosClientAPI(cluster=choose_cluster(config), verbose=self.options.verbose)
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

  app.set_usage(usage)
  app.interspersed_args(True)
  app.add_option('-v', dest='verbose', default=False, action='store_true',
                 help='Verbose logging. (default: %default)')
  app.add_option('-q', dest='quiet', default=False, action='store_true',
                  help='Minimum logging. (default: %default)')
  app.add_option('--cluster', dest='cluster', default=None,
                  help="Cluster to launch the job in (e.g. sjc1, smf1-prod, smf1-nonprod)."
                        " NOTE: If specified, this option overrides the cluster specified "
                        "in the config file. ")
  app.add_option('-o', '--open_browser', dest='open_browser', action='store_true', default=False,
                 help="Open a browser window to the job page after a job mutation.")
  app.add_option('--copy_app_from', dest='copy_app_from', default=None,
                  help="Local path to use for the app (will copy to the cluster)"
                       " (default: %default)")
  app.add_option('-n', '--new_mesos', default=False, action='store_true',
                 help="Run jobs configured using the new mesos config format.")
  app.add_option('-j', '--json', default=False, action='store_true',
                 help="Jobs are written in json.")
  app.add_option('-E', type='string', nargs=1, action='callback', default=[], metavar='NAME:VALUE',
                 callback=add_binding_to('bindings'), dest='bindings',
                 help='bind an environment name to a value.')


def main(args, options):
  if not args:
    app.help()
    sys.exit(1)

  if options.quiet:
    LogOptions.set_stderr_log_level('NONE')
    zookeeper.set_debug_level(zookeeper.LOG_LEVEL_ERROR)
  else:
    if options.verbose:
      LogOptions.set_stderr_log_level('DEBUG')
      zookeeper.set_debug_level(zookeeper.LOG_LEVEL_DEBUG)
    else:
      LogOptions.set_stderr_log_level('INFO')
      zookeeper.set_debug_level(zookeeper.LOG_LEVEL_INFO)

  cli = MesosCLI(options)

  try:
    cli.onecmd(' '.join(args))
  except AssertionError as e:
    _die('\n%s' % e)


initialize_options()
app.main()
