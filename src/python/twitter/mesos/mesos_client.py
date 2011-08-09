#!/usr/bin/env python2.6

"""Command-line client for managing jobs with the Twitter mesos scheduler.
"""

import cmd
import copy
import logging
import os
import subprocess
import sys
import time
import getpass
import pickle
import zookeeper
import errno
from optparse import OptionParser

from twitter.common import app
from twitter.common import options
from twitter.common import log
from twitter.common.log.options import LogOptions

from twitter.mesos.mesos import clusters
from twitter.mesos.mesos.location import Location
from twitter.mesos.mesos.mesos_configuration import MesosConfiguration
from twitter.mesos.mesos import scheduler_client
from twitter.mesos.mesos.tunnel_helper import TunnelHelper
from twitter.mesos.mesos.session_key_helper import SessionKeyHelper
from twitter.mesos.mesos.updater import *

from mesos_twitter.ttypes import *

__authors__ = ['William Farner',
               'Travis Crawford',
               'Alex Roetter']

def _die(msg):
  log.fatal(msg)
  sys.exit(1)

class MesosCookieHelper:
  @staticmethod
  def chmod_600(path):
    path_mode = int('600', 8)
    os.chmod(path, path_mode)

  @staticmethod
  def safe_create(path):
    if not os.path.exists(path):
      wp = open(path, "w")
      wp.close()
    try:
      MesosCookieHelper.chmod_600(path)
      return True
    except:
      return False

  @staticmethod
  def get_cookie_path():
    path = os.getenv('HOME')
    if not path:
      _die('$HOME is not set!')
    return os.path.join(path, '.mesos_session')

  @staticmethod
  def save_cookie_or_die(cookie):
    path = MesosCookieHelper.get_cookie_path()
    if not MesosCookieHelper.safe_create(path):
      _die('Unable to create %s!' % path)
    with open(path, "w") as wp:
      try:
        pickle.dump(cookie, wp)
      except Exception, e:
        _die("Failed to dump session cookie into %s: %s" % (path, e))

  @staticmethod
  def read_cached_cookie():
    path = MesosCookieHelper.get_cookie_path()
    with open(path, "r") as wp:
      try:
        return pickle.load(wp)
      except:
        return None

  @staticmethod
  def clear_cookie():
    path = MesosCookieHelper.get_cookie_path()
    try:
      os.remove(path)
    except OSError, e:
      if e.errno == errno.ENOENT:
        pass
      else:
        raise e

class MesosCLIHelper:
  @staticmethod
  def acquire_session_key_or_die(owner, try_cached=False, save=False):
    key = SessionKey(user = owner)
    SessionKeyHelper.sign_session(key)
    return key

  @staticmethod
  def call(cmd, host):
    if host is not None:
      return subprocess.call(['ssh', host, ' '.join(cmd)])
    else:
      return subprocess.call(cmd)

  @staticmethod
  def check_call(cmd, host):
    if host is not None:
      return subprocess.check_call(['ssh', host, ' '.join(cmd)])
    else:
      return subprocess.check_call(cmd)

  @staticmethod
  def assert_valid_cluster(cluster):
    assert cluster, "The --cluster argument is required for this command."
    if cluster.find('localhost:') == 0:
      scluster = cluster.split(':')
      if len(scluster) == 2:
        try:
          int(scluster[1])
        except ValueError, e:
          _die('The cluster argument is invalid: %s (error: %s)' % (cluster, e))
    else:
      clusters.assert_exists(cluster)

  @staticmethod
  def get_cluster(cmd_line_arg_cluster, config_cluster):
    cluster = None
    if cmd_line_arg_cluster is not None:
      cluster = cmd_line_arg_cluster
    if config_cluster is not None:
      cluster = config_cluster
    if cmd_line_arg_cluster is not None and config_cluster is not None:
      if cmd_line_arg_cluster != config_cluster:
        log.error(
        """Warning: --cluster and the cluster in your configuration do not match.
           Using the cluster specified on the command line. (cluster = %s)""" % cmd_line_arg_cluster)
        cluster = cmd_line_arg_cluster
    MesosCLIHelper.assert_valid_cluster(cluster)
    return cluster

  @staticmethod
  def copy_to_hadoop(ssh_proxy_host, src, dst):
    """Copy the local file src to a hadoop path dst. Should be used in
    conjunction with starting new mesos jobs
    """
    abs_src = os.path.expanduser(src)
    dst_dir = os.path.dirname(dst)

    assert os.path.exists(abs_src), 'App file does not exist, cannot continue - %s' % abs_src

    location = Location.get_location()

    hdfs_src = abs_src if location != Location.CORP else os.path.basename(abs_src)

    if ssh_proxy_host:
      log.info('Running in corp, copy will be done via %s' % ssh_proxy_host)
      subprocess.check_call(['scp', abs_src, '%s:' % ssh_proxy_host])

    if not MesosCLIHelper.call(['hadoop', 'fs', '-test', '-e', dst], ssh_proxy_host):
      log.info("Deleting existing file at %s" % dst)
      MesosCLIHelper.check_call(['hadoop', 'fs', '-rm', dst], ssh_proxy_host)
    elif MesosCLIHelper.call(['hadoop', 'fs', '-test', '-e', dst_dir], ssh_proxy_host):
      log.info('Creating directory %s' % dst_dir)
      MesosCLIHelper.check_call(['hadoop', 'fs', '-mkdir', dst_dir], ssh_proxy_host)

    log.info('Copying %s -> %s' % (hdfs_src, dst))
    MesosCLIHelper.check_call(['hadoop', 'fs', '-put', hdfs_src, dst], ssh_proxy_host)

  @staticmethod
  def copy_app_to_hadoop(source_path, hdfs_path, cluster, ssh_proxy):
    assert hdfs_path is not None, 'No target HDFS path specified'
    hdfs = clusters.get_hadoop_uri(cluster)
    hdfs_uri = '%s%s' % (hdfs, hdfs_path)
    MesosCLIHelper.copy_to_hadoop(ssh_proxy, source_path, hdfs_uri)

class requires_arguments(object):
  import functools

  def __init__(self, *args):
     self.expected_args = args

  def __call__(self, fn):
    def wrapped_fn(obj, line):
      sline = line.split()
      if len(sline) != len(self.expected_args):
        _die('Expected %d args, got %d for: %s' % (
            len(self.expected_args), len(sline), line))
      return fn(obj, *sline)
    wrapped_fn.__doc__ = fn.__doc__
    return wrapped_fn

class MesosCLI(cmd.Cmd):
  def __init__(self, opts):
    cmd.Cmd.__init__(self)
    self.options = opts
    zookeeper.set_debug_level(zookeeper.LOG_LEVEL_WARN)
    MesosCLIHelper.assert_valid_cluster(self.options.cluster)
    if self.options.force_authenticate:
      MesosCookieHelper.clear_cookie()
    self._construct_scheduler()  # populates ._proxy, ._scheduler, ._client

  @staticmethod
  def get_cron_collision_value(valueStr):
    if valueStr is None:
      return None
    if valueStr in CronCollisionPolicy._NAMES_TO_VALUES:
      return CronCollisionPolicy._NAMES_TO_VALUES[valueStr]
    else:
      log.fatal('Invalid cron collision policy %s' % valueStr)
      log.fatal('Must be one of %s' % CronCollisionPolicy._NAMES_TO_VALUES.keys())
      sys.exit(2)

  @staticmethod
  def get_scheduler_client(cluster, **kwargs):
    location = Location.get_location()
    assert location is not None, 'Failed to detect location, unable to continue.'

    log.info('Auto-detected location: %s' % location)

    if cluster.find('localhost:') == 0:
      log.info('Attempting to talk to local scheduler.')
      port = int(cluster.split(':')[1])
      return (None,
              scheduler_client.LocalSchedulerClient(port, ssl=True))
    else:
      ssh_proxy_host = TunnelHelper.get_tunnel_host(cluster) if location == Location.CORP else None
      if ssh_proxy_host is not None:
        log.info('Proxying through %s' % ssh_proxy_host)

      zk_host = clusters.get_zk_host(cluster)
      scheduler_path = clusters.get_scheduler_zk_path(cluster)

      return (ssh_proxy_host,
              scheduler_client.ZookeeperSchedulerClient(cluster, ssl=True, **kwargs))

  def _construct_scheduler(self):
    self._proxy, self._scheduler = MesosCLI.get_scheduler_client(
      self.options.cluster, verbose=self.options.verbose)
    assert self._scheduler, "Could not find scheduler (cluster = %s)" % self.options.cluster
    self._client = self._scheduler.get_thrift_client()
    assert self._client, "Could not construct thrift client."

  def read_config(self, *line):
    (job, config) = line
    query = TaskQuery()
    query.jobName = job

    jobs = MesosConfiguration(config).config

    if not query.jobName in jobs:
      _die('Could not find job "%s" in configuration file "%s"' % (
        query.jobName, config))

    job = jobs[query.jobName]
    if 'owner' in job:
      if 'role' in job:
        _die('both role and owner specified!')
      else:
        log.warning('WARNING: the owner field is deprecated.  use "role" instead.')
        job['role'] = job['owner']
    if 'role' not in job:
      _die('role must be specified!')
    query.owner = Identity(role = job['role'], user = getpass.getuser())

    # Force configuration map to be all strings.
    taskConfig = {}
    for k, v in job['task'].items():
      taskConfig[k] = str(v)
    task = TwitterTaskInfo()
    task.configuration = taskConfig

    # Replicate task objects to reflect number of instances.
    tasks = []
    for i in range(0, job['instances']):
      taskCopy = copy.deepcopy(task)
      taskCopy.shardId = i
      tasks.append(taskCopy)

    update_config_dict = job['update_config']
    update_config = UpdateConfig(update_config_dict['batchSize'],
        update_config_dict['restartThreshold'], update_config_dict['watchSecs'],
        update_config_dict['maxPerShardFailures'], update_config_dict['maxTotalFailures'])

    cron_collision_policy = None
    if 'cron_collision_policy' in job: cron_collision_policy = job['cron_collision_policy']
    return (job, query.owner, tasks, MesosCLI.get_cron_collision_value(cron_collision_policy),
        update_config)

  def acquire_session(self):
    return MesosCLIHelper.acquire_session_key_or_die(getpass.getuser(),
      self.options.cache_credentials,
      self.options.cache_credentials)

  @requires_arguments('job', 'config')
  def do_create(self, *line):
    """create job config"""
    (job, owner, tasks, cron_collision_policy, _) = self.read_config(*line)

    cluster = MesosCLIHelper.get_cluster(self.options.cluster, job.get('cluster'))

    if self.options.copy_app_from is not None:
      MesosCLIHelper.copy_app_to_hadoop(self.options.copy_app_from,
          job['task']['hdfs_path'], cluster, self._proxy)

    sessionkey = self.acquire_session()

    log.info('Creating job %s' % job['name'])
    resp = self._client.createJob(
      JobConfiguration(job['name'], owner, tasks, job['cron_schedule'],
          cron_collision_policy), sessionkey)
    log.info('Response from scheduler: %s (message: %s)'
      % (ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
    if resp.responseCode == ResponseCode.AUTH_FAILED:
      MesosCookieHelper.clear_cookie()

  @requires_arguments('role', 'job')
  def do_start_cron(self, *line):
    """start_cron role job"""

    (role, job) = line
    resp = self._client.startCronJob(role, job, self.acquire_session())
    log.info('Response from scheduler: %s (message: %s)'
      % (ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
    if resp.responseCode == ResponseCode.AUTH_FAILED:
      MesosCookieHelper.clear_cookie()

  @requires_arguments('role', 'job')
  def do_kill(self, *line):
    """kill role job"""

    log.info('Killing tasks')
    (role, job) = line
    query = TaskQuery()
    query.owner = Identity(role = role)
    query.jobName = job

    sessionkey = self.acquire_session()

    resp = self._client.killTasks(query, sessionkey)
    log.info('Response from scheduler: %s (message: %s)'
      % (ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
    if resp.responseCode == ResponseCode.AUTH_FAILED:
      MesosCookieHelper.clear_cookie()

  @requires_arguments('role', 'job')
  def do_status(self, *line):
    """status role job"""

    log.info('Fetching tasks status')
    (role, job) = line
    query = TaskQuery()
    query.owner = Identity(role = role)
    query.jobName = job

    ACTIVE_STATES = set([ScheduleStatus.PENDING, ScheduleStatus.STARTING, ScheduleStatus.RUNNING])
    def is_active(task):
      return task.scheduledTask.status in ACTIVE_STATES

    def print_task(task):
      scheduledTask = task.scheduledTask
      assignedTask = scheduledTask.assignedTask
      taskInfo = assignedTask.task
      taskString = """\tHDFS path: %s
\tcpus: %s, ram: %s MB, disk: %s MB""" % (taskInfo.hdfsPath,
                                          taskInfo.numCpus,
                                          taskInfo.ramMb,
                                          taskInfo.diskMb)
      if task.resources is not None:
        taskString += '\n\tports: %s' % task.resources.leasedPorts
      taskString += '\n\tfailure count: %s (max %s)' % (scheduledTask.failureCount,
                                                        taskInfo.maxTaskFailures)
      return taskString

    resp = self._client.getTasksStatus(query)
    log.info('Response from scheduler: %s (message: %s)'
      % (ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))

    def print_tasks(tasks):
      for task in tasks:
        scheduledTask = task.scheduledTask
        assignedTask = scheduledTask.assignedTask
        taskString = print_task(task)

        log.info('shard: %s, status: %s on %s\n%s' %
               (assignedTask.task.shardId,
                ScheduleStatus._VALUES_TO_NAMES[scheduledTask.status],
                assignedTask.slaveHost,
                taskString))

    if resp.tasks:
      active_tasks = filter(is_active, resp.tasks)
      log.info('Active Tasks (%s)' % len(active_tasks))
      print_tasks(active_tasks)
      inactive_tasks = filter(lambda x: not is_active(x), resp.tasks)
      log.info('Inactive Tasks (%s)' % len(inactive_tasks))
      print_tasks(inactive_tasks)
    else:
      log.info('No tasks found.')

  @requires_arguments('job', 'config')
  def do_update(self, *line):
    """update job config"""

    (job, owner, tasks, cron_collision_policy, update_config) = self.read_config(*line)

    cluster = MesosCLIHelper.get_cluster(self.options.cluster, job.get('cluster'))

    # TODO(William Farner): Add a rudimentary versioning system here so application updates
    #                       don't overwrite original binary.
    if self.options.copy_app_from is not None:
      MesosCLIHelper.copy_app_to_hadoop(self.options.copy_app_from, job['task']['hdfs_path'],
          cluster, self._proxy)

    sessionkey = self.acquire_session()

    log.info('Updating Job %s' % job['name'])

    resp = self._client.startUpdate(
      JobConfiguration(job['name'], owner, tasks, job['cron_schedule'], cron_collision_policy),
          sessionkey)
    if resp.responseCode == ResponseCode.AUTH_FAILED:
      MesosCookieHelper.clear_cookie()

    if resp.responseCode != ResponseCode.OK:
      log.warning('Response from scheduler: %s (message: %s)'
          % (ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
      return

    # TODO(William Farner): Cleanly handle connection failures in case the scheduler
    #                       restarts mid-update.
    updater = Updater(owner.role, job, self._client, time, resp.updateToken, sessionkey)

    failed_shards = updater.update(JobConfiguration(job['name'], owner, tasks, job['cron_schedule'],
        cron_collision_policy, update_config))

    if failed_shards:
      log.info('Update reverted, failures detected on shards %s' % failed_shards)
      resp = self._client.finishUpdate(owner.role, job['name'], UpdateResult.FAILED,
          resp.updateToken, sessionkey)
    else:
      log.info('Update Successful')
      resp = self._client.finishUpdate(owner.role, job['name'], UpdateResult.SUCCESS,
          resp.updateToken, sessionkey)
    if resp.responseCode != UpdateResponseCode.OK:
      log.info('Response from scheduler: %s (message: %s)'
        % (UpdateResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))

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

  options.set_usage(usage)
  options.add(
    '-v',
    dest='verbose',
    default=False,
    action='store_true',
    help='Verbose logging. (default: %default)')
  options.add(
    '-q',
    dest='quiet',
    default=False,
    action='store_true',
    help='Minimum logging. (default: %default)')
  options.add(
    '--cluster',
    dest='cluster',
    help='Cluster to launch the job in (e.g. sjc1, smf1-prod, smf1-nonprod)')
  options.add(
    '--copy_app_from',
    dest='copy_app_from',
    default=None,
    help="Local path to use for the app (will copy to the cluster)" + \
          " (default: %default)")
  options.add(
    '--nocache_credentials',
    dest='cache_credentials',
    default=True,
    action='store_false',
    help="Turn off session credential caching (in $HOME/.mesos_session")
  options.add(
    '--force_authenticate',
    dest='force_authenticate',
    default=False,
    action='store_true',
    help="Force reauthentication with Mesos master, replacing cached credentials.")

def main(args):
  values = options.values()

  if values.quiet:
    LogOptions.set_stdout_log_level('NONE')
  else:
    if values.verbose:
      LogOptions.set_stdout_log_level('DEBUG')
    else:
      LogOptions.set_stdout_log_level('INFO')

  if not args:
    options.help()
    return

  cli = MesosCLI(values)

  try:
    cli.onecmd(' '.join(args))
  except AssertionError as e:
    _die('\n%s' % e)

initialize_options()
app.main()
