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

from twitter.common import app, log
from twitter.common.log.options import LogOptions

from twitter.mesos import clusters
from twitter.mesos.location import Location
from twitter.mesos.scheduler_client import LocalSchedulerClient, ZookeeperSchedulerClient
from twitter.mesos.tunnel_helper import TunnelHelper
from twitter.mesos.session_key_helper import SessionKeyHelper
from twitter.mesos.updater import Updater
from twitter.mesos.proxy_config import ProxyConfig
from gen.twitter.mesos.ttypes import *

__authors__ = ['William Farner',
               'Travis Crawford',
               'Alex Roetter']

def _die(msg):
  log.fatal(msg)
  sys.exit(1)

class MesosCLIHelper:
  @staticmethod
  def acquire_session_key_or_die(owner):
    key = SessionKey(user = owner)
    SessionKeyHelper.sign_session(key, owner)
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
    assert cluster, "Cluster not specified!"
    if cluster.find('localhost:') == 0:
      scluster = cluster.split(':')
      if len(scluster) == 2:
        try:
          int(scluster[1])
        except ValueError as e:
          _die('The cluster argument is invalid: %s (error: %s)' % (cluster, e))
    else:
      clusters.assert_exists(cluster)

  @staticmethod
  def get_cluster(cmd_line_arg_cluster, config_cluster):
    if config_cluster and cmd_line_arg_cluster and config_cluster != cmd_line_arg_cluster:
      log.error(
      """Warning: --cluster and the cluster in your configuration do not match.
         Using the cluster specified on the command line. (cluster = %s)""" % cmd_line_arg_cluster)
    cluster = cmd_line_arg_cluster
    cluster = cluster or config_cluster
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

def log_scheduler(rc):
  log.info('Response from scheduler: %s (message: %s)'
      % (ResponseCode._VALUES_TO_NAMES[rc.responseCode], rc.message))

class MesosCLI(cmd.Cmd):
  def __init__(self, opts):
    cmd.Cmd.__init__(self)
    self.options = opts
    zookeeper.set_debug_level(zookeeper.LOG_LEVEL_WARN)
    self._cluster = self._config = self._client = self._proxy = self._scheduler = None

  def client(self):
    if not self._client:
      self._construct_scheduler(self._config)
    return self._client

  def proxy(self):
    if not self._proxy:
      self._construct_scheduler(self._config)
    return self._proxy

  def cluster(self):
    if not self._cluster:
      self._construct_scheduler(self._config)
    return self._cluster

  def config(self):
    assert self._config, 'Config is unset, have you called get_and_set_config?'
    return self._config

  @staticmethod
  def get_scheduler_client(cluster, **kwargs):
    location = Location.get_location()
    assert location is not None, 'Failed to detect location, unable to continue.'

    log.info('Auto-detected location: %s' % location)

    if cluster.find('localhost:') == 0:
      log.info('Attempting to talk to local scheduler.')
      port = int(cluster.split(':')[1])
      return (None, LocalSchedulerClient(port, ssl=True))
    else:
      ssh_proxy_host = TunnelHelper.get_tunnel_host(cluster) if location == Location.CORP else None
      if ssh_proxy_host is not None:
        log.info('Proxying through %s' % ssh_proxy_host)

      zk_host = clusters.get_zk_host(cluster)
      scheduler_path = clusters.get_scheduler_zk_path(cluster)

      return (ssh_proxy_host, ZookeeperSchedulerClient(cluster, ssl=True, **kwargs))

  def _construct_scheduler(self, config=None):
    """
      Populates:
        self._cluster
        self._proxy (if proxy necessary)
        self._scheduler
        self._client
    """
    self._cluster = MesosCLIHelper.get_cluster(
      self.options.cluster, config.cluster() if config else None)
    self._proxy, self._scheduler = MesosCLI.get_scheduler_client(
      self._cluster, verbose=self.options.verbose)
    assert self._scheduler, "Could not find scheduler (cluster = %s)" % self._cluster
    self._client = self._scheduler.get_thrift_client()
    assert self._client, "Could not construct thrift client."

  def get_and_set_config(self, *line):
    (job, config) = line

    if config.endswith('.thermos'):
      config = ProxyConfig.from_thermos(config)
    else:
      config = ProxyConfig.from_mesos(config)
    config.set_job(job)
    self._config = config
    return config.job()

  def acquire_session(self):
    return MesosCLIHelper.acquire_session_key_or_die(getpass.getuser())

  @requires_arguments('job', 'config')
  def do_create(self, *line):
    """create job config"""
    job = self.get_and_set_config(*line)

    if self.options.copy_app_from is not None:
      MesosCLIHelper.copy_app_to_hadoop(self.options.copy_app_from,
          self.config().hdfs_path(), self.cluster(), self.proxy())

    sessionkey = self.acquire_session()

    log.info('Creating job %s' % job.name)
    log_scheduler(self.client().createJob(job, sessionkey))


  @requires_arguments('job', 'config')
  def do_inspect(self, *line):
    """inspect job config"""
    log.info('Inspecting job %s in %s' % (line[0], line[1]))
    job = self.get_and_set_config(*line)

    if self.options.copy_app_from is not None:
      if self.config().hdfs_path():
        log.info('Detected HDFS package: %s' % self.config().hdfs_path())
        log.info('Would copy to %s via %s.' % (self.cluster(), self.proxy()))

    sessionkey = self.acquire_session()
    log.info('Parsed job: %s' % job)


  @requires_arguments('role', 'job')
  def do_start_cron(self, *line):
    """start_cron role job"""
    (role, job) = line
    log_scheduler(self.client().startCronJob(role, job, self.acquire_session()))


  @requires_arguments('role', 'job')
  def do_kill(self, *line):
    """kill role job"""

    log.info('Killing tasks')
    (role, job) = line
    query = TaskQuery()
    query.owner = Identity(role = role)
    query.jobName = job
    response = self.client().killTasks(query, self.acquire_session())
    log_scheduler(response)
    if response.responseCode != ResponseCode.OK:
      sys.exit(1)

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
      return task.status in ACTIVE_STATES

    def print_task(scheduled_task):
      assigned_task = scheduled_task.assignedTask
      taskInfo = assigned_task.task
      taskString = """\tHDFS path: %s
\tcpus: %s, ram: %s MB, disk: %s MB""" % (taskInfo.hdfsPath,
                                          taskInfo.numCpus,
                                          taskInfo.ramMb,
                                          taskInfo.diskMb)
      if assigned_task.assignedPorts:
        taskString += '\n\tports: %s' % assigned_task.assignedPorts
      taskString += '\n\tfailure count: %s (max %s)' % (scheduled_task.failureCount,
                                                        taskInfo.maxTaskFailures)
      return taskString

    resp = self.client().getTasksStatus(query)
    log_scheduler(resp)

    def print_tasks(tasks):
      for task in tasks:
        taskString = print_task(task)

        log.info('shard: %s, status: %s on %s\n%s' %
               (task.assignedTask.task.shardId,
                ScheduleStatus._VALUES_TO_NAMES[task.status],
                task.assignedTask.slaveHost,
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

    job = self.get_and_set_config(*line)

    # TODO(William Farner): Add a rudimentary versioning system here so application updates
    #                       don't overwrite original binary.
    if self.options.copy_app_from is not None:
      MesosCLIHelper.copy_app_to_hadoop(self.options.copy_app_from, self.config().hdfs_path(),
          self.cluster(), self.proxy())

    sessionkey = self.acquire_session()

    log.info('Updating job %s' % job.name)

    resp = self.client().startUpdate(job, sessionkey)
    if resp.responseCode != ResponseCode.OK:
      log.warning('Response from scheduler: %s (message: %s)'
          % (ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
      return

    # TODO(William Farner): Cleanly handle connection failures in case the scheduler
    #                       restarts mid-update.
    updater = Updater(job.owner.role, job.name, self.client(), time, resp.updateToken, sessionkey)
    failed_shards = updater.update(job)

    if failed_shards:
      log.info('Update reverted, failures detected on shards %s' % failed_shards)
    else:
      log.info('Update Successful')
    resp = self.client().finishUpdate(
      job.owner.role, job.name, UpdateResult.FAILED if failed_shards else UpdateResult.SUCCESS,
      resp.updateToken, sessionkey)

    if resp.responseCode != UpdateResponseCode.OK:
      log.info('Response from scheduler: %s (message: %s)'
        % (UpdateResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))


  @requires_arguments('role', 'job')
  def do_cancel_update(self, *line):
    """cancel_update role job"""

    (role, job) = line
    log.info('Canceling update on job %s' % job)
    log_scheduler(
      self.client().finishUpdate(role, job, UpdateResult.TERMINATE, None, self.acquire_session()))


  @requires_arguments('role')
  def do_get_quota(self, *line):
    """get_quota role"""

    role = line[0]
    resp = self.client().getQuota(role)
    quota = resp.quota

    quota_fields = [
      ('CPU', quota.numCpus),
      ('RAM', '%f GB' % (float(quota.ramMb) / 1024)),
      ('Disk', '%f GB' % (float(quota.diskMb) / 1024))
    ]
    log.info('Quota for %s:\n\t%s' %
             (role, '\n\t'.join(['%s\t%s' % (k, v) for (k, v) in quota_fields])))


  # TODO(wfarner): Revisit this once we have a better way to add named args per sub-cmmand
  @requires_arguments('role', 'cpu', 'ramMb', 'diskMb')
  def do_set_quota(self, *line):
    """set_quota role cpu ramMb diskMb"""

    (role, cpu_str, ram_mb_str, disk_mb_str) = line

    try:
      cpu = float(cpu_str)
      ram_mb = int(ram_mb_str)
      disk_mb = int(disk_mb_str)
    except ValueError:
      log.error('Invalid value')

    resp = self.client().setQuota(role, Quota(cpu, ram_mb, disk_mb), self.acquire_session())
    log.info('Response from scheduler: %s' % resp)


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
  app.add_option(
    '-v',
    dest='verbose',
    default=False,
    action='store_true',
    help='Verbose logging. (default: %default)')
  app.add_option(
    '-q',
    dest='quiet',
    default=False,
    action='store_true',
    help='Minimum logging. (default: %default)')
  app.add_option(
    '--cluster',
    dest='cluster',
    help='Cluster to launch the job in (e.g. sjc1, smf1-prod, smf1-nonprod)')
  app.add_option(
    '--copy_app_from',
    dest='copy_app_from',
    default=None,
    help="Local path to use for the app (will copy to the cluster)" + \
          " (default: %default)")

def main(args, options):
  if not args:
    app.help()
    sys.exit(1)

  if options.quiet:
    LogOptions.set_stdout_log_level('NONE')
  else:
    if options.verbose:
      LogOptions.set_stdout_log_level('DEBUG')
    else:
      LogOptions.set_stdout_log_level('INFO')

  cli = MesosCLI(options)

  try:
    cli.onecmd(' '.join(args))
  except AssertionError as e:
    _die('\n%s' % e)

initialize_options()
app.main()
