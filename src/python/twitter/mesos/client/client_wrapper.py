import functools
import getpass
import os
import posixpath
import subprocess
import time

from twitter.common import log, dirutil
from twitter.common.quantity import Amount, Time
from twitter.mesos.clusters import Cluster
from twitter.mesos.location import Location

from twitter.mesos.scheduler_client import SchedulerClient
from twitter.mesos.session_key_helper import SessionKeyHelper
from twitter.mesos.updater import Updater

from gen.twitter.mesos import MesosAdmin
from gen.twitter.mesos.ttypes import *
from thrift.transport import TTransport


class Command(object):
  @classmethod
  def maybe_tunnel(cls, cmd, host, user):
    return cmd if host is None else ['ssh', '-t', '%s@%s' % (user, host), ' '.join(cmd)]

  def __init__(self, cmd, user=None, host=None, verbose=False):
    self._verbose = verbose
    if isinstance(cmd, str):
      cmd = [cmd]
    elif not isinstance(cmd, list):
      raise ValueError('Command takes a string or a list!  Got %s' % type(cmd))

    self._cmd = self.maybe_tunnel(cmd, host, user or getpass.getuser())

  def logged(fn):
    def real_fn(self, *args, **kw):
      if self._verbose:
        print('Running command: %s' % self._cmd)
      return fn(self, *args, **kw)
    return real_fn

  @logged
  def call(self):
    return subprocess.call(self._cmd)

  @logged
  def check_call(self):
    return subprocess.check_call(self._cmd)

  @logged
  def check_output(self):
    return subprocess.Popen(self._cmd, stdout=subprocess.PIPE).communicate()[0].rstrip('\r\n')

  del logged


class HDFSHelper(object):
  """Helper class for performing HDFS operations."""

  def __init__(self, cluster, user=None, proxy=True):
    self._cluster = cluster
    cluster_conf = Cluster.get(cluster)
    self._uri = cluster_conf.hadoop_uri
    self._config = cluster_conf.hadoop_config
    self._user = user or getpass.getuser()
    self._proxy = cluster_conf.proxy if proxy else None

  @property
  def config(self):
    return self._config

  @property
  def uri(self):
    return self._uri

  def join(self, *paths):
    return posixpath.join(self._uri, *paths)

  def call(self, cmd, *args, **kwargs):
    """Runs hadoop fs command (via proxy if necessary) with the given command and args.
    Checks the result of the call by default but this can be disabled with check=False.
    """
    log.info("Running hadoop fs %s %s" % (cmd, list(args)))
    cmd = Command(['hadoop', '--config', self._config, 'fs', cmd] + list(args),
                   self._user, self._proxy)
    return cmd.check_call() if kwargs.get('check', False) else cmd.call()

  def copy_from(self, src, dst):
    """
    Copy file(s) in hdfs to local path (via proxy if necessary).
    NOTE: If src matches multiple files, make sure dst is a directory!
    """
    log.info('Copying %s -> %s' % (self.join(src), dst))

    hdfs_src = self.join(src)
    if self._proxy:
      scratch_dir = Command('mktemp -d', user=self._user, host=self._proxy).check_output()
      try:
        self.call('-get', hdfs_src, scratch_dir)
        Command('scp -rq %s@%s:%s/*' % (self._user, self._proxy, scratch_dir)).call()
      finally:
        Command('rm -rf %s' % scratch_dir, user=self._user, host=self._proxy).call()
    else:
      self.call('-get', hdfs_src, dst)

  def copy_to(self, src, dst):
    """
    Copy the local file src to a hadoop path dst.
    """
    abs_src = os.path.expanduser(src)
    dst_dir = os.path.dirname(dst)
    assert os.path.exists(abs_src), 'File does not exist, cannot copy: %s' % abs_src
    log.info('Copying %s -> %s' % (abs_src, self.join(dst_dir)))

    def do_put(source):
      hdfs_dst = self.join(dst)
      if not self.call('-test', '-e', hdfs_dst, check=False):
        self.call('-rm', '-skipTrash', hdfs_dst)
      self.call('-put', source, hdfs_dst)

    if self._proxy:
      scratch_dir = Command('mktemp -d', user=self._user, host=self._proxy).check_output()
      Command('chmod 755 %s' % scratch_dir, user=self._user, host=self._proxy).check_call()
      try:
        Command(['scp', abs_src, '%s@%s:%s' % (self._user, self._proxy, scratch_dir)]).check_call()
        do_put(os.path.join(scratch_dir, os.path.basename(abs_src)))
      finally:
        Command('rm -rf %s' % scratch_dir, user=self._user, host=self._proxy).call()
    else:
      do_put(abs_src)


class SchedulerProxy(object):
  """
    This class is responsible for creating a reliable thrift client to the
    twitter scheduler.  Basically all the dirty work needed by the
    MesosClientAPI.
  """
  CONNECT_MAXIMUM_WAIT = Amount(1, Time.MINUTES)
  RPC_RETRY_INTERVAL = Amount(5, Time.SECONDS)
  RPC_MAXIMUM_WAIT = Amount(10, Time.MINUTES)
  UNAUTHENTICATED_RPCS = frozenset([
    'populateJobConfig',
    'getTasksStatus',
    'getQuota'
  ])

  class TimeoutError(Exception): pass

  @staticmethod
  def assert_valid_cluster(cluster):
    assert cluster, "Cluster not specified!"
    if cluster.find(':') > -1:
      scluster = cluster.split(':')

      if scluster[0] != 'localhost':
        Cluster.assert_exists(scluster[0])

      if len(scluster) == 2:
        try:
          int(scluster[1])
        except ValueError as e:
          log.fatal('The cluster argument is invalid: %s (error: %s)' % (cluster, e))
          assert False, 'Invalid cluster argument: %s' % cluster
    else:
      Cluster.assert_exists(cluster)

  def __init__(self, cluster, verbose=False):
    self.cluster = cluster
    self._session_key = self._client = self._scheduler = None
    self.verbose = verbose
    self.assert_valid_cluster(cluster)

  def with_scheduler(method):
    """Decorator magic to make sure a connection is made to the scheduler"""
    def _wrapper(self, *args, **kwargs):
      if not self._scheduler:
        self._construct_scheduler()
      return method(self, *args, **kwargs)
    return _wrapper

  def invalidate(self):
    self._session_key = self._client = self._scheduler = None

  def requires_auth(method):
    def _wrapper(self, *args, **kwargs):
      if not self._session_key:
        self._session_key = SessionKeyHelper.acquire_session_key(getpass.getuser())
      return method(self, *args, **kwargs)
    return _wrapper

  @with_scheduler
  def client(self):
    return self._client

  @requires_auth
  def session_key(self):
    return self._session_key

  @with_scheduler
  def scheduler(self):
    return self._scheduler

  def _construct_scheduler(self):
    """
      Populates:
        self._scheduler
        self._client
    """
    self._scheduler = SchedulerClient.get(self.cluster, verbose=self.verbose)
    assert self._scheduler, "Could not find scheduler (cluster = %s)" % self.cluster
    start = time.time()
    while (time.time() - start) < self.CONNECT_MAXIMUM_WAIT.as_(Time.SECONDS):
      try:
        self._client = self._scheduler.get_thrift_client()
        break
      except SchedulerClient.CouldNotConnect as e:
        log.warning('Could not connect to scheduler: %s' % e)
    if not self._client:
      raise self.TimeoutError('Timed out trying to connect to scheduler at %s' % self.cluster)

  def __getattr__(self, method_name):
    # If the method does not exist, getattr will return AttributeError for us.
    method = getattr(MesosAdmin.Client, method_name)
    if not callable(method):
      return method

    @functools.wraps(method)
    def method_wrapper(*args):
      start = time.time()
      while (time.time() - start) < self.RPC_MAXIMUM_WAIT.as_(Time.SECONDS):
        auth_args = () if method_name in self.UNAUTHENTICATED_RPCS else (self.session_key(),)
        try:
          method = getattr(self.client(), method_name)
          if not callable(method):
            return method
          return method(*(args + auth_args))
        except (TTransport.TTransportException, self.TimeoutError) as e:
          log.warning('Connection error with scheduler: %s, reconnecting...' % e)
          self.invalidate()
          time.sleep(self.RPC_RETRY_INTERVAL.as_(Time.SECONDS))
      raise self.TimeoutError('Timed out attempting to issue %s to %s' % (
          method_name, self.cluster))

    return method_wrapper


class MesosClientAPI(object):
  """This class provides the API to talk to the twitter scheduler"""

  UPDATE_FAILURE_WARNING = """
Note: if the scheduler detects that an update is in progress (or was not
properly completed) it will reject subsequent updates.  This is because your
job is likely in a partially-updated state.  You should only begin another
update if you are confident that no other team members are updating this
job, and that the job is in a state suitable for an update.

After checking on the above, you may release the update lock on the job by
invoking cancel_update.
"""

  def __init__(self, *args, **kw):
    self._scheduler = SchedulerProxy(*args, **kw)
    self._cluster = self._scheduler.cluster

  @property
  def scheduler(self):
    return self._scheduler

  def hdfs_path(self, config, copy_app_from):
    if config.hdfs_path():
      return config.hdfs_path()
    else:
      return '/mesos/pkg/%s/%s' % (config.role(), os.path.basename(copy_app_from))

  def create_job(self, config, copy_app_from=None):
    if copy_app_from is not None:
      HDFSHelper(self._cluster, config.role(), Location.is_corp()).copy_to(copy_app_from,
        self.hdfs_path(config, copy_app_from))

    log.info('Creating job %s' % config.name())
    log.debug('Full configuration: %s' % config.job())
    return self._scheduler.createJob(config.job())

  def populate_job_config(self, config):
    return self._scheduler.populateJobConfig(config.job())

  def start_cronjob(self, role, jobname):
    log.info("Starting cron job: %s" % jobname)

    return self._scheduler.startCronJob(role, jobname)

  def kill_job(self, role, jobname):
    log.info("Killing tasks for job: %s" % jobname)

    query = TaskQuery()
    query.owner = Identity(role=role)
    query.jobName = jobname

    return self._scheduler.killTasks(query)

  def check_status(self, role, jobname=None):
    log.info("Checking status of role: %s / job: %s" % (role, jobname))

    query = TaskQuery()
    query.owner = Identity(role=role)
    if jobname:
      query.jobName = jobname
    return self.query(query)

  def query(self, query):
    return self._scheduler.getTasksStatus(query)

  def update_job(self, config, shards=None, copy_app_from=None):
    log.info("Updating job: %s" % config.name())

    if copy_app_from is not None:
      HDFSHelper(self._cluster, config.role(), Location.is_corp()).copy_to(copy_app_from,
        self.hdfs_path(config, copy_app_from))

    resp = self._scheduler.startUpdate(config.job())

    if resp.responseCode != ResponseCode.OK:
      log.info("Error starting update: %s" % resp.message)
      log.warning(self.UPDATE_FAILURE_WARNING)

      # Create an update response and return it
      update_resp = FinishUpdateResponse()
      update_resp.responseCode = ResponseCode.INVALID_REQUEST
      update_resp.message = resp.message
      return update_resp

    if not resp.rollingUpdateRequired:
      log.info('Update successful')
      return resp

    updater = Updater(config.role(), config.name(), self._scheduler, time, resp.updateToken)

    shardsIds = shards or sorted(map(lambda task: task.shardId, config.job().taskConfigs))
    failed_shards = updater.update(config.update_config(), shardsIds)

    if failed_shards:
      log.info('Update reverted, failures detected on shards %s' % failed_shards)
    else:
      log.info('Update successful')

    resp = self._scheduler.finishUpdate(
      config.role(), config.name(), UpdateResult.FAILED if failed_shards else UpdateResult.SUCCESS,
      resp.updateToken)

    if resp.responseCode != ResponseCode.OK:
      log.info("Error finalizing update: %s" % resp.message)

      # Create a update response and return it
      update_resp = FinishUpdateResponse()
      update_resp.responseCode = ResponseCode.INVALID_REQUEST
      update_resp.message = resp.message
      return update_resp

    resp = FinishUpdateResponse()
    resp.responseCode = ResponseCode.WARNING if failed_shards else ResponseCode.OK
    resp.message = "Update unsuccessful" if failed_shards else "Update successful"
    return resp

  def cancel_update(self, role, jobname):
    log.info("Canceling update on job: %s" % jobname)

    resp = self._scheduler.finishUpdate(role, jobname, UpdateResult.TERMINATE, None)

    if resp.responseCode != ResponseCode.OK:
      log.info("Error cancelling the update: %s" % resp.message)

      # Create a update response and return it
      update_resp = FinishUpdateResponse()
      update_resp.responseCode = ResponseCode.INVALID_REQUEST
      update_resp.message = resp.message
      return update_resp

    resp = FinishUpdateResponse()
    resp.responseCode = ResponseCode.OK
    resp.message = "Update Cancelled"
    return resp

  def get_quota(self, role):
    log.info("Getting quota for: %s" % role)

    return self._scheduler.getQuota(role)

  def set_quota(self, role, cpu, ram_mb, disk_mb):
    log.info("Setting quota for user:%s cpu:%f ram_mb:%d disk_mb: %d"
              % (role, cpu, ram_mb, disk_mb))

    return self._scheduler.setQuota(role, Quota(cpu, ram_mb, disk_mb))

  def force_task_state(self, task_id, status):
    log.info("Requesting that task %s transition to state %s" % (task_id, status))
    return self._scheduler.forceTaskState(task_id, status)
