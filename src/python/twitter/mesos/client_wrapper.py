import getpass
import os
import posixpath
import subprocess
import time

from twitter.common import log, dirutil
from twitter.mesos.clusters import Cluster
from twitter.mesos.location import Location

from twitter.mesos.scheduler_client import SchedulerClient
from twitter.mesos.session_key_helper import SessionKeyHelper
from twitter.mesos.updater import Updater

from gen.twitter.mesos.ttypes import *


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

  def __init__(self, cluster, user=None, proxy=None):
    self._cluster = cluster
    self._uri = Cluster.get(cluster).hadoop_uri
    self._config = Cluster.get(cluster).hadoop_config
    self._user = user or getpass.getuser()
    self._proxy = proxy or Cluster.get(cluster).proxy

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


class MesosHelper(object):
  @staticmethod
  def acquire_session_key(owner):
    key = SessionKey(user=owner)
    try:
      SessionKeyHelper.sign_session(key, owner)
    except Exception as e:
      log.warning('Cannot use SSH auth: %s' % e)
      log.warning('Attempting un-authenticated communication')
      key.nonce = SessionKeyHelper.get_timestamp()
      key.nonceSig = 'UNAUTHENTICATED'
    return key

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


class MesosClientBase(object):
  """
  This class is responsible for creating a thrift client
  to the twitter scheduler. Basically all the dirty work
  needed by the MesosClientAPI.
  """

  def __init__(self, cluster, verbose=False):
    self._cluster = cluster
    self._session_key = self._client = self._scheduler = None
    self.verbose = verbose
    MesosHelper.assert_valid_cluster(cluster)

  def with_scheduler(method):
    """Decorator magic to make sure a connection is made to the scheduler"""
    def _wrapper(self, *args, **kwargs):
      if not self._scheduler:
        self._construct_scheduler()
      return method(self, *args, **kwargs)
    return _wrapper

  def requires_auth(method):
    def _wrapper(self, *args, **kwargs):
      if not self._session_key:
        self._session_key = MesosHelper.acquire_session_key(getpass.getuser())
      return method(self, *args, **kwargs)
    return _wrapper

  @with_scheduler
  def client(self):
    return self._client

  @with_scheduler
  def cluster(self):
    return self._cluster

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
    self._scheduler = SchedulerClient.get(self._cluster, verbose=self.verbose)
    assert self._scheduler, "Could not find scheduler (cluster = %s)" % self._cluster
    self._client = self._scheduler.get_thrift_client()
    assert self._client, "Could not construct thrift client."


class MesosClientAPI(MesosClientBase):
  """This class provides the API to talk to the twitter scheduler"""

  def __init__(self, **kwargs):
    super(MesosClientAPI, self).__init__(**kwargs)

  def hdfs_path(self, config, copy_app_from):
    if config.hdfs_path():
      return config.hdfs_path()
    else:
      return '/mesos/pkg/%s/%s' % (config.role(), os.path.basename(copy_app_from))

  def create_job(self, config, copy_app_from=None):
    if copy_app_from is not None:
      HDFSHelper(self.cluster(), config.role()).copy_to(copy_app_from,
        self.hdfs_path(config, copy_app_from))

    log.info('Creating job %s' % config.name())
    return self.client().createJob(config.job(), self.session_key())

  def start_cronjob(self, role, jobname):
    log.info("Starting cron job: %s" % jobname)

    return self.client().startCronJob(role, jobname, self.session_key())

  def kill_job(self, role, jobname):
    log.info("Killing tasks for job: %s" % jobname)

    query = TaskQuery()
    query.owner = Identity(role=role)
    query.jobName = jobname

    return self.client().killTasks(query, self.session_key())

  def check_status(self, role, jobname=None):
    log.info("Checking status of role: %s / job: %s" % (role, jobname))

    query = TaskQuery()
    query.owner = Identity(role=role)
    if jobname:
      query.jobName = jobname
    return self.client().getTasksStatus(query)

  def update_job(self, config, copy_app_from=None):
    log.info("Updating job: %s" % config.name())

    if copy_app_from is not None:
      HDFSHelper(self.cluster(), config.role()).copy_to(copy_app_from,
        self.hdfs_path(config, copy_app_from))

    resp = self.client().startUpdate(config.job(), self.session_key())

    if resp.responseCode != ResponseCode.OK:
      log.info("Error doing start update: %s" % resp.message)

      # Create a update response and return it
      update_resp = FinishUpdateResponse()
      update_resp.responseCode = ResponseCode.INVALID_REQUEST
      update_resp.message = resp.message
      return update_resp

    # TODO(William Farner): Cleanly handle connection failures in case the scheduler
    #                       restarts mid-update.
    updater = Updater(config.role(), config.name(), self.client(), time, resp.updateToken,
                      self.session_key())
    failed_shards = updater.update(
      config.update_config(), sorted(map(lambda task: task.shardId, config.job().taskConfigs)))

    if failed_shards:
      log.info('Update reverted, failures detected on shards %s' % failed_shards)
    else:
      log.info('Update Successful')

    resp = self.client().finishUpdate(
      config.role(), config.name(), UpdateResult.FAILED if failed_shards else UpdateResult.SUCCESS,
      resp.updateToken, self._session_key)

    if resp.responseCode != ResponseCode.OK:
      log.info("Error doing finish update: %s" % resp.message)

      # Create a update response and return it
      update_resp = FinishUpdateResponse()
      update_resp.responseCode = ResponseCode.INVALID_REQUEST
      update_resp.message = resp.message
      return update_resp

    resp = FinishUpdateResponse()
    resp.responseCode = ResponseCode.WARNING if failed_shards else ResponseCode.OK
    resp.message = "Update Unsuccessful" if failed_shards else "Update Successful"
    return resp

  def cancel_update(self, role, jobname):
    log.info("Canceling update on job: %s" % jobname)

    resp = self.client().finishUpdate(role, jobname, UpdateResult.TERMINATE,
        None, self.session_key())

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

    return self.client().getQuota(role)

  def set_quota(self, role, cpu, ram_mb, disk_mb):
    log.info("Setting quota for user:%s cpu:%f ram_mb:%d disk_mb: %d"
              % (role, cpu, ram_mb, disk_mb))

    return self.client().setQuota(role, Quota(cpu, ram_mb, disk_mb), self.session_key())

  def force_task_state(self, task_id, status):
    log.info("Requesting that task %s transition to state %s" % (task_id, status))
    return self.client().forceTaskState(task_id, status, self.session_key())
