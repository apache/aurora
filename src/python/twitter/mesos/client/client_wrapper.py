import functools
import getpass
import time

from twitter.common import app, log
from twitter.common.lang import Compatibility
from twitter.common.quantity import Amount, Time
from twitter.mesos.clusters import Cluster

from twitter.mesos.scheduler_client import SchedulerClient
from twitter.mesos.session_key_helper import SessionKeyHelper
from twitter.mesos.updater import Updater

from gen.twitter.mesos import MesosAdmin
from gen.twitter.mesos.ttypes import (
    FinishUpdateResponse,
    Identity,
    ResponseCode,
    Quota,
    TaskQuery,
    UpdateResult)

from thrift.transport import TTransport


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

  def create_job(self, config):
    log.info('Creating job %s' % config.name())
    log.debug('Full configuration: %s' % config.job())
    return self._scheduler.createJob(config.job())

  def populate_job_config(self, config):
    return self._scheduler.populateJobConfig(config.job())

  def start_cronjob(self, role, jobname):
    log.info("Starting cron job: %s" % jobname)

    return self._scheduler.startCronJob(role, jobname)

  def kill_job(self, role, jobname, shards=None):
    log.info("Killing tasks for job: %s" % jobname)
    if not isinstance(jobname, Compatibility.string) or not jobname:
      raise ValueError('jobname must be a non-empty string!')

    query = TaskQuery()
    # TODO(wickman)  Allow us to send user=getpass.getuser() without it resulting in filtering jobs
    # only submitted by a particular user.
    query.owner = Identity(role=role)
    query.jobName = jobname
    if shards is not None:
      log.info("Shards to be killed: %s" % shards)
      query.shardIds = frozenset([int(s) for s in shards])

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

  def update_job(self, config, shards=None):
    log.info("Updating job: %s" % config.name())
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

  def perform_backup(self):
    return self._scheduler.performBackup()

  def list_backups(self):
    return self._scheduler.listBackups()

  def stage_recovery(self, backup_id):
    return self._scheduler.stageRecovery(backup_id)

  def query_recovery(self, query):
    return self._scheduler.queryRecovery(query)

  def delete_recovery_tasks(self, query):
    return self._scheduler.deleteRecoveryTasks(query)

  def commit_recovery(self):
    return self._scheduler.commitRecovery()

  def unload_recovery(self):
    return self._scheduler.unloadRecovery()


def is_verbose():
  return app.get_options().verbosity == 'verbose'

def create_client(cluster=None):
  if cluster is None:
    cluster = app.get_options().cluster
  return MesosClientAPI(cluster=cluster, verbose=is_verbose())
