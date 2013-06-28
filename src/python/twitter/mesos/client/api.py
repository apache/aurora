from twitter.common import log
from twitter.common.lang import Compatibility
from twitter.mesos.common import AuroraJobKey
from twitter.mesos.common.cluster import Cluster

from gen.twitter.mesos.constants import DEFAULT_ENVIRONMENT, LIVE_STATES
from gen.twitter.mesos.ttypes import (
    FinishUpdateResponse,
    Identity,
    JobKey,
    ResponseCode,
    Quota,
    ScheduleStatus,
    TaskQuery)

from .restarter import Restarter
from .scheduler_client import SchedulerProxy
from .updater import Updater


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
  class Error(Exception): pass
  class TypeError(Error, TypeError): pass
  class ClusterMismatch(Error, ValueError): pass

  def __init__(self, cluster, verbose=False):
    if not isinstance(cluster, Cluster):
      raise TypeError('MesosClientAPI expects instance of Cluster for "cluster", got %s' %
          type(cluster))
    self._scheduler = SchedulerProxy(cluster, verbose=verbose)
    self._cluster = cluster

  @property
  def cluster(self):
    return self._cluster

  @property
  def scheduler(self):
    return self._scheduler

  def create_job(self, config):
    log.info('Creating job %s' % config.name())
    log.debug('Full configuration: %s' % config.job())
    return self._scheduler.createJob(config.job())

  def populate_job_config(self, config):
    return self._scheduler.populateJobConfig(config.job())

  def start_cronjob(self, job_key):
    self._assert_valid_job_key(job_key)

    log.info("Starting cron job: %s" % job_key)
    return self._scheduler.startCronJob(job_key.to_thrift())

  def get_jobs(self, role):
    log.info("Retrieving jobs for role %s" % role)
    return self._scheduler.getJobs(role)

  def kill_job(self, job_key, shards=None):
    log.info("Killing tasks for job: %s" % job_key)
    if not isinstance(job_key, AuroraJobKey):
      raise TypeError('Expected type of job_key %r to be %s but got %s instead'
          % (job_key, AuroraJobKey.__name__, job_key.__class__.__name__))

    # Leave query.owner.user unset so the query doesn't filter jobs only submitted by a particular
    # user.
    # TODO(wfarner): Refactor this when Identity is removed from TaskQuery.
    query = job_key.to_thrift_query()
    if shards is not None:
      log.info("Shards to be killed: %s" % shards)
      query.shardIds = frozenset([int(s) for s in shards])

    return self._scheduler.killTasks(query)

  def check_status(self, job_key):
    self._assert_valid_job_key(job_key)

    log.info("Checking status of %s" % job_key)
    return self.query(job_key.to_thrift_query())

  @classmethod
  def build_query(cls, role, job, shards=None, statuses=LIVE_STATES, env=None):
    return TaskQuery(
      owner=Identity(role=role), jobName=job, statuses=statuses, shardIds=shards, environment=env)

  def query(self, query):
    return self._scheduler.getTasksStatus(query)

  def update_job(self, config, health_check_interval_seconds=3, shards=None):
    """Run a job update for a given config, for the specified shards.  If
       shards is left unspecified, update all shards.  Returns whether or not
       the update was successful."""

    log.info("Updating job: %s" % config.name())
    updater = Updater(config, self._scheduler)

    resp = updater.start()
    update_resp = FinishUpdateResponse()
    update_resp.responseCode = resp.responseCode
    update_resp.message = resp.message

    if resp.responseCode != ResponseCode.OK:
      log.info("Error starting update: %s" % resp.message)
      log.warning(self.UPDATE_FAILURE_WARNING)
      return update_resp
    elif not resp.rollingUpdateRequired:
      log.info('Update successful: %s' % resp.message)
      return update_resp

    failed_shards = updater.update(
        shards or list(range(config.instances())), health_check_interval_seconds)

    if failed_shards:
      log.info('Update reverted, failures detected on shards %s' % failed_shards)
    else:
      log.info('Update successful')

    resp = updater.finish(failed_shards)
    if resp.responseCode != ResponseCode.OK:
      log.error('There was an error finalizing the update: %s' % resp.message)

    return resp

  def cancel_update(self, job_key):
    """Cancel the update represented by job_key. Returns whether or not the cancellation was
    successful."""
    self._assert_valid_job_key(job_key)

    log.info("Canceling update on job %s" % job_key)
    resp = Updater.cancel_update(self._scheduler, job_key.role, job_key.env, job_key.name)
    if resp.responseCode != ResponseCode.OK:
      log.error('Error cancelling the update: %s' % resp.message)
    return resp

  def restart(self, job_key, shards, updater_config, health_check_interval_seconds):
    """Perform a rolling restart of the job. If shards is None or [], restart all shards. Returns the
       scheduler response for the last restarted batch of shards (which allows the client to show
       the job URL), or the status check response if no tasks were active.
    """
    self._assert_valid_job_key(job_key)

    return Restarter(job_key, updater_config, health_check_interval_seconds, self._scheduler
    ).restart(shards)

  def start_maintenance(self, hosts):
    log.info("Starting maintenance for: %s" % hosts.hostNames)
    return self._scheduler.startMaintenance(hosts)

  def drain_hosts(self, hosts):
    log.info("Draining tasks on: %s" % hosts.hostNames)
    return self._scheduler.drainHosts(hosts)

  def maintenance_status(self, hosts):
    log.info("Maintenance status for: %s" % hosts.hostNames)
    return self._scheduler.maintenanceStatus(hosts)

  def end_maintenance(self, hosts):
    log.info("Ending maintenance for: %s" % hosts.hostNames)
    return self._scheduler.endMaintenance(hosts)

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

  def get_job_updates(self):
    return self._scheduler.getJobUpdates()

  def snapshot(self):
    return self._scheduler.snapshot()

  def _assert_valid_job_key(self, job_key):
    if not isinstance(job_key, AuroraJobKey):
      raise self.TypeError('Invalid job_key %r: expected %s but got %s'
          % (job_key, AuroraJobKey.__name__, job_key.__class__.__name__))
    if job_key.cluster != self.cluster.name:
      raise self.ClusterMismatch('job %s does not belong to cluster %s' % (job_key,
          self.cluster.name))
