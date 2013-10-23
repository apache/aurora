from twitter.common import log

from twitter.aurora.common import AuroraJobKey
from twitter.aurora.common.auth import make_session_key
from twitter.aurora.common.cluster import Cluster

from gen.twitter.aurora.constants import LIVE_STATES
from gen.twitter.aurora.ttypes import (
    Response,
    Identity,
    Quota,
    ResponseCode,
    TaskQuery)

from .restarter import Restarter
from .scheduler_client import SchedulerProxy
from .updater import Updater


class AuroraClientAPI(object):
  """This class provides the API to talk to the twitter scheduler"""

  class Error(Exception): pass
  class TypeError(Error, TypeError): pass
  class ClusterMismatch(Error, ValueError): pass

  def __init__(self, cluster, verbose=False, session_key_factory=make_session_key):
    if not isinstance(cluster, Cluster):
      raise TypeError('AuroraClientAPI expects instance of Cluster for "cluster", got %s' %
          type(cluster))
    self._scheduler = SchedulerProxy(
        cluster, verbose=verbose, session_key_factory=session_key_factory)
    self._cluster = cluster

  @property
  def cluster(self):
    return self._cluster

  @property
  def scheduler(self):
    return self._scheduler

  def create_job(self, config, lock=None):
    log.info('Creating job %s' % config.name())
    log.debug('Full configuration: %s' % config.job())
    log.debug('Lock %s' % lock)
    return self._scheduler.createJob(config.job(), lock)

  def populate_job_config(self, config):
    return self._scheduler.populateJobConfig(config.job())

  def start_cronjob(self, job_key):
    self._assert_valid_job_key(job_key)

    log.info("Starting cron job: %s" % job_key)
    return self._scheduler.startCronJob(job_key.to_thrift())

  def get_jobs(self, role):
    log.info("Retrieving jobs for role %s" % role)
    return self._scheduler.getJobs(role)

  def kill_job(self, job_key, instances=None, lock=None):
    log.info("Killing tasks for job: %s" % job_key)
    if not isinstance(job_key, AuroraJobKey):
      raise TypeError('Expected type of job_key %r to be %s but got %s instead'
          % (job_key, AuroraJobKey.__name__, job_key.__class__.__name__))

    # Leave query.owner.user unset so the query doesn't filter jobs only submitted by a particular
    # user.
    # TODO(wfarner): Refactor this when Identity is removed from TaskQuery.
    query = job_key.to_thrift_query()
    if instances is not None:
      log.info("Instances to be killed: %s" % instances)
      query.instanceIds = frozenset([int(s) for s in instances])

    return self._scheduler.killTasks(query, lock)

  def check_status(self, job_key):
    self._assert_valid_job_key(job_key)

    log.info("Checking status of %s" % job_key)
    return self.query(job_key.to_thrift_query())

  @classmethod
  def build_query(cls, role, job, instances=None, statuses=LIVE_STATES, env=None):
    return TaskQuery(owner=Identity(role=role),
                     jobName=job,
                     statuses=statuses,
                     instanceIds=instances,
                     environment=env)

  def query(self, query):
    return self._scheduler.getTasksStatus(query)

  def update_job(self, config, health_check_interval_seconds=3, instances=None):
    """Run a job update for a given config, for the specified instances.  If
       instances is left unspecified, update all instances.  Returns whether or not
       the update was successful."""

    log.info("Updating job: %s" % config.name())
    updater = Updater(config, health_check_interval_seconds, self._scheduler)

    return updater.update(instances)

  def cancel_update(self, job_key):
    """Cancel the update represented by job_key. Returns whether or not the cancellation was
       successful."""
    self._assert_valid_job_key(job_key)

    log.info("Canceling update on job %s" % job_key)
    resp = Updater.cancel_update(self._scheduler, job_key)
    if resp.responseCode != ResponseCode.OK:
      log.error('Error cancelling the update: %s' % resp.message)
    return resp

  def restart(self, job_key, instances, updater_config, health_check_interval_seconds):
    """Perform a rolling restart of the job. If instances is None or [], restart all instances. Returns
       the scheduler response for the last restarted batch of instances (which allows the client to
       show the job URL), or the status check response if no tasks were active.
    """
    self._assert_valid_job_key(job_key)

    return Restarter(job_key, updater_config, health_check_interval_seconds, self._scheduler
    ).restart(instances)

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

  def unsafe_rewrite_config(self, rewrite_request):
    return self._scheduler.rewriteConfigs(rewrite_request)

  def _assert_valid_job_key(self, job_key):
    if not isinstance(job_key, AuroraJobKey):
      raise self.TypeError('Invalid job_key %r: expected %s but got %s'
          % (job_key, AuroraJobKey.__name__, job_key.__class__.__name__))
    if job_key.cluster != self.cluster.name:
      raise self.ClusterMismatch('job %s does not belong to cluster %s' % (job_key,
          self.cluster.name))
