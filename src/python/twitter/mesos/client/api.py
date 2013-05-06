from twitter.common import log
from twitter.common.lang import Compatibility
from twitter.mesos.client.updater import Updater

from gen.twitter.mesos.constants import DEFAULT_ENVIRONMENT, LIVE_STATES
from gen.twitter.mesos.ttypes import (
    FinishUpdateResponse,
    Identity,
    JobKey,
    ResponseCode,
    Quota,
    TaskQuery)

from .scheduler_client import SchedulerProxy


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

  def __init__(self, cluster, verbose=False):
    self._scheduler = SchedulerProxy(cluster, verbose=verbose)
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

  def start_cronjob(self, role, env, jobname):
    log.info("Starting cron job: %s" % jobname)

    job = JobKey(role=role, environment=env, name=jobname)

    # TODO(ksweeney): Remove nulls after Thrift refactor.
    return self._scheduler.startCronJob(None, None, job)

  def kill_job(self, role, env, jobname, shards=None):
    log.info("Killing tasks for job: %s/%s/%s" % (role, env, jobname))
    if not isinstance(role, Compatibility.string) or not role:
      raise ValueError('role must be a non-empty string')
    if not isinstance(jobname, Compatibility.string) or not jobname:
      raise ValueError('jobname must be a non-empty string')
    if not isinstance(env, Compatibility.string) or not env:
      raise ValueError('env must be a non-empty string')

    # Leave query.owner.user unset so the query doesn't filter jobs only submitted by a particular
    # user.
    # TODO(wfarner): Refactor this when Identity is removed from TaskQuery.
    query = TaskQuery(owner=Identity(role=role), environment=env, jobName=jobname)
    if shards is not None:
      log.info("Shards to be killed: %s" % shards)
      query.shardIds = frozenset([int(s) for s in shards])

    return self._scheduler.killTasks(query)

  def check_status(self, role, jobname=None, env=None):
    log.info("Checking status of role: %s / env: %s / job: %s" % (role, env, jobname))

    return self.query(TaskQuery(owner=Identity(role=role), environment=env, jobName=jobname))

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

  def cancel_update(self, role, env, jobname):
    """Cancel the update represented by role/env/jobname. Returns whether or not the cancellation
       was successful."""
    log.info("Canceling update on job: %s/%s/%s" % (role, env, jobname))
    resp = Updater.cancel_update(self._scheduler, role, env, jobname)
    if resp.responseCode != ResponseCode.OK:
      log.error('Error cancelling the update: %s' % resp.message)
    return resp

  def restart(self, role, jobname, shards):
    log.info("Restarting job %s shards %s" % (jobname, shards))

    # TODO(ksweeney): Change to use just job after JobKey refactor
    job = JobKey(role=role, environment=DEFAULT_ENVIRONMENT, name=jobname)

    return self._scheduler.restartShards(role, jobname, job, shards)

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
