#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

from twitter.common import log

from apache.aurora.common.aurora_job_key import AuroraJobKey
from apache.aurora.common.cluster import Cluster

from .restarter import Restarter
from .scheduler_client import SchedulerProxy
from .sla import Sla
from .updater_util import UpdaterConfig

from gen.apache.aurora.api.constants import LIVE_STATES
from gen.apache.aurora.api.ttypes import (
    ExplicitReconciliationSettings,
    InstanceKey,
    JobKey,
    JobUpdateKey,
    JobUpdateQuery,
    JobUpdateRequest,
    Metadata,
    Resource,
    ResourceAggregate,
    TaskQuery
)


class AuroraClientAPI(object):
  """This class provides the API to talk to the twitter scheduler"""

  class Error(Exception): pass
  class ClusterMismatch(Error, ValueError): pass
  class ThriftInternalError(Error): pass
  class UpdateConfigError(Error): pass

  def __init__(
      self,
      cluster,
      user_agent,
      verbose=False,
      bypass_leader_redirect=False):

    if not isinstance(cluster, Cluster):
      raise TypeError('AuroraClientAPI expects instance of Cluster for "cluster", got %s' %
          type(cluster))

    self._scheduler_proxy = SchedulerProxy(
        cluster,
        verbose=verbose,
        user_agent=user_agent,
        bypass_leader_redirect=bypass_leader_redirect)
    self._cluster = cluster

  @property
  def cluster(self):
    return self._cluster

  @property
  def scheduler_proxy(self):
    return self._scheduler_proxy

  def create_job(self, config):
    log.info('Creating job %s' % config.name())
    log.debug('Full configuration: %s' % config.job())
    return self._scheduler_proxy.createJob(config.job())

  def schedule_cron(self, config):
    log.info("Registering job %s with cron" % config.name())
    log.debug('Full configuration: %s' % config.job())
    return self._scheduler_proxy.scheduleCronJob(config.job())

  def deschedule_cron(self, jobkey):
    log.info("Removing cron schedule for job %s" % jobkey)
    return self._scheduler_proxy.descheduleCronJob(jobkey.to_thrift())

  def populate_job_config(self, config):
    # read-only calls are retriable.
    return self._scheduler_proxy.populateJobConfig(config.job(), retry=True)

  def start_cronjob(self, job_key):
    self._assert_valid_job_key(job_key)

    log.info("Starting cron job: %s" % job_key)
    return self._scheduler_proxy.startCronJob(job_key.to_thrift())

  def get_jobs(self, role):
    log.info("Retrieving jobs for role %s" % role)
    # read-only calls are retriable.
    return self._scheduler_proxy.getJobs(role, retry=True)

  def add_instances(self, job_key, instance_id, count):
    key = InstanceKey(jobKey=job_key.to_thrift(), instanceId=instance_id)
    log.info("Adding %s instances to %s using the task config of instance %s"
             % (count, job_key, instance_id))
    return self._scheduler_proxy.addInstances(key, count)

  def kill_job(self, job_key, instances=None, message=None):
    log.info("Killing tasks for job: %s" % job_key)
    self._assert_valid_job_key(job_key)

    if instances is not None:
      log.info("Instances to be killed: %s" % instances)
      instances = frozenset([int(s) for s in instances])
    return self._scheduler_proxy.killTasks(job_key.to_thrift(), instances, message)

  def check_status(self, job_key):
    self._assert_valid_job_key(job_key)

    log.info("Checking status of %s" % job_key)
    return self.query_no_configs(job_key.to_thrift_query())

  @classmethod
  def build_query(cls, role, job, env=None, instances=None, statuses=LIVE_STATES):
    return TaskQuery(jobKeys=[JobKey(role=role, environment=env, name=job)],
                     statuses=statuses,
                     instanceIds=instances)

  def query(self, query):
    try:
      # read-only calls are retriable.
      return self._scheduler_proxy.getTasksStatus(query, retry=True)
    except SchedulerProxy.ThriftInternalError as e:
      raise self.ThriftInternalError(e.args[0])

  def query_no_configs(self, query):
    """Returns all matching tasks without TaskConfig.executorConfig set."""
    try:
      # read-only calls are retriable.
      return self._scheduler_proxy.getTasksWithoutConfigs(query, retry=True)
    except SchedulerProxy.ThriftInternalError as e:
      raise self.ThriftInternalError(e.args[0])

  def _job_update_request(self, config, instances=None, metadata=None):
    try:
      settings = UpdaterConfig(**config.update_config().get()).to_thrift_update_settings(instances)
    except ValueError as e:
      raise self.UpdateConfigError(str(e))

    return JobUpdateRequest(
        instanceCount=config.instances(),
        settings=settings,
        taskConfig=config.job().taskConfig,
        metadata={Metadata(k, v) for k, v in metadata.items()} if metadata else None
    )

  def start_job_update(self, config, message, instances=None, metadata=None):
    """Requests Scheduler to start job update process.

    Arguments:
    config -- AuroraConfig instance with update details.
    message -- Audit message to include with the change.
    instances -- Optional list of instances to restrict update to.
    metadata -- Optional set of metadata (key, value) to associate with the update.

    Returns response object with update ID and acquired job lock.
    """
    request = self._job_update_request(config, instances, metadata)
    log.info("Starting update for: %s" % config.name())
    # retring starting a job update is safe, client and scheduler reconcile state if the
    # job update is in progress (AURORA-1711).
    return self._scheduler_proxy.startJobUpdate(request, message, retry=True)

  def pause_job_update(self, update_key, message):
    """Requests Scheduler to pause active job update.

    Arguments:
    update_key -- Update identifier.
    message -- Audit message to include with the change.

    Returns response object.
    """
    return self._scheduler_proxy.pauseJobUpdate(update_key, message)

  def resume_job_update(self, update_key, message):
    """Requests Scheduler to resume a job update paused previously.

    Arguments:
    update_key -- Update identifier.
    message -- Audit message to include with the change.

    Returns response object.
    """
    return self._scheduler_proxy.resumeJobUpdate(update_key, message)

  def abort_job_update(self, update_key, message):
    """Requests Scheduler to abort active or paused job update.

    Arguments:
    update_key -- Update identifier.
    message -- Audit message to include with the change.

    Returns response object.
    """
    return self._scheduler_proxy.abortJobUpdate(update_key, message)

  def rollback_job_update(self, update_key, message):
    """Requests Scheduler to rollback active job update.

    Arguments:
    update_key -- Update identifier.
    message -- Audit message to include with the change.

    Returns response object.
    """
    return self._scheduler_proxy.rollbackJobUpdate(update_key, message)

  def get_job_update_diff(self, config, instances=None):
    """Requests scheduler to calculate difference between scheduler and client job views.

    Arguments:
    config -- AuroraConfig instance with update details.
    message -- Audit message to include with the change.
    instances -- Optional list of instances to restrict update to.

    Returns response object with job update diff results.
    """
    request = self._job_update_request(config, instances)
    log.debug("Requesting job update diff details for: %s" % config.name())
    # read-only calls are retriable.
    return self._scheduler_proxy.getJobUpdateDiff(request, retry=True)

  def query_job_updates(
      self,
      role=None,
      job_key=None,
      user=None,
      update_statuses=None,
      update_key=None):
    """Returns all job updates matching the query.

    Arguments:
    role -- job role.
    job_key -- job key.
    user -- user who initiated an update.
    update_statuses -- set of JobUpdateStatus to match.
    update_key -- JobUpdateKey to match.

    Returns response object with all matching job update summaries.
    """
    # TODO(wfarner): Consider accepting JobUpdateQuery in this function instead of kwargs.
    # read-only calls are retriable.
    return self._scheduler_proxy.getJobUpdateSummaries(
        JobUpdateQuery(
            role=role,
            jobKey=job_key.to_thrift() if job_key else None,
            user=user,
            updateStatuses=update_statuses,
            key=update_key),
        retry=True)

  def get_job_update_details(self, key):
    """Gets JobUpdateDetails for the specified job update ID.

    Arguments:
    id -- job update ID.

    Returns a response object with JobUpdateDetails.
    """
    if not isinstance(key, JobUpdateKey):
      raise self.TypeError('Invalid key %r: expected %s but got %s'
                           % (key, JobUpdateKey.__name__, key.__class__.__name__))

    query = JobUpdateQuery(key=key)
    # read-only calls are retriable.
    return self._scheduler_proxy.getJobUpdateDetails(key, query, retry=True)

  def restart(self, job_key, instances, restart_settings):
    """Perform a rolling restart of the job.

       If instances is None or [], restart all instances.  Returns the
       scheduler response for the last restarted batch of instances (which
       allows the client to show the job URL), or the status check response
       if no tasks were active.
    """
    self._assert_valid_job_key(job_key)

    return Restarter(job_key, restart_settings, self._scheduler_proxy).restart(instances)

  def start_maintenance(self, hosts):
    log.info("Starting maintenance for: %s" % hosts.hostNames)
    return self._scheduler_proxy.startMaintenance(hosts)

  def drain_hosts(self, hosts):
    log.info("Draining tasks on: %s" % hosts.hostNames)
    return self._scheduler_proxy.drainHosts(hosts)

  def maintenance_status(self, hosts):
    log.info("Maintenance status for: %s" % hosts.hostNames)
    # read-only calls are retriable.
    return self._scheduler_proxy.maintenanceStatus(hosts, retry=True)

  def end_maintenance(self, hosts):
    log.info("Ending maintenance for: %s" % hosts.hostNames)
    return self._scheduler_proxy.endMaintenance(hosts)

  def get_quota(self, role):
    log.info("Getting quota for: %s" % role)
    # read-only calls are retriable.
    return self._scheduler_proxy.getQuota(role, retry=True)

  def set_quota(self, role, cpu, ram, disk):
    log.info("Setting quota for user:%s cpu:%f ram:%d disk: %d"
              % (role, cpu, ram, disk))
    return self._scheduler_proxy.setQuota(
        role,
        ResourceAggregate(cpu, ram, disk, frozenset([
            Resource(numCpus=cpu),
            Resource(ramMb=ram),
            Resource(diskMb=disk)])))

  def get_tier_configs(self):
    log.debug("Getting tier configurations")
    # read-only calls are retriable.
    return self._scheduler_proxy.getTierConfigs(retry=True)

  def force_task_state(self, task_id, status):
    log.info("Requesting that task %s transition to state %s" % (task_id, status))
    return self._scheduler_proxy.forceTaskState(task_id, status)

  def perform_backup(self):
    return self._scheduler_proxy.performBackup()

  def list_backups(self):
    return self._scheduler_proxy.listBackups()

  def stage_recovery(self, backup_id):
    return self._scheduler_proxy.stageRecovery(backup_id)

  def query_recovery(self, query):
    # read-only calls are retriable.
    return self._scheduler_proxy.queryRecovery(query, retry=True)

  def delete_recovery_tasks(self, query):
    return self._scheduler_proxy.deleteRecoveryTasks(query)

  def commit_recovery(self):
    return self._scheduler_proxy.commitRecovery()

  def unload_recovery(self):
    return self._scheduler_proxy.unloadRecovery()

  def snapshot(self):
    return self._scheduler_proxy.snapshot()

  def prune_tasks(self, query):
    return self._scheduler_proxy.pruneTasks(query)

  def sla_get_job_uptime_vector(self, job_key):
    self._assert_valid_job_key(job_key)
    return Sla(self._scheduler_proxy).get_job_uptime_vector(job_key)

  def sla_get_safe_domain_vector(self, min_instance_count, hosts=None):
    return Sla(self._scheduler_proxy).get_domain_uptime_vector(
        self._cluster,
        min_instance_count,
        hosts)

  def reconcile_explicit(self, batch_size):
    return self._scheduler_proxy.triggerExplicitTaskReconciliation(
      ExplicitReconciliationSettings(batchSize=batch_size))

  def reconcile_implicit(self):
    return self._scheduler_proxy.triggerImplicitTaskReconciliation()

  def _assert_valid_job_key(self, job_key):
    if not isinstance(job_key, AuroraJobKey):
      raise TypeError('Invalid job_key %r: expected %s but got %s'
          % (job_key, AuroraJobKey.__name__, job_key.__class__.__name__))
    if job_key.cluster != self.cluster.name:
      raise self.ClusterMismatch('job %s does not belong to cluster %s' % (job_key,
          self.cluster.name))
