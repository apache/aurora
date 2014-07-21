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
from apache.aurora.common.auth import make_session_key
from apache.aurora.common.cluster import Cluster

from .restarter import Restarter
from .scheduler_client import SchedulerProxy
from .sla import Sla
from .updater import Updater

from gen.apache.aurora.api.constants import LIVE_STATES
from gen.apache.aurora.api.ttypes import Identity, ResourceAggregate, ResponseCode, TaskQuery


class AuroraClientAPI(object):
  """This class provides the API to talk to the twitter scheduler"""

  class Error(Exception): pass
  class TypeError(Error, TypeError): pass
  class ClusterMismatch(Error, ValueError): pass
  class ThriftInternalError(Error): pass

  def __init__(self, cluster, verbose=False, session_key_factory=make_session_key):
    if not isinstance(cluster, Cluster):
      raise TypeError('AuroraClientAPI expects instance of Cluster for "cluster", got %s' %
          type(cluster))
    self._scheduler_proxy = SchedulerProxy(
        cluster, verbose=verbose, session_key_factory=session_key_factory)
    self._cluster = cluster

  @property
  def cluster(self):
    return self._cluster

  @property
  def scheduler_proxy(self):
    return self._scheduler_proxy

  def create_job(self, config, lock=None):
    log.info('Creating job %s' % config.name())
    log.debug('Full configuration: %s' % config.job())
    log.debug('Lock %s' % lock)
    return self._scheduler_proxy.createJob(config.job(), lock)

  def schedule_cron(self, config, lock=None):
    log.info("Registering job %s with cron" % config.name())
    log.debug('Full configuration: %s' % config.job())
    log.debug('Lock %s' % lock)
    return self._scheduler_proxy.scheduleCronJob(config.job(), lock)

  def deschedule_cron(self, jobkey):
    log.info("Removing cron schedule for job %s" % jobkey)
    return self._scheduler_proxy.descheduleCronJob(jobkey.to_thrift())

  def populate_job_config(self, config):
    return self._scheduler_proxy.populateJobConfig(config.job())

  def start_cronjob(self, job_key):
    self._assert_valid_job_key(job_key)

    log.info("Starting cron job: %s" % job_key)
    return self._scheduler_proxy.startCronJob(job_key.to_thrift())

  def get_jobs(self, role):
    log.info("Retrieving jobs for role %s" % role)
    return self._scheduler_proxy.getJobs(role)

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
    return self._scheduler_proxy.killTasks(query, lock)

  def check_status(self, job_key):
    self._assert_valid_job_key(job_key)

    log.info("Checking status of %s" % job_key)
    return self.query_no_configs(job_key.to_thrift_query())

  @classmethod
  def build_query(cls, role, job, instances=None, statuses=LIVE_STATES, env=None):
    return TaskQuery(owner=Identity(role=role),
                     jobName=job,
                     statuses=statuses,
                     instanceIds=instances,
                     environment=env)

  def query(self, query):
    try:
      return self._scheduler_proxy.getTasksStatus(query)
    except SchedulerProxy.ThriftInternalError as e:
      raise self.ThriftInternalError(e.args[0])

  def query_no_configs(self, query):
    """Returns all matching tasks without TaskConfig.executorConfig set."""
    try:
      return self._scheduler_proxy.getTasksWithoutConfigs(query)
    except SchedulerProxy.ThriftInternalError as e:
      raise self.ThriftInternalError(e.args[0])

  def update_job(self, config, health_check_interval_seconds=3, instances=None):
    """Run a job update for a given config, for the specified instances.  If
       instances is left unspecified, update all instances.  Returns whether or not
       the update was successful."""

    log.info("Updating job: %s" % config.name())
    updater = Updater(config, health_check_interval_seconds, self._scheduler_proxy)

    return updater.update(instances)

  def cancel_update(self, job_key):
    """Cancel the update represented by job_key. Returns whether or not the cancellation was
       successful."""
    self._assert_valid_job_key(job_key)

    log.info("Canceling update on job %s" % job_key)
    resp = Updater.cancel_update(self._scheduler_proxy, job_key)
    if resp.responseCode != ResponseCode.OK:
      log.error('Error cancelling the update: %s' % resp.messageDEPRECATED)
    return resp

  def restart(self, job_key, instances, updater_config, health_check_interval_seconds):
    """Perform a rolling restart of the job.

       If instances is None or [], restart all instances.  Returns the
       scheduler response for the last restarted batch of instances (which
       allows the client to show the job URL), or the status check response
       if no tasks were active.
    """
    self._assert_valid_job_key(job_key)

    return Restarter(job_key, updater_config, health_check_interval_seconds, self._scheduler_proxy
    ).restart(instances)

  def start_maintenance(self, hosts):
    log.info("Starting maintenance for: %s" % hosts.hostNames)
    return self._scheduler_proxy.startMaintenance(hosts)

  def drain_hosts(self, hosts):
    log.info("Draining tasks on: %s" % hosts.hostNames)
    return self._scheduler_proxy.drainHosts(hosts)

  def maintenance_status(self, hosts):
    log.info("Maintenance status for: %s" % hosts.hostNames)
    return self._scheduler_proxy.maintenanceStatus(hosts)

  def end_maintenance(self, hosts):
    log.info("Ending maintenance for: %s" % hosts.hostNames)
    return self._scheduler_proxy.endMaintenance(hosts)

  def get_quota(self, role):
    log.info("Getting quota for: %s" % role)
    return self._scheduler_proxy.getQuota(role)

  def set_quota(self, role, cpu, ram, disk):
    log.info("Setting quota for user:%s cpu:%f ram:%d disk: %d"
              % (role, cpu, ram, disk))
    return self._scheduler_proxy.setQuota(role, ResourceAggregate(cpu, ram, disk))

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
    return self._scheduler_proxy.queryRecovery(query)

  def delete_recovery_tasks(self, query):
    return self._scheduler_proxy.deleteRecoveryTasks(query)

  def commit_recovery(self):
    return self._scheduler_proxy.commitRecovery()

  def unload_recovery(self):
    return self._scheduler_proxy.unloadRecovery()

  def snapshot(self):
    return self._scheduler_proxy.snapshot()

  def unsafe_rewrite_config(self, rewrite_request):
    return self._scheduler_proxy.rewriteConfigs(rewrite_request)

  def get_locks(self):
    return self._scheduler_proxy.getLocks()

  def sla_get_job_uptime_vector(self, job_key):
    self._assert_valid_job_key(job_key)
    return Sla(self._scheduler_proxy).get_job_uptime_vector(job_key)

  def sla_get_safe_domain_vector(self, min_instance_count, hosts=None):
    return Sla(self._scheduler_proxy).get_domain_uptime_vector(
        self._cluster,
        min_instance_count,
        hosts)

  def _assert_valid_job_key(self, job_key):
    if not isinstance(job_key, AuroraJobKey):
      raise self.TypeError('Invalid job_key %r: expected %s but got %s'
          % (job_key, AuroraJobKey.__name__, job_key.__class__.__name__))
    if job_key.cluster != self.cluster.name:
      raise self.ClusterMismatch('job %s does not belong to cluster %s' % (job_key,
          self.cluster.name))
