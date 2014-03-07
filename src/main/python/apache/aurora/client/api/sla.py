#
# Copyright 2014 Apache Software Foundation
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

import math
import time

from collections import defaultdict, namedtuple
from copy import deepcopy

from apache.aurora.client.base import check_and_log_response
from apache.aurora.common.aurora_job_key import AuroraJobKey

from gen.apache.aurora.constants import LIVE_STATES
from gen.apache.aurora.ttypes import (
  Identity,
  Response,
  ResponseCode,
  ScheduleStatus,
  TaskQuery
)


SLA_LIVE_STATES = LIVE_STATES | set([ScheduleStatus.STARTING])


def job_key_from_scheduled(task, cluster):
  """Creates AuroraJobKey from the ScheduledTask.

  Arguments:
  task -- ScheduledTask to get job key from.
  cluster -- Cluster the task belongs to.
  """
  return AuroraJobKey(
      cluster=cluster.name,
      role=task.assignedTask.task.owner.role,
      env=task.assignedTask.task.environment,
      name=task.assignedTask.task.jobName
  )


def task_query(job_key=None, hosts=None, job_keys=None):
  """Creates TaskQuery optionally scoped by a job(s) or hosts.

  Arguments:
  job_key -- AuroraJobKey to scope the query by.
  hosts -- list of hostnames to scope the query by.
  job_keys -- list of AuroraJobKeys to scope the query by.
  """
  return TaskQuery(
      owner=Identity(role=job_key.role) if job_key else None,
      environment=job_key.env if job_key else None,
      jobName=job_key.name if job_key else None,
      slaveHosts=set(hosts) if hosts else None,
      jobKeys=set(k.to_thrift() for k in job_keys) if job_keys else None,
      statuses=SLA_LIVE_STATES)


class JobUpTimeSlaVector(object):
  """A grouping of job active tasks by:
      - instance: Map of instance ID -> instance uptime in seconds.
     Exposes an API for converting raw instance uptime data into job SLA metrics.
  """

  def __init__(self, tasks, now=None):
    self._tasks = tasks
    self._now = now or time.time()
    self._uptime_map = self._instance_uptime()

  def total_tasks(self):
    """Returns the total count of active tasks."""
    return len(self._uptime_map)

  def get_wait_time_to_sla(self, percentile, duration, total_tasks=None):
    """Returns an approximate wait time until the job reaches the specified SLA
       defined by percentile and duration.

    Arguments:
    percentile -- up count percentile to calculate wait time against.
    duration -- uptime duration to calculate wait time against.
    total_tasks -- optional total task count to calculate against.
    """
    upcount = self.get_task_up_count(duration, total_tasks)
    if upcount >= percentile:
      return 0

    # To get wait time to SLA:
    # - Calculate the desired number of up instances in order to satisfy the percentile.
    # - Find the desired index (x) in the instance list sorted in non-decreasing order of uptimes.
    #   If desired index outside of current element count -> return None for "infeasible".
    # - Calculate wait time as: duration - duration(x)
    elements = len(self._uptime_map)
    total = total_tasks or elements
    target_count = math.ceil(total * percentile / 100.0)
    index = elements - int(target_count)

    if index < 0 or index >= elements:
      return None
    else:
      return duration - sorted(self._uptime_map.values())[index]


  def get_task_up_count(self, duration, total_tasks=None):
    """Returns the percentage of job tasks that stayed up longer than duration.

    Arguments:
    duration -- uptime duration in seconds.
    total_tasks -- optional total task count to calculate against.
    """
    total = total_tasks or len(self._uptime_map)
    above = len([uptime for uptime in self._uptime_map.values() if uptime >= duration])
    return 100.0 * above / total if total else 0

  def get_job_uptime(self, percentile):
    """Returns the uptime (in seconds) of the job at the specified percentile.

    Arguments:
    percentile -- percentile to report uptime for.
    """
    if percentile <= 0 or percentile >= 100:
      raise ValueError('Percentile must be within (0, 100), got %r instead.' % percentile)

    total = len(self._uptime_map)
    value = math.floor(percentile / 100.0 * total)
    index = total - int(value) - 1
    return sorted(self._uptime_map.values())[index] if 0 <= index < total else 0

  def _instance_uptime(self):
    instance_map = {}
    for task in self._tasks:
      for event in task.taskEvents:
        if event.status == ScheduleStatus.STARTING:
          instance_map[task.assignedTask.instanceId] = math.floor(self._now - event.timestamp / 1000)
          break
    return instance_map


class DomainUpTimeSlaVector(object):
  """A grouping of all active tasks in the cluster by:
      - job: Map of job_key -> task. Provides logical mapping between jobs and their active tasks.
      - host: Map of hostname -> job_key. Provides logical mapping between hosts and their jobs.
     Exposes an API for querying safe domain details.
  """

  JobUpTimeLimit = namedtuple('JobUpTimeLimit', ['job', 'percentage', 'duration_secs'])
  JobUpTimeDetails = namedtuple('JobUpTimeDetails',
      ['job', 'predicted_percentage', 'safe', 'safe_in_secs'])

  def __init__(self, cluster, tasks):
    self._cluster = cluster
    self._tasks = tasks
    self._now = time.time()
    self._jobs, self._hosts = self._init_mappings()

  def get_safe_hosts(self, percentage, duration, job_limits=None):
    """Returns hosts safe to restart with respect to their job SLA.
       Every host is analyzed separately without considering other job hosts.

       Arguments:
       percentage -- default task up count percentage. Used if job_limits mapping is not found.
       duration -- default task uptime duration in seconds. Used if job_limits mapping is not found.
       job_limits -- optional SLA override map. Key: job key. Value JobUpTimeLimit. If specified,
                     replaces default percentage/duration within the job context.
    """
    safe_hosts = defaultdict(list)
    for host, job_keys in self._hosts.items():
      safe_limits = []
      for job_key in job_keys:
        job_duration = duration
        job_percentage = percentage
        if job_limits and job_key in job_limits:
          job_duration = job_limits[job_key].duration_secs
          job_percentage = job_limits[job_key].percentage

        filtered_percentage, _, _ = self._simulate_host_down(job_key, host, job_duration)
        safe_limits.append(self.JobUpTimeLimit(job_key, filtered_percentage, job_duration))

        if filtered_percentage < job_percentage:
          break

      else:
        safe_hosts[host] = safe_limits

    return safe_hosts

  def probe_hosts(self, percentage, duration, hosts):
    """Returns predicted job SLAs following the removal of provided hosts.

       For every given host creates a list of JobUpTimeDetails with predicted job SLA details
       in case the host is restarted, including: host, job_key, predicted up count, whether
       the predicted job SLA >= percentage and the expected wait time in seconds for the job
       to reach its SLA.

       Arguments:
       percentage -- task up count percentage.
       duration -- task uptime duration in seconds.
       hosts -- list of hosts to probe for job SLA changes.
    """
    probed_hosts = defaultdict(list)
    for host in hosts:
      for job_key in self._hosts.get(host, []):
        filtered_percentage, total_count, filtered_vector = self._simulate_host_down(
            job_key, host, duration)

        # Calculate wait time to SLA in case down host violates job's SLA.
        if filtered_percentage < percentage:
          safe = False
          wait_to_sla = filtered_vector.get_wait_time_to_sla(percentage, duration, total_count)
        else:
          safe = True
          wait_to_sla = 0

        probed_hosts[host].append(
            self.JobUpTimeDetails(job_key, filtered_percentage, safe, wait_to_sla))

    return probed_hosts

  def _simulate_host_down(self, job_key, host, duration):
    unfiltered_tasks = self._jobs[job_key]

    # Get total job task count to use in SLA calculation.
    total_count = len(unfiltered_tasks)

    # Get a list of job tasks that would remain after the affected host goes down
    # and create an SLA vector with these tasks.
    filtered_tasks = [task for task in unfiltered_tasks
                      if task.assignedTask.slaveHost != host]
    filtered_vector = JobUpTimeSlaVector(filtered_tasks, self._now)

    # Calculate the SLA that would be in effect should the host go down.
    filtered_percentage = filtered_vector.get_task_up_count(duration, total_count)

    return filtered_percentage, total_count, filtered_vector

  def _init_mappings(self):
    jobs = defaultdict(list)
    hosts = defaultdict(list)
    for task in self._tasks:
      job_key = job_key_from_scheduled(task, self._cluster)
      jobs[job_key].append(task)
      hosts[task.assignedTask.slaveHost].append(job_key)

    return jobs, hosts


class Sla(object):
  """Defines methods for generating job uptime metrics required for monitoring job SLA."""

  def __init__(self, scheduler):
    self._scheduler = scheduler

  def get_job_uptime_vector(self, job_key):
    """Returns a JobUpTimeSlaVector object for the given job key.

    Arguments:
    job_key -- job to create a task uptime vector for.
    """
    return JobUpTimeSlaVector(self._get_tasks(task_query(job_key=job_key)))

  def get_domain_uptime_vector(self, cluster, hosts=None):
    """Returns a DomainUpTimeSlaVector object with all available job uptimes.

    Arguments:
    cluster -- Cluster to get vector for.
    hosts -- optional list of hostnames to query by.
    """
    tasks = self._get_tasks(task_query(hosts=hosts)) if hosts else None
    job_keys = set(job_key_from_scheduled(t, cluster) for t in tasks) if tasks else None
    return DomainUpTimeSlaVector(cluster, self._get_tasks(task_query(job_keys=job_keys)))

  def _get_tasks(self, task_query):
    resp = self._scheduler.getTasksStatus(task_query)
    check_and_log_response(resp)
    return resp.result.scheduleStatusResult.tasks
