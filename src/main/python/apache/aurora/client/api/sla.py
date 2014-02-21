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

from gen.apache.aurora.constants import ACTIVE_STATES
from gen.apache.aurora.ttypes import (
  Identity,
  Quota,
  Response,
  ResponseCode,
  ScheduleStatus,
  TaskQuery
)


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

  JobUpTimeLimit = namedtuple('JobUpTimeLimit', ['job', 'percentage', 'duration_seconds'])

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
        # Get total job task count to use in SLA calculation.
        total_count = JobUpTimeSlaVector(self._jobs[job_key]).total_tasks()

        # Get a list of job tasks that would remain after the affected host goes down
        # and create an SLA vector with these tasks.
        filtered_tasks = [task for task in self._jobs[job_key]
                          if task.assignedTask.slaveHost != host]
        filtered_vector = JobUpTimeSlaVector(filtered_tasks, self._now)

        job_duration = duration
        job_percentage = percentage
        if job_limits and job_key in job_limits:
          job_duration = job_limits[job_key].duration_seconds
          job_percentage = job_limits[job_key].percentage

        # Calculate the SLA that would be in effect should the host go down.
        filtered_percentage = filtered_vector.get_task_up_count(job_duration, total_count)
        safe_limits.append(self.JobUpTimeLimit(job_key, filtered_percentage, job_duration))

        if filtered_percentage < job_percentage:
          break

      else:
        safe_hosts[host] = safe_limits

    return safe_hosts

  def _init_mappings(self):
    def job_key_from_scheduled(task):
      return AuroraJobKey(
          cluster=self._cluster,
          role=task.assignedTask.task.owner.role,
          env=task.assignedTask.task.environment,
          name=task.assignedTask.task.jobName
      )

    jobs = defaultdict(list)
    hosts = defaultdict(list)
    for task in self._tasks:
      job_key = job_key_from_scheduled(task)
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
    return JobUpTimeSlaVector(self._get_tasks(self._create_task_query(job_key=job_key)))

  def get_domain_uptime_vector(self, cluster):
    """Returns a DomainUpTimeSlaVector object with all available job uptimes."""
    return DomainUpTimeSlaVector(cluster, self._get_tasks(self._create_task_query()))

  def _get_tasks(self, task_query):
    resp = self._scheduler.getTasksStatus(task_query)
    check_and_log_response(resp)
    return resp.result.scheduleStatusResult.tasks

  def _create_task_query(self, job_key=None, host=None):
    return TaskQuery(
        owner=Identity(role=job_key.role) if job_key else None,
        environment=job_key.env if job_key else None,
        jobName=job_key.name if job_key else None,
        slaveHost=host,
        statuses=ACTIVE_STATES)
