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

from apache.aurora.client.base import check_and_log_response

from gen.apache.aurora.constants import ACTIVE_STATES
from gen.apache.aurora.ttypes import (
  Identity,
  JobKey,
  Quota,
  Response,
  ResponseCode,
  ScheduleStatus,
  TaskQuery
)


class JobUpTimeSlaVector(object):
  """Converts job tasks into SLA vector data: a list of instance uptimes.
     Exposes an API for converting raw instance uptime data into job SLA metrics.
  """

  def __init__(self, tasks):
    self._tasks = tasks
    self._now = time.time()
    self._uptime_map = self._instance_uptime()

  def get_task_up_count(self, duration):
    """Returns the percentage of job tasks that stayed up longer than duration.

    Arguments:
    duration -- uptime duration in seconds.
    """
    total = len(self._uptime_map)
    above = len([uptime for uptime in self._uptime_map.values() if uptime >= duration])
    return 0 if not total else 100.0 * above / total

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
