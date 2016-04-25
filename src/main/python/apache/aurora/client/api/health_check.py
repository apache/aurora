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

from abc import abstractmethod

from twitter.common import log
from twitter.common.lang import Interface

from gen.apache.aurora.api.ttypes import ScheduleStatus


class HealthCheck(Interface):
  @abstractmethod
  def health(self, task):
    """Checks health of the task and returns a True or False."""


class HealthStatus(object):
  @classmethod
  def alive(cls):
    return cls(True).health()

  @classmethod
  def dead(cls):
    return cls(False).health()

  def __init__(self, health):
    self._health = health

  def health(self):
    return self._health


class StatusHealthCheck(HealthCheck):
  """Verifies the health of a task based on the task status. A task is healthy iff,
    1. A task is in state RUNNING
    2. A task that satisfies (1) and is already known has the same task id.
  """

  def __init__(self):
    self._task_ids = {}

  def health(self, task):
    task_id = task.assignedTask.taskId
    instance_id = task.assignedTask.instanceId
    status = task.status

    if status == ScheduleStatus.RUNNING:
      if instance_id in self._task_ids:
        if task_id == self._task_ids.get(instance_id):
          return HealthStatus.alive()
        else:
          return HealthStatus.dead()
      else:
        log.info('Detected RUNNING instance %s' % instance_id)
        self._task_ids[instance_id] = task_id
        return HealthStatus.alive()
    else:
      return HealthStatus.dead()
