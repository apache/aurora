#
# Copyright 2013 Apache Software Foundation
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

from apache.aurora.common.http_signaler import HttpSignaler

from gen.apache.aurora.ttypes import ScheduleStatus


class HealthCheck(Interface):
  @abstractmethod
  def health(self, task):
    """Checks health of the task and returns a (healthy, retriable) pair."""


class HealthStatus(object):
  @classmethod
  def alive(cls):
    return cls(True).health()

  @classmethod
  def dead(cls):
    return cls(False).health()

  def __init__(self, retry, health):
    self._retry = retry
    self._health = health

  def health(self):
    return (self._health, self._retry)


class NotRetriable(HealthStatus):
  def __init__(self, health):
    super(NotRetriable, self).__init__(False, health)


class Retriable(HealthStatus):
  def __init__(self, health):
    super(Retriable, self).__init__(True, health)


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
        return Retriable.alive() if task_id == self._task_ids.get(instance_id) else NotRetriable.dead()
      else:
        log.info('Detected RUNNING instance %s' % instance_id)
        self._task_ids[instance_id] = task_id
        return Retriable.alive()
    else:
      return Retriable.dead()


class HttpHealthCheck(HealthCheck):
  """Verifies the health of a task based on http health checks. A new http signaler is created for a
  task iff,
    1. The instance id of the task is unknown.
    2. The instance id is known but the (host, port) is different for the task.
  """
  def __init__(self, http_signaler_factory=HttpSignaler):
    self._http_signalers = {}
    self._http_signaler_factory = http_signaler_factory

  def health(self, task):
    assigned_task = task.assignedTask
    instance_id = assigned_task.instanceId
    host_port = (assigned_task.slaveHost, assigned_task.assignedPorts['health'])
    http_signaler = None
    if instance_id in self._http_signalers:
      checker_host_port, signaler = self._http_signalers.get(instance_id)
      # Only reuse the health checker if it is for the same destination.
      if checker_host_port == host_port:
        http_signaler = signaler
    if not http_signaler:
      http_signaler = self._http_signaler_factory(host_port[1], host_port[0])
      self._http_signalers[instance_id] = (host_port, http_signaler)
    return Retriable.alive() if http_signaler.health()[0] else Retriable.dead()


class ChainedHealthCheck(HealthCheck):
  """Delegates health checks to configured health checkers."""
  def __init__(self, *health_checkers):
    self._health_checkers = health_checkers

  def health(self, task):
    for checker in self._health_checkers:
      healthy, retriable = checker.health(task)
      if not healthy:
        return (healthy, retriable)
    return Retriable.alive()


class InstanceWatcherHealthCheck(HealthCheck):
  """Makes the decision: if a task has health port, then use Status+HTTP, else use only status.
     Caveat: Only works if either ALL tasks have a health port or none of them have a health port.
  """
  # TODO(atollenaere) Refactor the code to use the executor StatusChecker/HealthChecker instead

  def __init__(self, http_signaler_factory=HttpSignaler):
    self._has_health_port = False
    self._health_checker = StatusHealthCheck()
    self._http_signaler_factory = http_signaler_factory

  def health(self, task):
    if not self._has_health_port and 'health' in task.assignedTask.assignedPorts:
      log.debug('Health port detected, enabling HTTP checks')
      self._health_checker = ChainedHealthCheck(self._health_checker, HttpHealthCheck(self._http_signaler_factory))
      self._has_health_port = True
    return self._health_checker.health(task)
